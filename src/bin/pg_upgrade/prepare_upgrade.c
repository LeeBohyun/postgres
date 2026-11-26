/*
 * prepare_upgrade.c
 *
 * Plan storage operations and prepare commands for upgrade WAL emission.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/prepare_upgrade.c
 */

#include "postgres_fe.h"

#include <dirent.h>

#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "common/controldata_utils.h"
#include "fe_utils/simple_list.h"
#include "pg_upgrade.h"
#include "pqexpbuffer.h"
#include "relink_file.h"

static UpgradeRelinkFile *relink_file;
static xl_pg_upgrade_start upgrade_start;

static int
compare_database(const void *key, const void *entry)
{
	Oid			database = *(const Oid *) key;
	Oid			other = ((const DbInfo *) entry)->db_oid;

	return (database > other) - (database < other);
}

static int
compare_file_map(const void *a, const void *b)
{
	const FileNameMap *left = a;
	const FileNameMap *right = b;
	int			cmp = strcmp(left->new_tablespace, right->new_tablespace);

	if (cmp != 0)
		return cmp;
	return (left->relfilenumber > right->relfilenumber) -
		(left->relfilenumber < right->relfilenumber);
}

/* Buffer relation commands until all directories in their database are written. */
static void
add_relation_command(PQExpBuffer entries, PQExpBuffer captures,
					 const char *directory,
					 Oid tablespace, Oid database, RelFileNumber filenumber,
					 bool inherit, ForkNumber fork)
{
	xl_pg_upgrade_relink_entry entry = {0};

	entry.key = (xl_pg_upgrade_key)
	{
		tablespace, database, filenumber
	};
	entry.operation = inherit ? UPGRADE_RELINK_INHERIT : UPGRADE_RELINK_RECREATE;
	appendBinaryPQExpBuffer(entries, (char *) &entry, sizeof(entry));
	if (!inherit)
		appendPQExpBuffer(captures, "COPY_RELFILE %u %u %u %u\n",
						  tablespace, database, filenumber, fork);

	/*
	 * Later heap WAL can refer to existing visibility-map pages without an
	 * image. Preserve auxiliary forks even when only the main fork is
	 * inherited from retained storage. Unlogged relations are rebuilt from
	 * their init forks and must not retain their current main or auxiliary
	 * contents.
	 */
	if (fork == MAIN_FORKNUM)
	{
		for (ForkNumber auxiliary = FSM_FORKNUM;
			 auxiliary <= VISIBILITYMAP_FORKNUM; auxiliary++)
		{
			char		path[MAXPGPATH];
			struct stat st;

			snprintf(path, sizeof(path), "%s/%u_%s", directory, filenumber,
					 forkNames[auxiliary]);
			if (lstat(path, &st) == 0)
				appendPQExpBuffer(captures, "COPY_RELFILE %u %u %u %u\n",
								  tablespace, database, filenumber, auxiliary);
			else if (errno != ENOENT)
				pg_fatal("could not stat \"%s\": %m", path);
		}
	}
}

static bool
has_init_fork(const char *directory, RelFileNumber filenumber)
{
	char		path[MAXPGPATH];
	struct stat st;

	snprintf(path, sizeof(path), "%s/%u_init", directory, filenumber);
	if (lstat(path, &st) == 0)
		return true;
	if (errno != ENOENT)
		pg_fatal("could not stat \"%s\": %m", path);
	return false;
}

static void
write_database_commands(Oid database)
{
	ClusterInfo *cluster = &new_cluster;
	FileNameMap *maps = NULL;
	DbInfo	   *new_db;
	DbInfo	   *old_db;
	int			nmaps = 0;
	int			nmapped = 0;
	PQExpBufferData entries;
	PQExpBufferData captures;
	xl_pg_upgrade_relink_entry *relations;
	int			nrelations;
	bool		found_catalog = false;

	new_db = bsearch(&database, cluster->dbarr.dbs, cluster->dbarr.ndbs,
					 sizeof(DbInfo), compare_database);
	old_db = bsearch(&database, old_cluster.dbarr.dbs,
					 old_cluster.dbarr.ndbs,
					 sizeof(DbInfo), compare_database);
	if (new_db != NULL && old_db != NULL)
	{
		maps = gen_db_file_maps(old_db, new_db, &nmaps,
								old_cluster.pgdata, cluster->pgdata);
		qsort(maps, nmaps, sizeof(*maps), compare_file_map);
	}
	initPQExpBuffer(&entries);
	initPQExpBuffer(&captures);

	for (int t = -2; t < cluster->num_tablespaces; t++)
	{
		Oid			tablespace = t == -2 ? DEFAULTTABLESPACE_OID :
			t == -1 ? GLOBALTABLESPACE_OID : cluster->tablespace_oids[t];
		const char *location = t < 0 ? cluster->pgdata : cluster->tablespaces[t];
		char	   *relative;
		char	   *directory;
		DIR		   *dir;
		struct dirent *de;
		xl_pg_upgrade_relink_entry entry = {0};
		bool		catalog_directory = database == InvalidOid ||
			(new_db != NULL && strcmp(location, new_db->db_tablespace[0] ?
									  new_db->db_tablespace : cluster->pgdata) == 0);
		bool		have_relmap = false;
		bool		have_version = false;

		if ((database == InvalidOid) != (tablespace == GLOBALTABLESPACE_OID))
			continue;
		relative = GetDatabasePath(database, tablespace);
		directory = psprintf("%s/%s", cluster->pgdata, relative);
		dir = opendir(directory);
		if (dir == NULL)
		{
			if (errno != ENOENT || catalog_directory)
				pg_fatal("could not open directory \"%s\": %m", directory);
			pg_free(relative);
			pg_free(directory);
			continue;
		}
		entry.key = (xl_pg_upgrade_key)
		{
			tablespace, database, 0
		};
		entry.operation = UPGRADE_RELINK_INHERIT;
		if (t >= 0 && strcmp(old_cluster.tablespaces[t], location) != 0)
			entry.flags = UPGRADE_RELINK_INPLACE;
		append_upgrade_relink_entry(relink_file, &entry);

		/*
		 * Add commands from the file mappings before scanning local files. An
		 * init fork identifies unlogged storage, which must be recreated.
		 */
		for (int i = 0; i < nmaps; i++)
		{
			bool		unlogged;
			bool		inherit;

			if (strcmp(maps[i].new_tablespace, location) != 0)
				continue;
			nmapped++;
			unlogged = has_init_fork(directory, maps[i].relfilenumber);
			inherit = !unlogged && upgrade_start.transfer_mode != TRANSFER_MODE_COPY &&
				upgrade_start.transfer_mode != TRANSFER_MODE_COPY_FILE_RANGE;
			add_relation_command(&entries, &captures, directory, tablespace, database,
								 maps[i].relfilenumber, inherit,
								 unlogged ? INIT_FORKNUM : MAIN_FORKNUM);
		}

		/* Capture remaining local relations and per-database auxiliary files. */
		while (errno = 0, (de = readdir(dir)) != NULL)
		{
			const char *suffix = de->d_name + strspn(de->d_name, "0123456789");
			RelFileNumber filenumber;
			FileNameMap key = {.new_tablespace = location};

			if (strcmp(de->d_name, "pg_filenode.map") == 0 ||
				strcmp(de->d_name, "PG_VERSION") == 0)
			{
				have_relmap |= strcmp(de->d_name, "pg_filenode.map") == 0;
				have_version |= strcmp(de->d_name, "PG_VERSION") == 0;
				continue;
			}
			if (*suffix != '\0' && strcmp(suffix, "_init") != 0)
				continue;
			filenumber = parse_relfilenumber(de->d_name);
			if (!RelFileNumberIsValid(filenumber))
				continue;
			key.relfilenumber = filenumber;
			if (nmaps > 0 &&
				bsearch(&key, maps, nmaps, sizeof(*maps), compare_file_map) != NULL)
				continue;
			if (*suffix == '\0' && has_init_fork(directory, filenumber))
				continue;
			add_relation_command(&entries, &captures, directory, tablespace, database,
								 filenumber, false,
								 *suffix == '\0' ? MAIN_FORKNUM : INIT_FORKNUM);
		}
		if (errno)
			pg_fatal("could not read directory \"%s\": %m", directory);
		if (closedir(dir) != 0)
			pg_fatal("could not close directory \"%s\": %m", directory);
		if (catalog_directory || have_relmap || have_version)
		{
			if (!have_relmap || (database != InvalidOid && !have_version))
				pg_fatal("required catalog files are missing from \"%s\"", directory);
			appendPQExpBuffer(&captures, "COPY_FILE %s/pg_filenode.map\n", relative);
			if (database != InvalidOid)
				appendPQExpBuffer(&captures, "COPY_FILE %s/PG_VERSION\n", relative);
			found_catalog = true;
		}
		pg_free(relative);
		pg_free(directory);
	}
	if (nmapped != nmaps)
		pg_fatal("could not find transferred relation storage for database %u", database);
	if (!found_catalog)
		pg_fatal("could not find catalog storage for database %u", database);
	if (PQExpBufferBroken(&entries) || PQExpBufferBroken(&captures))
		pg_fatal("out of memory");
	relations = (xl_pg_upgrade_relink_entry *) entries.data;
	nrelations = entries.len / sizeof(*relations);
	qsort(relations, nrelations, sizeof(*relations), PgUpgradeCompareKeys);
	for (int i = 0; i < nrelations; i++)
	{
		if (i > 0 && PgUpgradeCompareKeys(&relations[i - 1], &relations[i]) == 0)
			pg_fatal("duplicate relation storage key in database %u", database);
		append_upgrade_relink_entry(relink_file, &relations[i]);
	}
	finish_upgrade_relink_scope(relink_file);
	fputs(captures.data, relink_file->stream);
	termPQExpBuffer(&entries);
	termPQExpBuffer(&captures);
	pg_free(maps);
}

/* Include template0 without opening a connection to it. */
static void
collect_databases(const char *path, SimpleOidList *databases, bool missing_ok)
{
	DIR		   *dir = opendir(path);
	struct dirent *de;

	if (dir == NULL)
	{
		if (missing_ok && errno == ENOENT)
			return;
		pg_fatal("could not open directory \"%s\": %m", path);
	}
	while (errno = 0, (de = readdir(dir)) != NULL)
	{
		Oid			database = parse_relfilenumber(de->d_name);
		char	   *directory;
		struct stat st;

		if (!OidIsValid(database) ||
			de->d_name[strspn(de->d_name, "0123456789")] != '\0')
			continue;
		directory = psprintf("%s/%s", path, de->d_name);
		if (lstat(directory, &st) != 0)
			pg_fatal("could not stat \"%s\": %m", directory);
		pg_free(directory);
		if (S_ISDIR(st.st_mode) && !simple_oid_list_member(databases, database))
			simple_oid_list_append(databases, database);
	}
	if (errno)
		pg_fatal("could not read directory \"%s\": %m", path);
	if (closedir(dir) != 0)
		pg_fatal("could not close directory \"%s\": %m", path);
}

static void
write_upgrade_commands(void)
{
	ClusterInfo *cluster = &new_cluster;
	SimpleOidList databases = {0};
	char	   *path = psprintf("%s/base", cluster->pgdata);

	for (int i = 0; i < cluster->dbarr.ndbs; i++)
		simple_oid_list_append(&databases, cluster->dbarr.dbs[i].db_oid);
	collect_databases(path, &databases, false);
	pg_free(path);
	for (int t = 0; t < cluster->num_tablespaces; t++)
	{
		path = psprintf("%s/pg_tblspc/%u%s", cluster->pgdata,
						cluster->tablespace_oids[t], cluster->tablespace_suffix);
		collect_databases(path, &databases, true);
		pg_free(path);
	}
	write_database_commands(InvalidOid);
	for (SimpleOidListCell *cell = databases.head; cell != NULL; cell = cell->next)
		write_database_commands(cell->val);
	simple_oid_list_destroy(&databases);
	fprintf(relink_file->stream,
			"COPY_FILE PG_VERSION\nCOPY_SLRU pg_xact\n"
			"COPY_SLRU pg_multixact/offsets\nCOPY_SLRU pg_multixact/members\n");
	fprintf(relink_file->stream, "COMPLETE\n");
}

void
create_upgrade_preparation(void)
{
	relink_file = create_upgrade_relink_file(new_cluster.pgdata);
	upgrade_start.transfer_mode = user_opts.transfer_mode;
}

/*
 * Fill START with the old system identifier, version and storage parameters,
 * timeline, transfer mode, and final shutdown checkpoint's end LSN.
 */
void
prepare_upgrade_old_cluster(void)
{
	ClusterInfo *cluster = &old_cluster;
	const ControlData *control = &cluster->controldata;
	xl_pg_upgrade_start *start = &upgrade_start;
	XLogSegNo	segno;

	/* The catalog dump started and stopped the source after the first read. */
	memset(&cluster->controldata, 0, sizeof(cluster->controldata));
	get_control_data(cluster);

	start->old_system_identifier = control->system_identifier;
	start->boundary_lsn = control->shutdown_checkpoint_end_lsn;
	start->marker.old_major = cluster->major_version;
	start->old_catalog_version = control->cat_ver;
	start->old_control_version = control->ctrl_ver;
	start->block_size = control->blocksz;
	start->relseg_blocks = control->largesz;
	start->wal_block_size = control->walsz;
	start->wal_segment_size = control->walseg;
	start->slru_pages_per_segment = SLRU_PAGES_PER_SEGMENT;
	start->old_tli = control->chkpnt_tli;

	/*
	 * Start upgrade WAL in the segment immediately after the old cluster's
	 * shutdown checkpoint. pg_resetwal normally skips past all existing WAL
	 * files, including unused files reserved for future WAL. That could
	 * create a gap between the old cluster's WAL and the upgrade WAL,
	 * breaking replay continuity.
	 */
	XLByteToPrevSeg(start->boundary_lsn, segno, control->walseg);
	XLogFileName(cluster->controldata.upgrade_start_wal_file,
				 control->chkpnt_tli, segno + 1, control->walseg);
}

/*
 * Write START and RELINK storage commands to pg_upgrade_relink.tmp, then
 * rename it to pg_upgrade_relink so the next server startup emits upgrade WAL.
 * The caller syncs the prepared data first, unless --no-sync was requested.
 */
void
finish_upgrade_preparation(void)
{
	ControlFileData *control;
	UpgradeRelinkFile *file = relink_file;
	xl_pg_upgrade_marker *marker = &upgrade_start.marker;
	bool		crc_ok;

	Assert(relink_file != NULL && marker->old_major != 0);
	marker->new_major = new_cluster.major_version;
	control = get_controlfile(new_cluster.pgdata, &crc_ok);
	if (!crc_ok)
		pg_fatal("new cluster control file has an invalid checksum");
	start_upgrade_relink_file(relink_file,
							  control->system_identifier,
							  &upgrade_start);
	pg_free(control);
	write_upgrade_commands();
	Assert(file->count == 0);
	/* ferror retains failures from any of the command writes. */
	if (ferror(file->stream) || fclose(file->stream) != 0)
		pg_fatal("could not write upgrade relink file \"%s\": %m", file->path);
	file->stream = NULL;
	publish_upgrade_relink_file(relink_file);
	pg_free(relink_file);
}
