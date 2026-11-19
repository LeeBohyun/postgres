/*
 * prepare_upgrade.c
 *
 * Prepare the relink file and START record data for upgrade WAL emission.
 * Copy old WAL and timeline history needed for recovery.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/prepare_upgrade.c
 */

#include "postgres_fe.h"

#include <dirent.h>
#include <fcntl.h>

#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/string.h"
#include "lib/stringinfo.h"
#include "pg_upgrade.h"
#include "prepare_upgrade.h"
#include "relink_file.h"

/*
 * Track the relink file and START record data until upgrade WAL emission.
 */
struct UpgradePreparation
{
	UpgradeRelinkFile *file;
	ClusterInfo *old_cluster;
	ClusterInfo *new_cluster;
	transferMode mode;
	xl_pg_upgrade_start start;
	bool		collected[2];
	bool		old_cluster_ready;
	bool		finished;
};

static int
compare_storage(const void *a, const void *b)
{
	const PgUpgradeRelinkEntry *left = a;
	const PgUpgradeRelinkEntry *right = b;

	if (left->tablespace_oid != right->tablespace_oid)
		return left->tablespace_oid < right->tablespace_oid ? -1 : 1;
	return (left->filenumber > right->filenumber) -
		(left->filenumber < right->filenumber);
}

static DbInfo *
find_database(ClusterInfo *cluster, Oid oid)
{
	size_t		low = 0;
	size_t		high = cluster->dbarr.ndbs;

	while (low < high)
	{
		size_t		mid = low + (high - low) / 2;
		DbInfo	   *database = &cluster->dbarr.dbs[mid];

		if (database->db_oid == oid)
			return database;
		if (database->db_oid < oid)
			low = mid + 1;
		else
			high = mid;
	}
	return NULL;
}

static bool
tablespace_is_inplace(const UpgradePreparation * preparation, Oid tablespace)
{
	if (tablespace == DEFAULTTABLESPACE_OID ||
		tablespace == GLOBALTABLESPACE_OID)
		return false;
	for (int i = 0; i < preparation->new_cluster->num_tablespaces; i++)
		if (preparation->new_cluster->tablespace_oids[i] == tablespace)
			return strcmp(preparation->old_cluster->tablespaces[i],
						  preparation->new_cluster->tablespaces[i]) != 0;
	pg_fatal("relink input refers to unknown tablespace %u", tablespace);
}

static void
write_relink_scope(UpgradePreparation *preparation, bool new_section,
				   Oid database, Oid tablespace, bool recreate,
				   RelinkInfoArr *inventory)
{
	PgUpgradeRelinkDatabase header = {0};

	if (inventory->nrels > 1)
		qsort(inventory->rels, inventory->nrels,
			  sizeof(*inventory->rels), compare_storage);
	for (int i = 1; i < inventory->nrels; i++)
		if (compare_storage(&inventory->rels[i - 1], &inventory->rels[i]) == 0)
			pg_fatal("duplicate physical relation key in database %u",
					 database);

	header.database_oid = database;
	header.default_tablespace = tablespace;
	header.relation_count = inventory->nrels;
	if (recreate)
		header.flags |= PG_UPGRADE_RELINK_RECREATE_SCOPE;
	if (new_section && tablespace_is_inplace(preparation, tablespace))
		header.flags |= PG_UPGRADE_RELINK_DEFAULT_INPLACE;
	begin_upgrade_relink_scope(preparation->file, new_section, &header);

	for (int i = 0; i < inventory->nrels; i++)
	{
		PgUpgradeRelinkEntry entry = inventory->rels[i];

		if (!new_section)
			entry.persistence = 0;
		else if (entry.tablespace_oid != tablespace &&
				 tablespace_is_inplace(preparation, entry.tablespace_oid))
			entry.flags |= UPGRADE_RELINK_INPLACE;
		append_upgrade_relink_entry(preparation->file, &entry);
	}
	pg_free(inventory->rels);
	inventory->rels = NULL;
	inventory->nrels = 0;
}

UpgradePreparation *
create_upgrade_preparation(ClusterInfo *old_cluster, ClusterInfo *new_cluster,
						   transferMode mode)
{
	UpgradePreparation *preparation = pg_malloc0_object(UpgradePreparation);

	preparation->file = create_upgrade_relink_file(new_cluster->pgdata);
	preparation->old_cluster = old_cluster;
	preparation->new_cluster = new_cluster;
	preparation->mode = mode;
	return preparation;
}

void
prepare_upgrade_relink_input(UpgradePreparation *preparation,
							 ClusterInfo *cluster, RelinkInputSide side,
							 const UpgradeTask *task)
{
	bool		new_section = side == RELINK_INPUT_NEW;
	bool		template0_written = false;
	RelinkInfoArr template0 = {0};
	int			batch_size = Max(1, user_opts.jobs);
	int			batch_end = Min(batch_size, cluster->dbarr.ndbs);

	if (preparation == NULL || preparation->finished ||
		preparation->collected[side] ||
		(side == RELINK_INPUT_OLD ? cluster != preparation->old_cluster :
		 cluster != preparation->new_cluster) ||
		(side == RELINK_INPUT_NEW &&
		 !preparation->collected[RELINK_INPUT_OLD]))
		pg_fatal("upgrade relink input was requested out of order");
	if (new_section)
	{
		begin_upgrade_relink_new(preparation->file);
		template0 = get_template0_rel_infos(cluster);
	}
	upgrade_task_run_range(task, cluster, 0, batch_end);
	write_relink_scope(preparation, new_section, InvalidOid, GLOBALTABLESPACE_OID,
					   false, &cluster->shared_relink_arr);
	for (int i = 0; i < cluster->dbarr.ndbs || !template0_written;)
	{
		DbInfo	   *database = i < cluster->dbarr.ndbs ? &cluster->dbarr.dbs[i] : NULL;

		/* Collect the next batch after writing and freeing the previous one. */
		if (database != NULL && i == batch_end)
		{
			batch_end = i + Min(batch_size, cluster->dbarr.ndbs - i);
			upgrade_task_run_range(task, cluster, i, batch_end);
		}

		/* Insert template0 into the OID-ordered database list. */
		if (!template0_written &&
			(database == NULL || cluster->template0->db_oid < database->db_oid))
		{
			write_relink_scope(preparation, new_section,
							   cluster->template0->db_oid,
							   cluster->template0->db_tablespace_oid,
							   !new_section, &template0);
			template0_written = true;
			continue;
		}
		if (new_section)
		{
			DbInfo	   *old_database = find_database(preparation->old_cluster,
													 database->db_oid);

			if (old_database != NULL)
			{
				int			nmaps;
				FileNameMap *maps = gen_db_file_maps(old_database, database, &nmaps,
													 preparation->old_cluster->pgdata,
													 preparation->new_cluster->pgdata);

				pg_free(maps);
			}
		}
		write_relink_scope(preparation, new_section, database->db_oid,
						   database->db_tablespace_oid, false, &database->relink_arr);
		i++;
	}
	preparation->collected[side] = true;
}

/*
 * Fill START with the old system identifier, version and storage parameters,
 * timeline, transfer mode, and final shutdown checkpoint's end LSN.
 */
void
prepare_upgrade_old_cluster(UpgradePreparation *preparation)
{
	ClusterInfo *cluster = preparation->old_cluster;
	const ControlData *control = &cluster->controldata;
	xl_pg_upgrade_start *start = &preparation->start;

	if (!preparation->collected[RELINK_INPUT_OLD] ||
		preparation->old_cluster_ready)
		pg_fatal("old cluster was prepared for WAL upgrade out of order");
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
	start->transfer_mode = preparation->mode;
	preparation->old_cluster_ready = true;
}

/*
 * Check the new pg_control checksum, finish START and the relink-file header,
 * then checksum and close the file before the backend reads it.
 */
void
finish_upgrade_preparation(UpgradePreparation * preparation)
{
	ControlFileData *control;
	xl_pg_upgrade_marker *marker = &preparation->start.marker;
	bool		crc_ok;

	if (!preparation->old_cluster_ready ||
		!preparation->collected[RELINK_INPUT_NEW] || preparation->finished)
		pg_fatal("WAL-upgrade preparation is incomplete");
	marker->new_major = preparation->new_cluster->major_version;
	snprintf(marker->pg_version, sizeof(marker->pg_version), "%u\n",
			 marker->new_major / 10000);
	control = get_controlfile(preparation->new_cluster->pgdata, &crc_ok);
	if (!crc_ok)
		pg_fatal("new cluster control file has an invalid checksum");
	finish_upgrade_relink_file(preparation->file,
							   control->system_identifier,
							   &preparation->start);
	pg_free(control);
	preparation->finished = true;
}

void
emit_upgrade_wal(UpgradePreparation *preparation, PGconn *conn)
{
	prep_status("Emitting upgrade WAL");
	invoke_upgrade_wal_emission(preparation->file, conn);
	check_ok();
}

void
free_upgrade_preparation(UpgradePreparation * preparation)
{
	if (preparation == NULL)
		return;
	free_upgrade_relink_file(preparation->file);
	pg_free(preparation);
}

/* Copy history files for recovery on the retained old-cluster timeline. */
static void
copy_wal_timeline_history(void)
{
	char		waldir[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	snprintf(waldir, sizeof(waldir), "%s/pg_wal", old_cluster.pgdata);
	dir = opendir(waldir);
	if (dir == NULL)
		pg_fatal("could not open directory \"%s\": %m", waldir);
	while ((de = readdir(dir)) != NULL)
	{
		char		source[MAXPGPATH];
		char		dst[MAXPGPATH];

		if (!IsTLHistoryFileName(de->d_name))
			continue;
		snprintf(source, sizeof(source), "%s/%s", waldir, de->d_name);
		snprintf(dst, sizeof(dst), "%s/pg_wal/%s", new_cluster.pgdata,
				 de->d_name);
		copyFile(source, dst, "pg_wal", de->d_name);
	}
	closedir(dir);
}

/* Stage the segment containing the final old checkpoint. */
static void
stage_old_checkpoint_wal(void)
{
	ControlData *control = &old_cluster.controldata;
	const char *name = control->chkpnt_redo_wal_file;
	char		source[MAXPGPATH];
	char		dst[MAXPGPATH];
	char		status[MAXPGPATH];
	struct stat st;
	int			fd;

	snprintf(status, sizeof(status), "%s/pg_wal/archive_status/%s.done",
			 old_cluster.pgdata, name);
	if (stat(status, &st) == 0)
	{
		if (!S_ISREG(st.st_mode))
			pg_fatal("invalid old archive status file \"%s\"", status);
		return;
	}
	if (errno != ENOENT)
		pg_fatal("could not inspect old archive status \"%s\": %m", status);

	snprintf(source, sizeof(source), "%s/pg_wal/%s", old_cluster.pgdata, name);
	snprintf(dst, sizeof(dst), "%s/pg_wal/%s", new_cluster.pgdata, name);
	if (stat(source, &st) != 0 || !S_ISREG(st.st_mode) ||
		st.st_size != control->walseg)
		pg_fatal("old checkpoint WAL file \"%s\" is missing or has an invalid size",
				 source);
	snprintf(status, sizeof(status), "%s/pg_wal/archive_status/%s.done",
			 new_cluster.pgdata, name);
	if (stat(status, &st) == 0)
		pg_fatal("old checkpoint archive status already exists: \"%s\"", status);
	if (errno != ENOENT)
		pg_fatal("could not inspect archive status \"%s\": %m", status);
	/* Reject an existing destination instead of replacing archived WAL. */
	copyFile(source, dst, "pg_wal", name);
	if (fsync_fname(dst, false) != 0 || fsync_parent_path(dst) != 0)
		pg_fatal("could not synchronize staged old checkpoint WAL file \"%s\"",
				 dst);

	snprintf(status, sizeof(status), "%s/pg_wal/archive_status/%s.ready",
			 new_cluster.pgdata, name);
	fd = open(status, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY,
			  pg_file_create_mode);
	if (fd < 0)
		pg_fatal("could not create old checkpoint archive status \"%s\": %m",
				 status);
	if (close(fd) != 0 || fsync_fname(status, false) != 0 ||
		fsync_parent_path(status) != 0)
		pg_fatal("could not synchronize old checkpoint archive status \"%s\": %m",
				 status);
}

void
prepare_upgrade_archive(bool stage_checkpoint)
{
	copy_wal_timeline_history();
	if (stage_checkpoint)
		stage_old_checkpoint_wal();
}

char *
get_old_cluster_archive_command(void)
{
	PGconn	   *conn = connectToServer(&old_cluster, "template1");
	PGresult   *res;
	char	   *mode;
	char	   *cmd;
	char	   *result = NULL;

	res = executeQueryOrDie(conn,
							"SELECT current_setting('archive_mode'), "
							"current_setting('archive_command')");
	mode = PQgetvalue(res, 0, 0);
	cmd = PQgetvalue(res, 0, 1);

	if (strcmp(mode, "off") != 0 &&
		cmd[0] != '\0' &&
		strcmp(cmd, "(disabled)") != 0)
		result = pg_strdup(cmd);

	PQclear(res);
	PQfinish(conn);
	return result;
}

void
configure_upgrade_archive(const char *archive_command)
{
	PGconn	   *conn = connectToServer(&new_cluster, "template1");
	char	   *literal = PQescapeLiteral(conn, archive_command,
									 strlen(archive_command));

	if (literal == NULL)
		pg_fatal("could not quote archive_command: %s", PQerrorMessage(conn));
	PQclear(executeQueryOrDie(conn, "ALTER SYSTEM SET archive_mode = 'on'"));
	PQclear(executeQueryOrDie(conn, "ALTER SYSTEM SET archive_command = %s",
							  literal));
	PQfreemem(literal);
	PQfinish(conn);
}
