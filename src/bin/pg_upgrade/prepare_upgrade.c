/*
 * prepare_upgrade.c
 *
 * Store old and target catalog observations for upgrade WAL emission.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/prepare_upgrade.c
 */

#include "postgres_fe.h"

#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "pg_upgrade.h"
#include "emit_upgrade_wal.h"
#include "prepare_upgrade.h"
#include "upgrade_catalogs.h"

struct UpgradePreparation
{
	UpgradeRelinkFile *file;
	ClusterInfo *old_cluster;
	ClusterInfo *new_cluster;
	transferMode mode;
	xl_pg_upgrade_start window;
	bool		collected[2];
	bool		source_ready;
	bool		finished;
};

static int
compare_storage(const void *a, const void *b)
{
	const		UpgradeCatalogRelation *left = a;
	const		UpgradeCatalogRelation *right = b;

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

static void
mark_transferred_relations(UpgradePreparation * preparation,
						   const UpgradeCatalogDatabase * database,
						   UpgradeCatalogScope * scope)
{
	DbInfo	   *old_database;
	FileNameMap *maps;
	size_t		index = 0;
	int			nmaps;

	if (database->oid == InvalidOid || database->dbinfo == NULL)
		return;
	old_database = find_database(preparation->old_cluster, database->oid);
	if (old_database == NULL)
		return;
	maps = gen_db_file_maps(old_database, database->dbinfo, &nmaps,
							preparation->old_cluster->pgdata,
							preparation->new_cluster->pgdata);
	for (int i = 0; i < nmaps; i++)
	{
		Oid			relation_oid = maps[i].reloid;

		while (index < scope->nrelations &&
			   scope->relations[index].relation_oid < relation_oid)
			index++;
		if (index == scope->nrelations ||
			scope->relations[index].relation_oid != relation_oid)
			pg_fatal("transferred relation %u is missing from the target",
					 relation_oid);
		scope->relations[index++].transferred = true;
	}
	pg_free(maps);
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
	pg_fatal("catalog observation refers to unknown tablespace %u", tablespace);
}

static Oid
next_tablespace(const UpgradeCatalogScope * scope, size_t index,
				Oid default_tablespace, bool default_pending)
{
	Oid			relation_tablespace = index < scope->nrelations ?
		scope->relations[index].tablespace_oid : InvalidOid;

	if (!default_pending)
		return relation_tablespace;
	if (relation_tablespace == InvalidOid)
		return default_tablespace;
	return Min(default_tablespace, relation_tablespace);
}

static uint32
count_directories(const UpgradeCatalogScope * scope, Oid default_tablespace)
{
	size_t		index = 0;
	uint32		count = 0;
	bool		default_pending = true;

	while (default_pending || index < scope->nrelations)
	{
		Oid			tablespace = next_tablespace(scope, index,
												 default_tablespace, default_pending);

		if (count == PG_UINT32_MAX)
			pg_fatal("too many database directories for upgrade WAL");
		count++;
		if (default_pending && tablespace == default_tablespace)
			default_pending = false;
		while (index < scope->nrelations &&
			   scope->relations[index].tablespace_oid == tablespace)
			index++;
	}
	return count;
}

static void
write_catalog_scope(void *arg, UpgradeCatalogSide side,
					const UpgradeCatalogDatabase * database,
					UpgradeCatalogScope * scope)
{
	UpgradePreparation *preparation = arg;
	PgUpgradeCatalogDatabase header = {0};
	size_t		index = 0;
	bool		default_pending = true;
	bool		target = side == UPGRADE_CATALOG_NEW;

	if (target)
		mark_transferred_relations(preparation, database, scope);
	if (scope->nrelations > 1)
		qsort(scope->relations, scope->nrelations,
			  sizeof(*scope->relations), compare_storage);
	for (size_t i = 1; i < scope->nrelations; i++)
		if (compare_storage(&scope->relations[i - 1], &scope->relations[i]) == 0)
			pg_fatal("duplicate physical relation key in database %u",
					 database->oid);
	if (scope->nrelations > PG_UINT32_MAX)
		pg_fatal("too many relations for upgrade WAL in database %u",
				 database->oid);

	header.database_oid = database->oid;
	header.default_tablespace = database->tablespace_oid;
	header.directory_count = count_directories(scope, database->tablespace_oid);
	header.relation_count = (uint32) scope->nrelations;
	if (database->relations_collected)
		header.flags |= PG_UPGRADE_DATABASE_RELATIONS_AVAILABLE;
	if (database->template0)
		header.flags |= PG_UPGRADE_DATABASE_TEMPLATE0;
	begin_upgrade_relink_scope(preparation->file, target, &header);

	while (default_pending || index < scope->nrelations)
	{
		Oid			tablespace = next_tablespace(scope, index,
												 database->tablespace_oid,
												 default_pending);
		PgUpgradeCatalogEntry entry = {.tablespace_oid = tablespace};

		if (target && tablespace_is_inplace(preparation, tablespace))
			entry.flags = PG_UPGRADE_CATALOG_INPLACE;
		append_upgrade_relink_catalog_entry(preparation->file, &entry);
		if (default_pending && tablespace == database->tablespace_oid)
			default_pending = false;
		while (index < scope->nrelations &&
			   scope->relations[index].tablespace_oid == tablespace)
			index++;
	}
	for (size_t i = 0; i < scope->nrelations; i++)
	{
		const		UpgradeCatalogRelation *relation = &scope->relations[i];
		PgUpgradeCatalogEntry entry =
		{
			.tablespace_oid = relation->tablespace_oid,
			.filenumber = relation->filenumber
		};

		if (target)
		{
			entry.relkind = relation->relkind;
			entry.persistence = relation->persistence;
			if (relation->transferred)
				entry.flags = PG_UPGRADE_CATALOG_TRANSFERRED;
		}
		append_upgrade_relink_catalog_entry(preparation->file, &entry);
	}
	end_upgrade_relink_scope(preparation->file);
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
prepare_upgrade_catalogs(UpgradePreparation * preparation,
						 ClusterInfo *cluster, UpgradeCatalogSide side)
{
	if (preparation == NULL || preparation->finished ||
		preparation->collected[side] ||
		(side == UPGRADE_CATALOG_OLD ? cluster != preparation->old_cluster :
		 cluster != preparation->new_cluster) ||
		(side == UPGRADE_CATALOG_NEW &&
		 !preparation->collected[UPGRADE_CATALOG_OLD]))
		pg_fatal("upgrade catalog observations were requested out of order");
	collect_upgrade_catalogs(cluster, side, write_catalog_scope, preparation);
	preparation->collected[side] = true;
}

void
prepare_upgrade_source(UpgradePreparation * preparation)
{
	ClusterInfo *cluster = preparation->old_cluster;
	const ControlData *control = &cluster->controldata;
	xl_pg_upgrade_start *source = &preparation->window;

	if (!preparation->collected[UPGRADE_CATALOG_OLD] ||
		preparation->source_ready)
		pg_fatal("WAL-upgrade source was prepared out of order");
	source->old_system_identifier = control->system_identifier;
	source->boundary_lsn = control->shutdown_checkpoint_end_lsn;
	source->marker.old_major = cluster->major_version;
	source->old_catalog_version = control->cat_ver;
	source->old_control_version = control->ctrl_ver;
	source->block_size = control->blocksz;
	source->relseg_blocks = control->largesz;
	source->wal_block_size = control->walsz;
	source->wal_segment_size = control->walseg;
	source->slru_pages_per_segment = SLRU_PAGES_PER_SEGMENT;
	source->old_tli = control->chkpnt_tli;
	source->transfer_mode = preparation->mode;
	preparation->source_ready = true;
}

void
finish_upgrade_preparation(UpgradePreparation * preparation)
{
	xl_pg_upgrade_marker *marker = &preparation->window.marker;

	if (!preparation->source_ready ||
		!preparation->collected[UPGRADE_CATALOG_NEW] || preparation->finished)
		pg_fatal("WAL-upgrade preparation is incomplete");
	marker->new_major = preparation->new_cluster->major_version;
	snprintf(marker->pg_version, sizeof(marker->pg_version), "%u\n",
			 marker->new_major / 10000);
	set_upgrade_relink_start(preparation->file, &preparation->window);
	finish_upgrade_relink_file(preparation->file, marker);
	preparation->finished = true;
}

void
bind_upgrade_preparation(UpgradePreparation * preparation)
{
	if (preparation == NULL || !preparation->finished)
		pg_fatal("cannot bind an incomplete WAL-upgrade preparation");
	bind_upgrade_relink_file(preparation->file);
}

void
emit_upgrade_wal(UpgradePreparation * preparation, PGconn *conn)
{
	prep_status("Emitting upgrade WAL");
	emit_upgrade_relink_file(preparation->file, conn);
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
