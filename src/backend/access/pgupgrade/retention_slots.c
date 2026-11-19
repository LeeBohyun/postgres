/*-------------------------------------------------------------------------
 *
 * retention_slots.c
 *    Recreate slots that retain the pg_upgrade transaction.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/retention_slots.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>

#include "access/pgupgrade_retention_slots.h"
#include "access/xlog.h"
#include "common/file_utils.h"
#include "common/string.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "replication/slot.h"
#include "storage/fd.h"
#include "utils/memutils.h"

#define OLD_REPLICATION_SLOT_MAGIC 0x1051CA1

typedef struct OldReplicationSlotPrefix
{
	uint32		magic;
	pg_crc32c	checksum;
	uint32		version;
	uint32		length;
	NameData	name;
	Oid			database;
	ReplicationSlotPersistency persistency;
	TransactionId xmin;
	TransactionId catalog_xmin;
	XLogRecPtr	restart_lsn;
	union
	{
		XLogRecPtr	invalidated_at;
		uint32		invalidation_cause;
	}			invalidation;
}			OldReplicationSlotPrefix;

typedef struct PgUpgradeSlotMigration
{
	List	   *names;
	bool		prepared;
}			PgUpgradeSlotMigration;

static PgUpgradeSlotMigration slot_migration;

static uint32
old_replication_slot_version(uint32 major_version)
{
	uint32		old_major = major_version / 10000;

	if (old_major == 14 || old_major == 15)
		return 2;
	if (old_major == 16)
		return 3;
	if (old_major >= 17 && old_major <= 20)
		return 5;

	elog(FATAL, "unsupported old major version %u for replication slot migration",
		 old_major);
	pg_unreachable();
}

static List *
read_old_physical_slot_names(const char *old_datadir, uint32 major_version,
							 XLogRecPtr checkpoint_end_lsn)
{
	char		slotdir[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	List	   *names = NIL;
	uint32		old_major = major_version / 10000;
	uint32		expected_version = 0;

	if (snprintf(slotdir, sizeof(slotdir), "%s/%s", old_datadir,
				 PG_REPLSLOT_DIR) >= sizeof(slotdir))
		elog(FATAL, "old replication slot directory path is too long");

	dir = AllocateDir(slotdir);
	if (dir == NULL && errno == ENOENT && major_version < 90400)
		return NIL;
	while ((de = ReadDirExtended(dir, slotdir, FATAL)) != NULL)
	{
		char		slotpath[MAXPGPATH];
		char		path[MAXPGPATH];
		int			fd;
		struct stat st;
		OldReplicationSlotPrefix *slot;
		char	   *contents;
		size_t		total_size;
		pg_crc32c	checksum;
		bool		invalidated;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0 ||
			pg_str_endswith(de->d_name, ".tmp"))
			continue;
		if (snprintf(slotpath, sizeof(slotpath), "%s/%s", slotdir,
					 de->d_name) >= sizeof(slotpath))
			elog(FATAL, "old replication slot path is too long");
		if (get_dirent_type(slotpath, de, false, FATAL) != PGFILETYPE_DIR)
			continue;
		if (expected_version == 0)
			expected_version = old_replication_slot_version(major_version);
		if (snprintf(path, sizeof(path), "%s/state", slotpath) >= sizeof(path))
			elog(FATAL, "old replication slot state path is too long");

		fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read old replication slot file \"%s\": %m", path)));

		if (st.st_size < sizeof(OldReplicationSlotPrefix) ||
			st.st_size > MaxAllocSize)
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("old replication slot file \"%s\" has an invalid length", path)));
		total_size = st.st_size;
		contents = palloc(total_size);
		if (pg_pread(fd, contents, total_size, 0) != total_size ||
			CloseTransientFile(fd) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read old replication slot file \"%s\": %m", path)));

		slot = (OldReplicationSlotPrefix *) contents;
		if (slot->magic != OLD_REPLICATION_SLOT_MAGIC ||
			slot->version != expected_version ||
			slot->length != total_size - offsetof(OldReplicationSlotPrefix, name))
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("old replication slot file \"%s\" has an invalid header", path)));

		INIT_CRC32C(checksum);
		COMP_CRC32C(checksum,
					contents + offsetof(OldReplicationSlotPrefix, version),
					total_size - offsetof(OldReplicationSlotPrefix, version));
		FIN_CRC32C(checksum);
		if (!EQ_CRC32C(checksum, slot->checksum))
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("old replication slot file \"%s\" has an invalid checksum",
							path)));

		if (strnlen(NameStr(slot->name), NAMEDATALEN) == NAMEDATALEN ||
			strcmp(NameStr(slot->name), de->d_name) != 0)
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("old replication slot file \"%s\" has an invalid name",
							path)));

		if (slot->version == 2)
			invalidated = XLogRecPtrIsValid(slot->invalidation.invalidated_at);
		else
			invalidated = slot->invalidation.invalidation_cause != 0;

		if (slot->persistency == RS_PERSISTENT && slot->database == InvalidOid)
		{
			if (strcmp(NameStr(slot->name), CONFLICT_DETECTION_SLOT) == 0)
			{
				if (old_major >= 19)
				{
					pfree(contents);
					continue;
				}
				ereport(FATAL,
						(errcode(ERRCODE_RESERVED_NAME),
						 errmsg("old physical replication slot \"%s\" uses a reserved name",
								NameStr(slot->name)),
						 errhint("Drop or rename the slot before starting pg_upgrade.")));
			}
			ReplicationSlotValidateName(NameStr(slot->name), false, FATAL);
			if (!XLogRecPtrIsValid(slot->restart_lsn) || invalidated)
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("old physical replication slot \"%s\" is not valid",
								NameStr(slot->name))));
			if (slot->restart_lsn < checkpoint_end_lsn)
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("old physical replication slot \"%s\" has not durably received the final shutdown checkpoint",
								NameStr(slot->name))));
			names = lappend(names, pstrdup(NameStr(slot->name)));
		}
		pfree(contents);
	}
	FreeDir(dir);

	return names;
}

void
PgUpgradePreparePhysicalSlotMigration(const char *old_datadir,
									  uint32 major_version,
									  XLogRecPtr checkpoint_end_lsn)
{
	Assert(!slot_migration.prepared);
	slot_migration.names = read_old_physical_slot_names(old_datadir,
														major_version,
														checkpoint_end_lsn);
	slot_migration.prepared = true;
}

void
PgUpgradeCreateMigratedPhysicalSlots(XLogRecPtr replay_start_lsn)
{
	int			used_slots = 0;
	int			missing_slots = 0;

	if (!slot_migration.prepared)
		return;
	if (slot_migration.names == NIL)
	{
		slot_migration.prepared = false;
		return;
	}
	if (!enableFsync)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("migrated physical replication slots require \"fsync\" to be enabled")));
	if (max_slot_wal_keep_size_mb != -1)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("migrated physical replication slots require \"max_slot_wal_keep_size\" to be -1")));
	if (idle_replication_slot_timeout_secs != 0)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("migrated physical replication slots require \"idle_replication_slot_timeout\" to be 0")));
	if (max_replication_slots == 0 || wal_level < WAL_LEVEL_REPLICA)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("migrated physical replication slots require replication slots and \"wal_level\" of \"replica\" or higher")));

	for (int i = 0; i < max_replication_slots; i++)
		if (ReplicationSlotCtl->replication_slots[i].in_use)
			used_slots++;

	foreach_ptr(char, name, slot_migration.names)
	{
		if (SearchNamedReplicationSlot(name, true) == NULL)
		{
			missing_slots++;
			continue;
		}

		ReplicationSlotAcquire(name, true, true);
		if (ReplicationSlotIndex(MyReplicationSlot) >= max_replication_slots ||
			!SlotIsPhysical(MyReplicationSlot) ||
			MyReplicationSlot->data.persistency != RS_PERSISTENT ||
			MyReplicationSlot->data.restart_lsn != replay_start_lsn ||
			MyReplicationSlot->last_saved_restart_lsn != replay_start_lsn)
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("replication slot \"%s\" is not an inactive persistent physical slot at the pg_upgrade replay start LSN",
							name)));
		ReplicationSlotRelease();
	}

	if (used_slots + missing_slots > max_replication_slots)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("not enough replication slots for the migrated physical slots"),
				 errhint("Increase \"max_replication_slots\".")));

	foreach_ptr(char, name, slot_migration.names)
	{
		if (SearchNamedReplicationSlot(name, true) != NULL)
			continue;

		ReplicationSlotCreate(name, false, RS_EPHEMERAL, false, false,
							  false, false);
		ReplicationSlotReserveWal();
		Assert(MyReplicationSlot->data.restart_lsn == replay_start_lsn);
		ReplicationSlotPersist();
		Assert(MyReplicationSlot->last_saved_restart_lsn == replay_start_lsn);
		ReplicationSlotRelease();
	}

	list_free_deep(slot_migration.names);
	slot_migration.names = NIL;
	slot_migration.prepared = false;
}
