/*-------------------------------------------------------------------------
 *
 * control.c
 *    Control-file setup and completion state for WAL-logged pg_upgrade.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/control.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/pgupgrade_config.h"
#include "access/pgupgrade_control.h"
#include "access/pgupgrade_wal.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/catversion.h"
#include "common/controldata_utils.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "storage/subsystems.h"

typedef struct PgUpgradeControlData
{
	OldUpgradeControlData archive_old_control;
	bool		archive_old_control_valid;
} PgUpgradeControlData;

static OldUpgradeControlData local_archive_old_control;
static bool local_archive_old_control_valid;
static PgUpgradeControlData *upgrade_control;

static void pgupgrade_shmem_request(void *arg);
static void pgupgrade_shmem_init(void *arg);

const ShmemCallbacks PgUpgradeShmemCallbacks = {
	.request_fn = pgupgrade_shmem_request,
	.init_fn = pgupgrade_shmem_init,
};

static void
pgupgrade_shmem_request(void *arg)
{
	ShmemRequestStruct(.name = "WAL Upgrade Control",
					   .size = sizeof(PgUpgradeControlData),
					   .ptr = (void **) &upgrade_control,
		);
}

static void
pgupgrade_shmem_init(void *arg)
{
	MemSet(upgrade_control, 0, sizeof(PgUpgradeControlData));
	upgrade_control->archive_old_control = local_archive_old_control;
	upgrade_control->archive_old_control_valid = local_archive_old_control_valid;
}

void
PgUpgradeValidateControlImage(const char *data, Size len,
							  const ControlFileData *control_file,
							  ControlFileData *image)
{
	pg_crc32c	crc;

	if (len != PG_CONTROL_FILE_SIZE)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_control image in pg_upgrade WAL has size %zu, expected %d",
						len, PG_CONTROL_FILE_SIZE)));

	memcpy(image, data, sizeof(*image));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, image, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, image->crc))
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("incorrect checksum in pg_control image from pg_upgrade WAL")));

#define CHECK_UPGRADE_CONTROL_FIELD(field) \
	do { \
		if (image->field != control_file->field) \
			ereport(PANIC, \
					(errcode(ERRCODE_DATA_CORRUPTED), \
					 errmsg("pg_control image in pg_upgrade WAL has incompatible field \"%s\"", \
							#field))); \
	} while (0)

	CHECK_UPGRADE_CONTROL_FIELD(system_identifier);
	CHECK_UPGRADE_CONTROL_FIELD(pg_control_version);
	CHECK_UPGRADE_CONTROL_FIELD(catalog_version_no);
	CHECK_UPGRADE_CONTROL_FIELD(maxAlign);
	CHECK_UPGRADE_CONTROL_FIELD(floatFormat);
	CHECK_UPGRADE_CONTROL_FIELD(blcksz);
	CHECK_UPGRADE_CONTROL_FIELD(relseg_size);
	CHECK_UPGRADE_CONTROL_FIELD(slru_pages_per_segment);
	CHECK_UPGRADE_CONTROL_FIELD(xlog_blcksz);
	CHECK_UPGRADE_CONTROL_FIELD(xlog_seg_size);
	CHECK_UPGRADE_CONTROL_FIELD(nameDataLen);
	CHECK_UPGRADE_CONTROL_FIELD(indexMaxKeys);
	CHECK_UPGRADE_CONTROL_FIELD(toast_max_chunk_size);
	CHECK_UPGRADE_CONTROL_FIELD(loblksize);
	CHECK_UPGRADE_CONTROL_FIELD(float8ByVal);

#undef CHECK_UPGRADE_CONTROL_FIELD

	if (image->backupEndPoint != PG_INT64_MAX ||
		control_file->backupEndPoint != PG_INT64_MAX)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_control image in pg_upgrade WAL has invalid upgrade state")));

	if (image->data_checksum_version_init != PG_DATA_CHECKSUM_OFF &&
		image->data_checksum_version_init != PG_DATA_CHECKSUM_VERSION)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_control image in pg_upgrade WAL has invalid initial checksum state %u",
						image->data_checksum_version_init)));
	if (image->data_checksum_version != PG_DATA_CHECKSUM_OFF &&
		image->data_checksum_version != PG_DATA_CHECKSUM_VERSION)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_control image in pg_upgrade WAL has invalid checksum state %u",
						image->data_checksum_version)));
}

bool
PgUpgradeGetArchiveOldControlData(OldUpgradeControlData *result)
{
	if (!upgrade_control->archive_old_control_valid)
		return false;
	*result = upgrade_control->archive_old_control;
	return true;
}

/*
 * Create startup files for upgrade recovery.  Read the WAL segment size from
 * the retained old standby or staged archive WAL.  When replacing old-version
 * files, keep any valid same-version pg_control.
 */
static void
synthesize_upgrade_startup_files(bool archive_recovery)
{
	ControlFileData cf;
	OldUpgradeControlData old_control;
	char		verpath[MAXPGPATH];
	char		ctlpath[MAXPGPATH];
	int			fd;
	int			upgrade_wal_segment_size;

	snprintf(ctlpath, sizeof(ctlpath), "%s/%s", DataDir, XLOG_CONTROL_FILE);
	local_archive_old_control_valid = false;

	if (archive_recovery)
	{
		struct stat st;

		if (stat(ctlpath, &st) == 0)
		{
			bool		existing_crc_ok = false;
			ControlFileData *existing = get_controlfile(DataDir, &existing_crc_ok);

			if (existing != NULL)
			{
				bool		usable = (existing_crc_ok &&
									  existing->pg_control_version == PG_CONTROL_VERSION &&
									  existing->catalog_version_no == CATALOG_VERSION_NO);
				char		opts_path[MAXPGPATH];

				/* Save old-major fields before replacing its startup files. */
				snprintf(opts_path, sizeof(opts_path), "%s/postmaster.opts", DataDir);
				if ((!usable || existing->system_identifier != 0) &&
					!(usable && existing->backupEndPoint == PG_INT64_MAX) &&
					(!usable || existing->state == DB_SHUTDOWNED ||
					 existing->state == DB_SHUTDOWNED_IN_RECOVERY) &&
					stat(opts_path, &st) == 0)
				{
					ReadArchiveUpgradeControlData(DataDir,
												  &local_archive_old_control);
					local_archive_old_control_valid = true;
				}

				pfree(existing);
				if (usable)
				{
					ereport(LOG,
							(errmsg("keeping the existing new-version control file for upgrade-stream recovery"),
							 errdetail("A valid same-version pg_control is present; not synthesizing over it (preserves its data checksum state).")));
					return;
				}
			}
		}
	}

	if (!archive_recovery)
	{
		if (pg_upgrade_standby_old_datadir == NULL ||
			pg_upgrade_standby_old_datadir[0] == '\0')
			ereport(FATAL,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("streaming pg_upgrade skeleton requires \"pg_upgrade_standby_old_datadir\"")));
		ReadOldUpgradeControlData(pg_upgrade_standby_old_datadir, &old_control);
		upgrade_wal_segment_size = old_control.wal_segment_size;
	}
	else
		upgrade_wal_segment_size = GetUpgradeArchiveWalSegmentSize();

	InitControlFileData(&cf, 0, PG_DATA_CHECKSUM_OFF,
						upgrade_wal_segment_size, FATAL);

	{
		static const char *const subdirs[] = {
			"global", "base", "pg_wal", "pg_wal/archive_status", "pg_wal/summaries",
			"pg_commit_ts", "pg_dynshmem", "pg_notify", "pg_serial",
			"pg_snapshots", "pg_subtrans", "pg_twophase",
			"pg_multixact", "pg_multixact/members", "pg_multixact/offsets",
			"pg_replslot", "pg_tblspc", "pg_stat", "pg_stat_tmp", "pg_xact",
			"pg_logical", "pg_logical/snapshots", "pg_logical/mappings"
		};

		for (int s = 0; s < (int) lengthof(subdirs); s++)
		{
			char		dpath[MAXPGPATH];

			snprintf(dpath, sizeof(dpath), "%s/%s", DataDir, subdirs[s]);
			if (MakePGDirectory(dpath) != 0 && errno != EEXIST)
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("could not create directory \"%s\": %m", dpath)));
		}
	}

	WriteControlFileData(&cf, ctlpath, !archive_recovery, FATAL);

	snprintf(verpath, sizeof(verpath), "%s/PG_VERSION", DataDir);
	fd = BasicOpenFile(verpath, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", verpath)));
	if (write(fd, PG_MAJORVERSION "\n", strlen(PG_MAJORVERSION) + 1) !=
		(int) (strlen(PG_MAJORVERSION) + 1))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", verpath)));
	if (pg_fsync(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", verpath)));
	if (close(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", verpath)));

	ereport(LOG,
			(errmsg("synthesized a fresh pg_control and PG_VERSION for an upgrade-stream standby"),
			 errdetail("The standby will stream and replay the upgrade "
					   "WAL from its primary; no initdb was required.")));
}

void
PgUpgradePrepareStartupFiles(void)
{
	char		path[MAXPGPATH];
	struct stat st;
	UpgradeRecoveryMode mode = GetUpgradeRecoveryMode();

	if (mode == UPGRADE_RECOVERY_NONE)
		return;
	snprintf(path, sizeof(path), "%s/PG_VERSION", DataDir);
	if (mode == UPGRADE_RECOVERY_STANDBY && stat(path, &st) == 0)
		return;
	synthesize_upgrade_startup_files(mode == UPGRADE_RECOVERY_ARCHIVE);
}
