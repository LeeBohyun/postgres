/*-------------------------------------------------------------------------
 *
 * config.c
 *    Server configuration for WAL-logged pg_upgrade recovery.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/config.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/pgupgrade_config.h"
#include "common/pg_upgrade_records.h"

int			pg_upgrade_standby_transfer_mode = PG_UPGRADE_XFER_MIRROR;
char	   *pg_upgrade_standby_old_datadir = NULL;

const struct config_enum_entry pg_upgrade_standby_transfer_mode_options[] = {
	{"mirror", PG_UPGRADE_XFER_MIRROR, false},
	{"clone", UPGRADE_RELINK_MODE_CLONE, false},
	{"copy", UPGRADE_RELINK_MODE_COPY, false},
	{"copy_file_range", UPGRADE_RELINK_MODE_COPY_FILE_RANGE, false},
	{"link", UPGRADE_RELINK_MODE_LINK, false},
	{"swap", UPGRADE_RELINK_MODE_SWAP, false},
	{NULL, 0, false}
};
