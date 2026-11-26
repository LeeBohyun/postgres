/*-------------------------------------------------------------------------
 *
 * pgupgrade_config.h
 *    Server configuration for WAL-logged pg_upgrade recovery.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGUPGRADE_CONFIG_H
#define PGUPGRADE_CONFIG_H

#include "utils/guc.h"

#define PG_UPGRADE_XFER_MIRROR (-1)

extern PGDLLIMPORT int pg_upgrade_standby_transfer_mode;
extern PGDLLIMPORT char *pg_upgrade_standby_old_datadir;

extern PGDLLIMPORT const struct config_enum_entry pg_upgrade_standby_transfer_mode_options[];

#endif							/* PGUPGRADE_CONFIG_H */
