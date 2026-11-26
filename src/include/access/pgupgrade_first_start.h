/*-------------------------------------------------------------------------
 *
 * pgupgrade_first_start.h
 *    First startup of a cluster prepared by pg_upgrade --wal-upgrade.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/include/access/pgupgrade_first_start.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGUPGRADE_FIRST_START_H
#define PGUPGRADE_FIRST_START_H

#include "access/xlogdefs.h"

#include "access/xlogdefs.h"

extern bool PgUpgradeFirstStartPending(void);
extern bool PgUpgradeFirstStartInProgress(void);
extern XLogRecPtr PgUpgradeFirstStartRedo(void);
extern void PgUpgradePrepareFirstStart(void);
extern void PgUpgradeRunFirstStart(void);
extern XLogRecPtr PgUpgradeCompletionCheckPoint(XLogRecPtr complete_lsn);

#endif
