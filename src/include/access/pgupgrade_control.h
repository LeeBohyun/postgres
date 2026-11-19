/*-------------------------------------------------------------------------
 *
 * pgupgrade_control.h
 *    Control-file state for WAL-logged pg_upgrade.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/include/access/pgupgrade_control.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGUPGRADE_CONTROL_H
#define PGUPGRADE_CONTROL_H

#include "access/xlogdefs.h"
#include "storage/shmem.h"

extern const ShmemCallbacks PgUpgradeShmemCallbacks;

extern void ArmUpgradeCompletionCheckpoint(XLogRecPtr complete_lsn);
extern XLogRecPtr PgUpgradeGetCompletionLSN(XLogRecPtr checkpoint_lsn);
extern void PgUpgradeClearCompletionLSN(void);
extern void BeginControlFileUpgrade(void);

#endif							/* PGUPGRADE_CONTROL_H */
