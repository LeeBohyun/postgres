/*-------------------------------------------------------------------------
 *
 * pgupgrade_retention_slots.h
 *    Recreate slots that retain the pg_upgrade transaction.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/include/access/pgupgrade_retention_slots.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGUPGRADE_RETENTION_SLOTS_H
#define PGUPGRADE_RETENTION_SLOTS_H

#include "access/xlogdefs.h"

extern void PgUpgradePreparePhysicalSlotMigration(const char *old_datadir,
												  uint32 major_version,
												  XLogRecPtr checkpoint_end_lsn);
extern void PgUpgradeCreateMigratedPhysicalSlots(XLogRecPtr replay_start_lsn);

#endif							/* PGUPGRADE_RETENTION_SLOTS_H */
