/*-------------------------------------------------------------------------
 *
 * pgupgrade_handoff.h
 *    Old-cluster handoff for WAL-logged pg_upgrade.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/include/access/pgupgrade_handoff.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGUPGRADE_HANDOFF_H
#define PGUPGRADE_HANDOFF_H

#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "nodes/pg_list.h"

typedef enum PgUpgradeHandoffPhase
{
	PG_UPGRADE_HANDOFF_NONE,
	PG_UPGRADE_HANDOFF_PENDING,
	PG_UPGRADE_HANDOFF_DURABLE_PAUSE,
} PgUpgradeHandoffPhase;

extern XLogRecPtr PgUpgradeEmitHandoffIfArmed(void);
extern bool PgUpgradeHandoffIsArmed(void);
extern bool PgUpgradeHandoffSlotsAreFrozen(void);
extern void PgUpgradeFindPersistedHandoff(const ControlFileData *control_file,
										  XLogReaderState *checkpoint_reader,
										  const CheckPoint *checkpoint,
										  TimeLineID checkpoint_tli,
										  List *timeline_history,
										  bool standby_mode_requested);
extern void PgUpgradeResetHandoffRecovery(void);
extern void PgUpgradeInitializeHandoffRecovery(void);
extern bool PgUpgradeRestoreHandoff(XLogRecPtr min_recovery_point);
extern void PgUpgradeCancelHandoffIfRequested(void);
extern void PgUpgradeUpdateHandoffPause(bool recovery_pause);
extern void PgUpgradeCheckPromotion(void);
extern void PgUpgradeDiscardStaleHandoff(DBState state);

extern void BeginPgUpgradeHandoff(XLogRecPtr lsn);
extern void CompletePgUpgradeHandoff(void);
extern void CancelPgUpgradeHandoff(void);
extern XLogRecPtr GetPgUpgradeHandoffRetention(void);
extern bool PgUpgradeHandoffCancellationRequested(void);

#endif							/* PGUPGRADE_HANDOFF_H */
