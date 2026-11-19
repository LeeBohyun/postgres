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

#include "access/pgupgrade_wal.h"
#include "access/xlogdefs.h"
#include "catalog/pg_control.h"

extern void ArmControlFileForUpgradeRecovery(
											 const CheckPoint *replay_start_checkpoint,
											 uint64 wal_sysid, bool for_streaming);
extern void AdoptUpgradeControlFile(const char *data, Size len);
extern void VerifyUpgradeRestartPoint(XLogRecPtr checkpoint_lsn,
									  TimeLineID checkpoint_tli);
extern void PgUpgradePrepareStartupFiles(void);
extern void PgUpgradeValidateControlImage(const char *data, Size len,
										  const ControlFileData *control_file,
										  ControlFileData *image);

extern void SetControlFileUpgradeFinalized(void);
extern void SetControlFileUpgradeComplete(XLogRecPtr end_lsn,
										  TimeLineID replay_tli);
extern void ArmUpgradeCompletionCheckpoint(XLogRecPtr complete_lsn);
extern XLogRecPtr PgUpgradeGetCompletionLSN(XLogRecPtr checkpoint_lsn);
extern void PgUpgradeClearCompletionLSN(void);
extern void GetControlFileUpgradeState(bool *started, bool *finalized);
extern void SetControlFileUpgradeStarted(void);
extern void BeginControlFileUpgrade(void);

extern bool PgUpgradeGetArchiveOldControlData(OldUpgradeControlData *result);

#endif							/* PGUPGRADE_CONTROL_H */
