/*-------------------------------------------------------------------------
 *
 * pgupgrade_handoff_receipt.h
 *    Wait for durable receipt of a WAL-logged pg_upgrade handoff.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/include/access/pgupgrade_handoff_receipt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGUPGRADE_HANDOFF_RECEIPT_H
#define PGUPGRADE_HANDOFF_RECEIPT_H

#include "access/xlogdefs.h"

extern void PgUpgradePrepareHandoffSlots(void);
extern void PgUpgradeCancelHandoffSlots(void);
extern bool PgUpgradeWaitForHandoffSlots(XLogRecPtr checkpoint_end_lsn);

#endif							/* PGUPGRADE_HANDOFF_RECEIPT_H */
