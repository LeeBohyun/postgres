/*-------------------------------------------------------------------------
 *
 * pgupgrade_emit.h
 *    Startup interface for upgrade WAL emission.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/include/access/pgupgrade_emit.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGUPGRADE_EMIT_H
#define PGUPGRADE_EMIT_H

#include "access/xlogdefs.h"
#include "common/pg_upgrade_records.h"

extern const xl_pg_upgrade_start *PgUpgradePrepareEmission(void);
extern XLogRecPtr PgUpgradeEmitWalFile(void);

#endif							/* PGUPGRADE_EMIT_H */
