/*-------------------------------------------------------------------------
 *
 * pgupgrade_emit.h
 *    Backend interface for upgrade WAL emission.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/include/access/pgupgrade_emit.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGUPGRADE_EMIT_H
#define PGUPGRADE_EMIT_H

/*
 * Validate the relink input and emit the upgrade WAL records, ending with
 * COMPLETE.
 */
extern void PgUpgradeEmitWalFile(void);

#endif							/* PGUPGRADE_EMIT_H */
