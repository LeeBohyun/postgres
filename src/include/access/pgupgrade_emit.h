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
 * Accept START, RELINK scope batches, and COMPLETE in one writable top-level
 * binary-upgrade transaction. The last batch for each scope captures its
 * declared files. COMPLETE captures PG_VERSION and the SLRUs after every scope
 * has completed. COMMIT enables the completion checkpoint.
 */
extern void PgUpgradeEmitWal(uint8 opcode,
							 const uint8 *data, size_t length);
extern void PgUpgradeEmitWalFile(void);

#endif							/* PGUPGRADE_EMIT_H */
