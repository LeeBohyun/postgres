/*-------------------------------------------------------------------------
 *
 * redo.c
 *    Recovery entry point for WAL-logged pg_upgrade.
 *
 * Upgrade WAL replay is not implemented yet. It is needed to upgrade a
 * physical standby.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/redo.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/pgupgrade_wal.h"

void
pg_upgrade_redo(XLogReaderState *record)
{
	elog(PANIC, "pg_upgrade WAL replay is not supported");
}
