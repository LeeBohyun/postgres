/*-------------------------------------------------------------------------
 *
 * redo.c
 *    Recovery entry point for WAL-logged pg_upgrade.
 *
 * Replay support is added separately. Reject these records explicitly until
 * a resource-manager callback can apply them.
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
