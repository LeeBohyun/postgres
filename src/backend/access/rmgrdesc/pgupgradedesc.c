/*-------------------------------------------------------------------------
 *
 * pgupgradedesc.c
 *	  rmgr descriptor routines for RM_PG_UPGRADE_ID.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/pgupgradedesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/pgupgrade_wal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "lib/stringinfo.h"

static const char *
relink_operation_name(uint8 operation)
{
	switch (operation)
	{
		case UPGRADE_RELINK_INHERIT:
			return "INHERIT";
		case UPGRADE_RELINK_RECREATE:
			return "RECREATE";
		case UPGRADE_RELINK_CREATE:
			return "CREATE";
		case UPGRADE_RELINK_DELETE:
			return "DELETE";
	}
	return "UNKNOWN";
}

void
pg_upgrade_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_UPGRADE_START || info == XLOG_UPGRADE_COMPLETE)
	{
		xl_pg_upgrade_marker marker;
		const char *error = NULL;

		if (!PgUpgradeReadMarker(info, rec, XLogRecGetDataLen(record), &marker, &error))
			appendStringInfo(buf, "invalid upgrade marker: %s", error);
		else
		{
			appendStringInfo(buf, "old_major_version %u; new_major_version %u; time " INT64_FORMAT,
							 marker.old_major, marker.new_major, marker.transaction_time);
			if (info == XLOG_UPGRADE_START)
			{
				xl_pg_upgrade_start start;

				memcpy(&start, rec, SizeOfPgUpgradeStart);
				appendStringInfo(buf, "; transfer_mode %u", start.transfer_mode);
			}
		}
	}
	else if (info == XLOG_UPGRADE_RELINK)
	{
		Size		length = XLogRecGetDataLen(record);
		uint32		flags;

		if (length < SizeOfPgUpgradeRelink ||
			(length - SizeOfPgUpgradeRelink) % SizeOfPgUpgradeRelinkEntry != 0)
		{
			appendStringInfoString(buf, "invalid RELINK length");
			return;
		}
		memcpy(&flags, rec, sizeof(flags));
		appendStringInfo(buf, "scope%s%s; entries %zu",
						 flags & UPGRADE_RELINK_BEGIN ? " BEGIN" : "",
						 flags & UPGRADE_RELINK_END ? " END" : "",
						 (length - SizeOfPgUpgradeRelink) / SizeOfPgUpgradeRelinkEntry);
		for (Size offset = SizeOfPgUpgradeRelink; offset < length;
			 offset += SizeOfPgUpgradeRelinkEntry)
		{
			xl_pg_upgrade_relink_entry entry;

			memcpy(&entry, rec + offset, sizeof(entry));
			appendStringInfo(buf,
							 "; %s %s key %u/%u/%u flags %u",
							 PgUpgradeRelinkEntryIsDirectory(&entry) ?
							 "DIRECTORY" : "RELATION",
							 relink_operation_name(entry.operation),
							 entry.key.tablespace_oid,
							 entry.key.database_oid,
							 entry.key.filenumber,
							 entry.flags);
		}
	}
	else if (info == XLOG_UPGRADE_RAWFILE)
	{
		xl_pg_upgrade_rawfile xlrec;
		char	   *path = rec + SizeOfPgUpgradeRawFile;

		memcpy(&xlrec, rec, SizeOfPgUpgradeRawFile);
		appendStringInfo(buf, "rawfile \"%.*s\"; offset %llu; bytes %u",
						 (int) xlrec.path_len, path,
						 (unsigned long long) xlrec.offset, xlrec.data_len);
	}
}

const char *
pg_upgrade_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_UPGRADE_START:
			return "PG_UPGRADE_START";
		case XLOG_UPGRADE_COMPLETE:
			return "PG_UPGRADE_COMPLETE";
		case XLOG_UPGRADE_RELINK:
			return "UPGRADE_RELINK";
		case XLOG_UPGRADE_RAWFILE:
			return "UPGRADE_RAWFILE";
	}
	return NULL;
}
