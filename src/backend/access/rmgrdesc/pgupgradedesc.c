/*-------------------------------------------------------------------------
 *
 * pgupgradedesc.c
 *	  rmgr descriptor routines for RM_PG_UPGRADE_ID WAL records.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
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

void
pg_upgrade_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_UPGRADE_START || info == XLOG_UPGRADE_COMPLETE)
	{
		xl_pg_upgrade xlrec;

		memcpy(&xlrec, rec, SizeOfXLPgUpgrade);
		appendStringInfo(buf, "old_major_version %u; new_major_version %u; time %lld",
						 xlrec.old_major_version,
						 xlrec.new_major_version,
						 (long long) xlrec.upgrade_time);
	}
	else if (info == XLOG_UPGRADE_RELINK)
	{
		/* Manifest of user relations to link on the standby: no file data. */
		char	   *ptr = rec;
		char	   *end = rec + XLogRecGetDataLen(record);
		int			nentries = 0;
		xl_upgrade_relink_entry first;

		while (ptr + SizeOfXLUpgradeRelinkEntry <= end)
		{
			xl_upgrade_relink_entry ent;

			memcpy(&ent, ptr, SizeOfXLUpgradeRelinkEntry);
			if (nentries == 0)
				first = ent;
			nentries++;
			ptr += SizeOfXLUpgradeRelinkEntry;
		}
		appendStringInfo(buf, "%d files to relink; first rel %u/%u/%u fork %u seg %u",
						 nentries,
						 nentries ? first.tablespace_oid : 0,
						 nentries ? first.database_oid : 0,
						 nentries ? first.relfilenumber : 0,
						 nentries ? first.forknum : 0,
						 nentries ? first.segno : 0);
	}
	else if (info == XLOG_UPGRADE_RAWFILE)
	{
		xl_upgrade_rawfile xlrec;
		char	   *path = rec + SizeOfXLUpgradeRawfile;

		memcpy(&xlrec, rec, SizeOfXLUpgradeRawfile);
		appendStringInfo(buf, "rawfile \"%.*s\"; offset %llu; bytes %u",
						 (int) xlrec.path_len, path,
						 (unsigned long long) xlrec.offset, xlrec.data_len);
	}
	else if (info == XLOG_UPGRADE_DIRTREE)
	{
		xl_upgrade_dirtree xlrec;
		char	   *first = rec + SizeOfXLUpgradeDirtree;

		memcpy(&xlrec, rec, SizeOfXLUpgradeDirtree);
		appendStringInfo(buf, "dirs %u (%u bytes); symlinks %u (%u bytes); first \"%s\"",
						 xlrec.ndirs, xlrec.dir_bytes,
						 xlrec.nsymlinks, xlrec.sym_bytes,
						 xlrec.ndirs > 0 ? first : "");
	}
	else if (info == XLOG_UPGRADE_HANDOFF)
	{
		xl_pg_upgrade_handoff xlrec;

		memcpy(&xlrec, rec, SizeOfXLPgUpgradeHandoff);
		appendStringInfo(buf, "old_major_version %u; target_major_version %u; time %lld",
						 xlrec.old_major_version,
						 xlrec.target_major_version,
						 (long long) xlrec.handoff_time);
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
		case XLOG_UPGRADE_DIRTREE:
			return "UPGRADE_DIRTREE";
		case XLOG_UPGRADE_HANDOFF:
			return "PG_UPGRADE_HANDOFF";
	}
	return NULL;
}
