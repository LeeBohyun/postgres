/*-------------------------------------------------------------------------
 *
 * startup.c
 *    Select and arm a WAL-logged pg_upgrade transaction.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/startup.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/pgupgrade_config.h"
#include "access/pgupgrade_control.h"
#include "access/pgupgrade_retention_slots.h"
#include "access/pgupgrade_wal.h"
#include "access/timeline.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include "common/string.h"
#include "miscadmin.h"
#include "nodes/miscnodes.h"
#include "replication/walreceiver.h"
#include "storage/fd.h"
#include "utils/elog.h"
#include "utils/pg_lsn.h"


typedef struct UpgradeWalReadPrivate
{
	char		dir[MAXPGPATH];
	TimeLineID	tli;
	XLogRecPtr	endptr;
} UpgradeWalReadPrivate;

typedef struct UpgradeWalSegment
{
	TimeLineID	tli;
	XLogSegNo	segno;
	uint64		sysid;
	int			segsize;
	uint32		size;
} UpgradeWalSegment;

typedef struct UpgradeWalTransaction
{
	TimeLineID	file_tli;
	CheckPoint	checkpoint;
	XLogRecPtr	start_lsn;
	uint64		sysid;
	int			segsize;
	xl_pg_upgrade_marker start;
	const char *error;
} UpgradeWalTransaction;

static void
UpgradeWalSegOpen(XLogReaderState *state, XLogSegNo nextSegNo,
				  TimeLineID *tli_p)
{
	UpgradeWalReadPrivate *priv = (UpgradeWalReadPrivate *) state->private_data;
	char		fname[MAXFNAMELEN];
	char		path[MAXPGPATH];

	XLogFileName(fname, priv->tli, nextSegNo, state->segcxt.ws_segsize);
	snprintf(path, sizeof(path), "%s/%s", priv->dir, fname);
	state->seg.ws_file = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (state->seg.ws_file < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open upgrade WAL segment \"%s\": %m", path)));
}

static void
UpgradeWalSegClose(XLogReaderState *state)
{
	if (state->seg.ws_file >= 0)
		close(state->seg.ws_file);
	state->seg.ws_file = -1;
}

static int
UpgradeWalPageRead(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				   XLogRecPtr targetRecPtr, char *readBuf)
{
	UpgradeWalReadPrivate *priv = (UpgradeWalReadPrivate *) state->private_data;
	int			count = XLOG_BLCKSZ;
	WALReadError errinfo;

	if (targetPagePtr + XLOG_BLCKSZ > priv->endptr)
	{
		if (targetPagePtr + reqLen > priv->endptr)
			return -1;
		count = (int) (priv->endptr - targetPagePtr);
	}

	if (!WALRead(state, readBuf, targetPagePtr, count, priv->tli, &errinfo))
		return -1;
	return count;
}

static XLogReaderState *
AllocateUpgradeWalReader(int wal_segment_size, UpgradeWalReadPrivate *priv,
						 uint64 system_identifier)
{
	XLogReaderState *reader;

	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = UpgradeWalPageRead,
										   .segment_open = UpgradeWalSegOpen,
										   .segment_close = UpgradeWalSegClose),
								priv);
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	reader->system_identifier = system_identifier;
	return reader;
}

static int
CompareUpgradeWalSegments(const void *a, const void *b)
{
	const UpgradeWalSegment *left = a;
	const UpgradeWalSegment *right = b;

	if (left->tli != right->tli)
		return left->tli < right->tli ? -1 : 1;
	if (left->segno != right->segno)
		return left->segno < right->segno ? -1 : 1;
	return 0;
}

/* Collect each START with its most recent shutdown checkpoint. */
static List *
ScanUpgradeWalRun(const char *waldir, const UpgradeWalSegment *first_segment,
				  const UpgradeWalSegment *last_segment, List *transactions)
{
	UpgradeWalReadPrivate priv;
	XLogReaderState *reader;
	XLogRecPtr	startptr;
	XLogRecPtr	first;
	CheckPoint	last_ckpt;
	char	   *errormsg = NULL;

	MemSet(&last_ckpt, 0, sizeof(CheckPoint));
	priv.tli = first_segment->tli;
	strlcpy(priv.dir, waldir, sizeof(priv.dir));
	XLogSegNoOffsetToRecPtr(first_segment->segno, 0,
							first_segment->segsize, startptr);
	XLogSegNoOffsetToRecPtr(last_segment->segno, last_segment->size,
							first_segment->segsize, priv.endptr);

	reader = AllocateUpgradeWalReader(first_segment->segsize, &priv,
									  first_segment->sysid);
	first = XLogFindNextRecord(reader, startptr, &errormsg);
	if (XLogRecPtrIsInvalid(first))
	{
		XLogReaderFree(reader);
		return transactions;
	}

	XLogBeginRead(reader, first);
	for (;;)
	{
		XLogRecord *record = XLogReadRecord(reader, &errormsg);
		uint8		rmid;
		uint8		info;

		if (record == NULL)
			break;

		rmid = XLogRecGetRmid(reader);
		info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		/*
		 * Track the most recent shutdown checkpoint as the upgrade replay
		 * start.
		 */
		if (rmid == RM_XLOG_ID && info == XLOG_CHECKPOINT_SHUTDOWN)
		{
			last_ckpt.redo = InvalidXLogRecPtr;
			if (XLogRecGetDataLen(reader) == sizeof(CheckPoint))
			{
				memcpy(&last_ckpt, XLogRecGetData(reader), sizeof(CheckPoint));
				if (last_ckpt.redo != reader->ReadRecPtr ||
					last_ckpt.ThisTimeLineID == 0)
					last_ckpt.redo = InvalidXLogRecPtr;
			}
		}
		else if (rmid == RM_PG_UPGRADE_ID && info == XLOG_UPGRADE_START)
		{
			UpgradeWalTransaction *transaction = palloc0_object(UpgradeWalTransaction);

			transaction->file_tli = first_segment->tli;
			transaction->checkpoint = last_ckpt;
			transaction->start_lsn = reader->ReadRecPtr;
			transaction->sysid = first_segment->sysid;
			transaction->segsize = first_segment->segsize;
			transactions = lappend(transactions, transaction);
			if (PgUpgradeReadMarker(info, XLogRecGetData(reader),
									XLogRecGetDataLen(reader), &transaction->start,
									&transaction->error) &&
				(transaction->start.new_major / 10000 != PG_VERSION_NUM / 10000 ||
				 strncmp(transaction->start.pg_version, PG_MAJORVERSION "\n",
						 sizeof(transaction->start.pg_version)) != 0))
				transaction->error = "START new major version does not match this server";
		}
	}

	XLogReaderFree(reader);
	return transactions;
}

static bool
ReadUpgradeWalSegment(const char *waldir, const char *filename,
					  UpgradeWalSegment *segment)
{
	char		path[MAXPGPATH];
	int			fd;
	struct stat st;
	XLogLongPageHeaderData header;
	XLogRecPtr	segment_start;

	if (!IsXLogFileName(filename))
		return false;
	snprintf(path, sizeof(path), "%s/%s", waldir, filename);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	if (fstat(fd, &st) != 0 ||
		pg_pread(fd, &header, sizeof(header), 0) != sizeof(header))
	{
		CloseTransientFile(fd);
		return false;
	}
	CloseTransientFile(fd);
	if (header.std.xlp_magic != XLOG_PAGE_MAGIC ||
		!(header.std.xlp_info & XLP_LONG_HEADER) ||
		(header.std.xlp_info & ~XLP_ALL_FLAGS) != 0 ||
		!IsValidWalSegSize(header.xlp_seg_size) ||
		header.xlp_xlog_blcksz != XLOG_BLCKSZ ||
		header.xlp_sysid == 0 || st.st_size > header.xlp_seg_size)
		return false;
	segment->segsize = header.xlp_seg_size;
	segment->size = st.st_size;
	segment->sysid = header.xlp_sysid;
	XLogFromFileName(filename, &segment->tli, &segment->segno,
					 segment->segsize);
	XLogSegNoOffsetToRecPtr(segment->segno, 0, segment->segsize, segment_start);
	if (segment->tli == 0 || header.std.xlp_pageaddr != segment_start)
		return false;
	return true;
}

int
GetUpgradeArchiveWalSegmentSize(void)
{
	char		waldir[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	int			segsize = 0;

	snprintf(waldir, sizeof(waldir), "%s/%s", DataDir, XLOGDIR);
	dir = AllocateDir(waldir);
	if (dir == NULL)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m", waldir)));
	while ((de = ReadDir(dir, waldir)) != NULL)
	{
		UpgradeWalSegment segment;

		if (!ReadUpgradeWalSegment(waldir, de->d_name, &segment))
			continue;
		if (segsize != 0 && segsize != segment.segsize)
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("staged pg_upgrade WAL has conflicting segment sizes"),
					 errdetail("Found both %d-byte and %d-byte WAL segments.",
							   segsize, segment.segsize)));
		segsize = segment.segsize;
	}
	FreeDir(dir);
	if (segsize == 0)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not determine the pg_upgrade WAL segment size"),
				 errhint("Stage the new-major shutdown checkpoint and upgrade transaction "
						 "before starting archive upgrade recovery.")));
	return segsize;
}

/* Scan contiguous new-major WAL by timeline, system identifier, and size. */
static List *
FindUpgradeWalTransactions(const char *waldir)
{
	DIR		   *dir;
	struct dirent *de;
	UpgradeWalSegment *segments;
	int			nsegments = 0;
	int			capacity = 16;
	List	   *transactions = NIL;

	segments = palloc_array(UpgradeWalSegment, capacity);
	dir = AllocateDir(waldir);
	if (dir == NULL)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m", waldir)));
	while ((de = ReadDir(dir, waldir)) != NULL)
	{
		UpgradeWalSegment segment;

		if (!ReadUpgradeWalSegment(waldir, de->d_name, &segment))
			continue;
		if (nsegments == capacity)
		{
			capacity *= 2;
			segments = repalloc_array(segments, UpgradeWalSegment, capacity);
		}
		segments[nsegments++] = segment;
	}
	FreeDir(dir);
	qsort(segments, nsegments, sizeof(UpgradeWalSegment), CompareUpgradeWalSegments);
	for (int begin = 0; begin < nsegments;)
	{
		int			end = begin;

		while (end + 1 < nsegments &&
			   segments[end].size == segments[end].segsize &&
			   segments[end + 1].tli == segments[begin].tli &&
			   segments[end + 1].segno == segments[end].segno + 1 &&
			   segments[end + 1].sysid == segments[begin].sysid &&
			   segments[end + 1].segsize == segments[begin].segsize)
			end++;
		transactions = ScanUpgradeWalRun(waldir, &segments[begin],
									 &segments[end], transactions);
		begin = end + 1;
	}
	pfree(segments);
	return transactions;
}

/*
 * Select the latest upgrade transaction in the requested timeline history.
 * Copies of a transaction in promoted segments must agree on its boundaries.
 */
static UpgradeWalTransaction *
SelectUpgradeWalTransaction(List *transactions, List *history)
{
	UpgradeWalTransaction *selected = NULL;
	ListCell   *lc;

	foreach(lc, transactions)
	{
		UpgradeWalTransaction *transaction = lfirst(lc);
		TimeLineID	start_tli;

		if (!tliInHistory(transaction->file_tli, history))
			continue;
		/* A promoted segment can include START from an earlier timeline. */
		start_tli = tliOfPointInHistory(transaction->start_lsn, history);
		if (start_tli > transaction->file_tli)
			continue;

		if (!XLogRecPtrIsInvalid(transaction->checkpoint.redo) &&
			(transaction->checkpoint.ThisTimeLineID != start_tli ||
			 tliOfPointInHistory(transaction->checkpoint.redo, history) != start_tli))
			transaction->error = "the checkpoint and START are on different "
				"recovery timelines";
		if (selected == NULL || transaction->start_lsn > selected->start_lsn)
			selected = transaction;
		else if (transaction->start_lsn == selected->start_lsn)
		{
			if (transaction->sysid != selected->sysid ||
				(!XLogRecPtrIsInvalid(transaction->checkpoint.redo) &&
				 !XLogRecPtrIsInvalid(selected->checkpoint.redo) &&
				 transaction->checkpoint.redo != selected->checkpoint.redo))
				ereport(FATAL,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("staged pg_upgrade WAL contains conflicting upgrade transactions")));
			if (!XLogRecPtrIsInvalid(transaction->checkpoint.redo) &&
				(XLogRecPtrIsInvalid(selected->checkpoint.redo) ||
				 (selected->error != NULL && transaction->error == NULL)))
				selected = transaction;
		}
	}

	if (selected == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not find a staged pg_upgrade transaction in the "
						"requested recovery history"),
				 errhint("Stage the upgrade checkpoint and "
						 "START-through-COMPLETE WAL for the requested "
						 "recovery timeline.")));
	if (XLogRecPtrIsInvalid(selected->checkpoint.redo))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_upgrade WAL is missing the shutdown checkpoint "
						"before START")));
	if (selected->error != NULL)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid pg_upgrade transaction"),
				 errdetail_internal("%s", selected->error)));
	if (selected->segsize != wal_segment_size)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_upgrade WAL segment size does not match "
						"pg_control"),
				 errdetail("The staged WAL uses %d bytes, but pg_control "
						   "specifies %d bytes.",
						   selected->segsize, wal_segment_size)));
	return selected;
}


static void
AppendShellArg(StringInfo command, const char *arg)
{
#ifdef WIN32
	appendStringInfoChar(command, '"');
	for (; *arg; arg++)
	{
		if (*arg == '"')
			appendStringInfoChar(command, '\\');
		appendStringInfoChar(command, *arg);
	}
	appendStringInfoChar(command, '"');
#else
	appendStringInfoChar(command, '\'');
	for (; *arg; arg++)
	{
		if (*arg == '\'')
			appendStringInfoString(command, "'\\''");
		else
			appendStringInfoChar(command, *arg);
	}
	appendStringInfoChar(command, '\'');
#endif
}

static XLogRecPtr
ParseControlLSN(const char *value, const char *label)
{
	ErrorSaveContext escontext = {T_ErrorSaveContext};
	XLogRecPtr	lsn = pg_lsn_in_safe(value, (Node *) &escontext);

	if (escontext.error_occurred)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid %s in old pg_controldata output", label)));
	return lsn;
}

static uint64
ParseOldControlNumber(const char *value, const char *label, uint64 maximum)
{
	uint64		parsed = 0;

	if (*value == '\0')
		elog(FATAL, "empty %s in old pg_controldata output", label);
	for (; *value != '\0'; value++)
	{
		unsigned	digit = (unsigned char) *value - '0';

		if (digit > 9 || parsed > (maximum - digit) / 10)
			elog(FATAL, "invalid %s in old pg_controldata output", label);
		parsed = parsed * 10 + digit;
	}
	if (parsed == 0)
		elog(FATAL, "zero %s in old pg_controldata output", label);
	return parsed;
}

static bool
OldControlLineHasWarning(const char *line)
{
	for (; *line != '\0'; line++)
		if (pg_strncasecmp(line, "warning:", 8) == 0)
			return true;
	return false;
}

static FILE *
OpenOldControlPipe(const char *utility, const char *old_datadir)
{
	StringInfoData command;
	char	   *saved_lc_all = NULL;
	FILE	   *output;
	int			save_errno;

	initStringInfo(&command);
	AppendShellArg(&command, utility);
	appendStringInfoString(&command, " -D ");
	AppendShellArg(&command, old_datadir);
	appendStringInfoString(&command, " 2>&1");
	if (getenv("LC_ALL") != NULL)
		saved_lc_all = pstrdup(getenv("LC_ALL"));
	if (setenv("LC_ALL", "C", 1) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not set locale for old pg_controldata: %m")));
	output = OpenPipeStream(command.data, "r");
	save_errno = errno;
	if (saved_lc_all != NULL)
	{
		if (setenv("LC_ALL", saved_lc_all, 1) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not restore locale after old pg_controldata: %m")));
		pfree(saved_lc_all);
	}
	else if (unsetenv("LC_ALL") != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not restore locale after old pg_controldata: %m")));
	pfree(command.data);
	if (output == NULL)
	{
		errno = save_errno;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not run matching old pg_controldata \"%s\": %m", utility)));
	}
	return output;
}

static uint32
ReadOldMajorVersion(const char *old_datadir)
{
	char		path[MAXPGPATH];
	char		version[64];
	char		extra;
	char	   *end;
	FILE	   *file;
	int			major;
	int			minor = 0;
	bool		valid;

	snprintf(path, sizeof(path), "%s/PG_VERSION", old_datadir);
	file = AllocateFile(path, "r");
	if (file == NULL)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read old cluster version file \"%s\": %m",
						path)));
	if (fscanf(file, "%63s", version) != 1 ||
		fscanf(file, " %c", &extra) == 1 || ferror(file))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not parse old cluster version file \"%s\"",
						path)));
	FreeFile(file);

	errno = 0;
	major = strtoint(version, &end, 10);
	valid = errno != ERANGE && major > 0 &&
		(uint32) major <= PG_UINT32_MAX / 10000;
	if (valid && major < 10)
	{
		valid = *end == '.';
		if (valid)
		{
			errno = 0;
			minor = strtoint(end + 1, &end, 10);
			valid = errno != ERANGE && minor >= 0 && minor <= 99;
		}
	}
	if (!valid || *end != '\0')
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid old cluster version in file \"%s\"", path)));

	return (uint32) major * 10000 + minor * 100;
}

typedef enum OldControlFieldType
{
	OLD_CONTROL_UINT32,
	OLD_CONTROL_UINT64,
	OLD_CONTROL_LSN,
	OLD_CONTROL_ZEROABLE_TLI,
	OLD_CONTROL_SEGMENT_SIZE
} OldControlFieldType;

typedef struct OldControlField
{
	const char *name;
	const char *label;
	size_t		offset;
	OldControlFieldType type;
} OldControlField;

#define OLD_CONTROL_FIELD(member, name, label, type) \
	{name, label, offsetof(OldUpgradeControlData, member), type}

static const OldControlField old_control_fields[] = {
	OLD_CONTROL_FIELD(system_identifier, "Database system identifier",
					  "system identifier", OLD_CONTROL_UINT64),
	OLD_CONTROL_FIELD(control_version, "pg_control version number",
					  "control version", OLD_CONTROL_UINT32),
	OLD_CONTROL_FIELD(catalog_version, "Catalog version number",
					  "catalog version", OLD_CONTROL_UINT32),
	OLD_CONTROL_FIELD(block_size, "Database block size",
					  "database block size", OLD_CONTROL_UINT32),
	OLD_CONTROL_FIELD(blocks_per_segment, "Blocks per segment of large relation",
					  "relation segment size", OLD_CONTROL_UINT32),
	OLD_CONTROL_FIELD(wal_block_size, "WAL block size",
					  "WAL block size", OLD_CONTROL_UINT32),
	OLD_CONTROL_FIELD(checkpoint_lsn, "Latest checkpoint location",
					  "checkpoint location", OLD_CONTROL_LSN),
	OLD_CONTROL_FIELD(checkpoint_redo, "Latest checkpoint's REDO location",
					  "checkpoint REDO", OLD_CONTROL_LSN),
	OLD_CONTROL_FIELD(checkpoint_tli, "Latest checkpoint's TimeLineID",
					  "checkpoint timeline", OLD_CONTROL_UINT32),
	OLD_CONTROL_FIELD(checkpoint_end_lsn, "Minimum recovery ending location",
					  "minimum recovery location", OLD_CONTROL_LSN),
	OLD_CONTROL_FIELD(checkpoint_end_tli, "Min recovery ending loc's timeline",
					  "minimum recovery timeline", OLD_CONTROL_ZEROABLE_TLI),
	OLD_CONTROL_FIELD(wal_segment_size, "Bytes per WAL segment",
					  "WAL segment size", OLD_CONTROL_SEGMENT_SIZE)
};

static void
ParseOldControlField(OldUpgradeControlData *result, const char *name,
					 const char *value, uint32 *seen)
{
	for (int i = 0; i < lengthof(old_control_fields); i++)
	{
		const OldControlField *field = &old_control_fields[i];
		uint32		bit = ((uint32) 1) << i;
		char	   *destination;
		uint64		parsed;

		if (strcmp(name, field->name) != 0)
			continue;
		if (*seen & bit)
			elog(FATAL, "duplicate %s in old pg_controldata output",
				 field->label);

		destination = (char *) result + field->offset;
		switch (field->type)
		{
			case OLD_CONTROL_UINT32:
				*(uint32 *) destination = (uint32) ParseOldControlNumber(value,
																		 field->label,
																		 PG_UINT32_MAX);
				break;
			case OLD_CONTROL_UINT64:
				*(uint64 *) destination = ParseOldControlNumber(value,
																field->label,
																PG_UINT64_MAX);
				break;
			case OLD_CONTROL_LSN:
				*(XLogRecPtr *) destination = ParseControlLSN(value, field->label);
				break;
			case OLD_CONTROL_ZEROABLE_TLI:
				*(TimeLineID *) destination = strcmp(value, "0") == 0 ? 0 :
					(TimeLineID) ParseOldControlNumber(value, field->label,
													   PG_UINT32_MAX);
				break;
			case OLD_CONTROL_SEGMENT_SIZE:
				parsed = ParseOldControlNumber(value, field->label,
											   INT_MAX);
				if (!IsValidWalSegSize((int) parsed))
					elog(FATAL, "invalid WAL segment size in old pg_controldata output");
				*(int *) destination = (int) parsed;
				break;
		}
		*seen |= bit;
		return;
	}
}

#undef OLD_CONTROL_FIELD

/*
 * Run the old installation's pg_controldata to read identity, layout, and
 * checkpoint fields.
 */
static void
ReadUpgradeControlData(const char *old_datadir,
					   OldUpgradeControlData *result, bool require_handoff)
{
	char		opts_path[MAXPGPATH];
	char		line[MAXPGPATH * 2];
	char		old_bindir[MAXPGPATH];
	char		utility[MAXPGPATH];
	char	   *first_arg;
	FILE	   *opts;
	FILE	   *output;
	char	   *control_warning = NULL;
	bool		got_state = false;
	uint32		seen = 0;
	int			status;

	MemSet(result, 0, sizeof(*result));
	snprintf(opts_path, sizeof(opts_path), "%s/postmaster.opts", old_datadir);
	opts = AllocateFile(opts_path, "r");
	if (opts == NULL || fgets(line, sizeof(line), opts) == NULL)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read old standby options file \"%s\"", opts_path),
				 errhint("The retained standby must have been started with its "
						 "old-version installation.")));
	if (fgets(old_bindir, sizeof(old_bindir), opts) != NULL)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("old standby options file \"%s\" has more than one "
						"line", opts_path)));
	FreeFile(opts);

	pg_strip_crlf(line);
	first_arg = strstr(line, " \"");
	if (first_arg != NULL)
		*first_arg = '\0';
	if (line[0] == '\0' || strlen(line) >= sizeof(old_bindir))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("old standby options file \"%s\" has no executable "
						"path", opts_path)));
	strlcpy(old_bindir, line, sizeof(old_bindir));
	get_parent_directory(old_bindir);
	snprintf(utility, sizeof(utility), "%s/pg_controldata%s", old_bindir, EXE);
	if (access(utility, X_OK) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not execute matching old pg_controldata \"%s\": %m",
						utility)));

	result->major_version = ReadOldMajorVersion(old_datadir);
	output = OpenOldControlPipe(utility, old_datadir);

	while (fgets(line, sizeof(line), output) != NULL)
	{
		char	   *value = strchr(line, ':');

		if (strlen(line) == sizeof(line) - 1 && line[sizeof(line) - 2] != '\n')
			elog(FATAL, "overlong line in old pg_controldata output");
		pg_strip_crlf(line);
		/* Reject control-file warnings even when pg_controldata exits zero. */
		if (control_warning == NULL && OldControlLineHasWarning(line))
			control_warning = pstrdup(line);
		if (control_warning != NULL)
			continue;
		if (value == NULL)
			continue;
		*value++ = '\0';
		while (*value == ' ' || *value == '\t')
			value++;

		if (strcmp(line, "Database cluster state") == 0)
		{
			if (got_state)
				elog(FATAL, "duplicate database state in old pg_controldata "
					 "output");
			if (strcmp(value, "shut down in recovery") != 0 &&
				(require_handoff || strcmp(value, "shut down") != 0))
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("retained old standby is not shut down in "
								"recovery"),
						 errdetail("The retained data directory is in state "
								   "\"%s\".",
								   value),
						 errhint("Stop the old standby before using its data "
								 "directory for pg_upgrade RELINK.")));
			got_state = true;
		}
		else
			ParseOldControlField(result, line, value, &seen);
	}

	if (ferror(output))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read matching old pg_controldata output: %m")));
	status = ClosePipeStream(output);
	if (status != 0)
		ereport(FATAL,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("matching old pg_controldata failed with status %d", status)));
	if (control_warning != NULL)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("matching old pg_controldata reported untrustworthy "
						"control data"),
				 errdetail_internal("%s", control_warning)));

	if (!got_state || seen != (((uint32) 1 << lengthof(old_control_fields)) - 1))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("matching old pg_controldata output is missing "
						"required upgrade fields")));
	if (require_handoff && result->checkpoint_lsn != result->checkpoint_redo)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("old standby's final checkpoint location does not "
						"match its REDO location")));
	if (require_handoff &&
		(result->checkpoint_end_lsn <= result->checkpoint_lsn ||
		 result->checkpoint_end_tli != result->checkpoint_tli))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("old standby has an invalid final checkpoint end")));
	if (result->checkpoint_lsn == InvalidXLogRecPtr ||
		result->checkpoint_redo == InvalidXLogRecPtr ||
		result->checkpoint_redo > result->checkpoint_lsn ||
		(result->checkpoint_end_lsn != InvalidXLogRecPtr &&
		 result->checkpoint_end_tli == 0))
		elog(FATAL, "old cluster has invalid checkpoint identity or recovery "
			 "positions");
}

void
ReadOldUpgradeControlData(const char *old_datadir, OldUpgradeControlData *result)
{
	ReadUpgradeControlData(old_datadir, result, true);
}

/* Read old-major control data without requiring HANDOFF state. */
void
ReadArchiveUpgradeControlData(const char *old_datadir, OldUpgradeControlData *result)
{
	ReadUpgradeControlData(old_datadir, result, false);
}

/* With pg_upgrade.signal present, standby.signal takes precedence. */
UpgradeRecoveryMode
GetUpgradeRecoveryMode(void)
{
	char		path[MAXPGPATH];
	struct stat st;

	snprintf(path, sizeof(path), "%s/%s", DataDir, PG_UPGRADE_SIGNAL_FILE);
	if (stat(path, &st) != 0)
		return UPGRADE_RECOVERY_NONE;

	snprintf(path, sizeof(path), "%s/%s", DataDir, STANDBY_SIGNAL_FILE);
	if (stat(path, &st) == 0)
		return UPGRADE_RECOVERY_STANDBY;
	if (errno != ENOENT)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", path)));

	snprintf(path, sizeof(path), "%s/%s", DataDir, RECOVERY_SIGNAL_FILE);
	if (stat(path, &st) == 0)
		return UPGRADE_RECOVERY_ARCHIVE;
	if (errno != ENOENT)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", path)));

	return UPGRADE_RECOVERY_NONE;
}

/*
 * Derive the upgrade replay start LSN from the first segment after the
 * retained shutdown checkpoint. Read the incoming system identifier and a
 * nonzero timeline from the primary. Write a synthetic replay-start checkpoint
 * on the retained checkpoint timeline to pg_control.
 */
static void
ArmFromLocalStandby(void)
{
	WalReceiverConn *conn;
	char	   *err = NULL;
	TimeLineID	primary_tli = 0;
	char	   *sysid_str;
	uint64		sysid = 0;
	const char *old_datadir;
	OldUpgradeControlData old_control;
	XLogSegNo	replay_start_segno;
	CheckPoint	replay_start_checkpoint;

	if (PrimaryConnInfo == NULL || PrimaryConnInfo[0] == '\0')
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("streaming pg_upgrade skeleton requires "
						"\"primary_conninfo\"")));
	if ((PrimarySlotName != NULL && PrimarySlotName[0] != '\0') ||
		wal_receiver_create_temp_slot)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("streaming pg_upgrade replay must start without a "
						"replication slot"),
				 errhint("Set \"primary_slot_name\" to an empty string and "
						 "\"wal_receiver_create_temp_slot\" to off.")));

	old_datadir = pg_upgrade_standby_old_datadir;
	if (old_datadir == NULL || old_datadir[0] == '\0')
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("streaming --wal-upgrade skeleton requires "
						"\"pg_upgrade_standby_old_datadir\" to derive the "
						"upgrade replay start LSN"),
				 errhint("Set \"pg_upgrade_standby_old_datadir\" in "
						 "postgresql.conf to this standby's retained "
						 "pre-upgrade data directory.")));

	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		elog(FATAL, "libpqwalreceiver didn't initialize correctly");

	conn = walrcv_connect(PrimaryConnInfo, true, false, false,
						  "pg_upgrade_identify_system", &err);
	if (conn == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the primary to identify the "
						"pg_upgrade system: %s",
						err ? err : "unknown error"),
				 errhint("Set primary_conninfo to a live --wal-upgrade "
						 "primary.")));

	sysid_str = walrcv_identify_system(conn, &primary_tli, NULL);
	walrcv_disconnect(conn);
	if (sysid_str == NULL ||
		sscanf(sysid_str, "%" SCNu64, &sysid) != 1 ||
		sysid == 0)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("malformed system identifier from primary: \"%s\"",
						sysid_str ? sysid_str : "(null)")));

	if (primary_tli == 0)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("primary reported an invalid timeline for pg_upgrade "
						"recovery")));

	ReadOldUpgradeControlData(old_datadir, &old_control);
	PgUpgradePreparePhysicalSlotMigration(old_datadir,
										  old_control.major_version,
										  old_control.checkpoint_end_lsn);

	if (old_control.wal_segment_size != wal_segment_size)
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_upgrade streaming standby requires matching WAL "
						"segment sizes"),
				 errdetail("The retained old data directory uses %d-byte WAL "
						   "segments but this cluster uses %d-byte segments.",
						   old_control.wal_segment_size, wal_segment_size),
				 errhint("Re-initialize the new cluster with the same "
						 "--wal-segsize as the old cluster before streaming "
						 "the upgrade transaction.")));

	/* Start after the last segment occupied by the old shutdown checkpoint. */
	XLByteToPrevSeg(old_control.checkpoint_end_lsn, replay_start_segno,
					old_control.wal_segment_size);
	replay_start_segno++;

	/* pg_resetwal places the new checkpoint after the long page header. */
	MemSet(&replay_start_checkpoint, 0, sizeof(replay_start_checkpoint));
	XLogSegNoOffsetToRecPtr(replay_start_segno, SizeOfXLogLongPHD,
							old_control.wal_segment_size,
							replay_start_checkpoint.redo);
	/* Use the upgrade timeline even if the primary has since been promoted. */
	replay_start_checkpoint.ThisTimeLineID = old_control.checkpoint_tli;
	replay_start_checkpoint.PrevTimeLineID = old_control.checkpoint_tli;

	ereport(LOG,
			(errmsg("auto-armed streaming standby from the retained final "
					"checkpoint (sysid " UINT64_FORMAT ", upgrade replay "
					"start LSN %X/%08X, redo %X/%08X, TLI %u, upgrade "
					"replay start segment %llu)",
					sysid, LSN_FORMAT_ARGS(replay_start_checkpoint.redo),
					LSN_FORMAT_ARGS(replay_start_checkpoint.redo),
					replay_start_checkpoint.ThisTimeLineID,
					(unsigned long long) replay_start_segno)));

	ArmControlFileForUpgradeRecovery(&replay_start_checkpoint,
									 sysid, true);
}

/*
 * Configure upgrade recovery before StartupXLOG reads WAL. Streaming recovery
 * derives its replay start LSN from the retained old standby. Archive recovery
 * scans staged WAL for the shutdown checkpoint before START. A finalized
 * streaming standby keeps its saved restartpoint.
 */
void
PerformWalUpgradeIfNeeded(void)
{
	UpgradeRecoveryMode mode;
	bool		started;
	bool		finalized;
	bool		crc_ok;
	ControlFileData *control;
	TimeLineID	base_tli;
	List	   *transactions;
	List	   *history;
	ListCell   *lc;
	UpgradeWalTransaction *transaction;

	GetControlFileUpgradeState(&started, &finalized);

	if (IsBinaryUpgrade)
		return;

	if (finalized && !started)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_control has an invalid pg_upgrade state"),
				 errdetail("The upgrade is finalized but was never started.")));

	if (started && !finalized)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("the pg_upgrade transaction was only partially applied"),
				 errhint("Discard this new-version skeleton and retry the "
						 "upgrade with a fresh skeleton.")));

	mode = GetUpgradeRecoveryMode();
	if (mode == UPGRADE_RECOVERY_NONE)
		return;

	/* Restart a finalized standby without selecting its upgrade transaction again. */
	if (finalized && mode == UPGRADE_RECOVERY_STANDBY)
		return;

	if (mode == UPGRADE_RECOVERY_STANDBY)
	{
		ArmFromLocalStandby();
		PgUpgradeSelectReplay();
		return;
	}

	Assert(mode == UPGRADE_RECOVERY_ARCHIVE);

	control = get_controlfile(DataDir, &crc_ok);
	if (!crc_ok)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("incorrect checksum in control file")));
	base_tli = Max(control->checkPointCopy.ThisTimeLineID,
				   control->minRecoveryPointTLI);
	transactions = FindUpgradeWalTransactions(XLOGDIR);

	/* For synthesized pg_control, use the requested or staged checkpoint TLI. */
	if (base_tli == 0)
	{
		if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_NUMERIC)
			base_tli = recoveryTargetTLIRequested;
		else
		{
			foreach(lc, transactions)
			{
				UpgradeWalTransaction *candidate = lfirst(lc);

				if (XLogRecPtrIsInvalid(candidate->checkpoint.redo) ||
					candidate->start.new_major / 10000 != PG_VERSION_NUM / 10000)
					continue;
				if (base_tli != 0 && base_tli != candidate->checkpoint.ThisTimeLineID)
					ereport(FATAL,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("staged pg_upgrade WAL has ambiguous recovery "
									"timelines"),
							 errhint("Set recovery_target_timeline to the required "
									 "numeric timeline.")));
				base_tli = candidate->checkpoint.ThisTimeLineID;
			}
		}
		if (base_tli == 0)
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("could not find a staged pg_upgrade replay start "
							"checkpoint"),
					 errhint("Stage the new-major shutdown checkpoint and "
							 "START-through-COMPLETE WAL before starting "
							 "archive upgrade recovery.")));
	}
	InitWalRecoverySettings(base_tli);
	if (recoveryTargetTLI != 1 && !existsTimeLineHistory(recoveryTargetTLI))
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("history file for recovery target timeline %u is "
						"missing", recoveryTargetTLI)));
	history = readTimeLineHistory(recoveryTargetTLI);
	if (recoveryTargetTLI != 1 && list_length(history) == 1)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("history file for recovery target timeline %u is "
						"empty", recoveryTargetTLI)));
	transaction = SelectUpgradeWalTransaction(transactions, history);

	/* The retained recovery state must belong to the selected history. */
	if (!XLogRecPtrIsInvalid(control->checkPoint) &&
		tliOfPointInHistory(control->checkPoint, history) !=
		control->checkPointCopy.ThisTimeLineID)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("the retained checkpoint is not in the requested "
						"upgrade recovery history")));
	if (!XLogRecPtrIsInvalid(control->minRecoveryPoint) &&
		tliOfPointInHistory(control->minRecoveryPoint - 1, history) !=
		control->minRecoveryPointTLI)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("the retained minimum recovery point is not in the "
						"requested upgrade recovery history")));

	if (finalized && control->system_identifier == transaction->sysid &&
		control->checkPointCopy.redo > transaction->start_lsn)
	{
		pfree(control);
		list_free_deep(history);
		list_free_deep(transactions);
		return;
	}
	if (control->checkPoint >= transaction->checkpoint.redo ||
		control->minRecoveryPoint > transaction->checkpoint.redo)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("the retained recovery state does not precede the "
						"selected pg_upgrade transaction"),
				 errhint("Restore the old cluster to its pre-upgrade boundary "
						 "before retrying upgrade recovery.")));

	PgUpgradeSelectReplay();

	ereport(LOG,
			(errmsg("starting pg_upgrade archive recovery from checkpoint "
					"%X/%08X on timeline %u",
					LSN_FORMAT_ARGS(transaction->checkpoint.redo),
					transaction->checkpoint.ThisTimeLineID)));
	ArmControlFileForUpgradeRecovery(&transaction->checkpoint,
									 transaction->sysid, false);
	pfree(control);
	list_free_deep(history);
	list_free_deep(transactions);
}
