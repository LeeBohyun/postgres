#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/pgupgrade_wal.h"
#include "access/xact.h"
#include "access/timeline.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/relpath.h"
#include "common/string.h"
#include "miscadmin.h"
#include "nodes/miscnodes.h"
#include "port/pg_crc32c.h"
#include "postmaster/bgwriter.h"
#include "replication/slot.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "storage/fd.h"
#include "storage/reinit.h"
#include "storage/copydir.h"
#include "storage/md.h"
#include "storage/procsignal.h"
#include "replication/walreceiver.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"


typedef struct UpgradeWalReadPrivate
{
	char		dir[MAXPGPATH];
	TimeLineID	tli;
	XLogRecPtr	endptr;
	List	   *history;
} UpgradeWalReadPrivate;

typedef struct UpgradeWalSegment
{
	TimeLineID	tli;
	XLogSegNo	segno;
	uint64		sysid;
	int			segsize;
	uint32		size;
}			UpgradeWalSegment;

typedef struct UpgradeWalWindow
{
	TimeLineID	file_tli;
	CheckPoint	checkpoint;
	XLogRecPtr	replay_start_lsn;
	XLogRecPtr	start_lsn;
	XLogRecPtr	complete_end_lsn;
	XLogRecPtr	complete_record_end;
	TransactionId emission_xid;
	uint64		sysid;
	int			segsize;
	xl_pg_upgrade_marker start;
	const char *error;
}			UpgradeWalWindow;

static void PgUpgradeReplayComplete(XLogReaderState *record);

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
	if (priv->history != NIL &&
		((XLogPageHeader) readBuf)->xlp_tli !=
		tliOfPointInHistory(targetPagePtr, priv->history))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_upgrade WAL page is not in the requested recovery history"),
				 errdetail("The page at %X/%08X has timeline %u.",
						   LSN_FORMAT_ARGS(targetPagePtr),
						   ((XLogPageHeader) readBuf)->xlp_tli)));

	return count;
}

static int
CompareUpgradeWalSegments(const void *a, const void *b)
{
	const		UpgradeWalSegment *left = a;
	const		UpgradeWalSegment *right = b;

	if (left->tli != right->tli)
		return left->tli < right->tli ? -1 : 1;
	if (left->segno != right->segno)
		return left->segno < right->segno ? -1 : 1;
	return 0;
}

/*
 * Collect each START with its most recent shutdown checkpoint and any
 * matching COMPLETE and transaction COMMIT.
 */
static List *
ScanUpgradeWalRun(const char *waldir, const UpgradeWalSegment * first_segment,
				  const UpgradeWalSegment * last_segment, List *windows)
{
	UpgradeWalReadPrivate priv;
	XLogReaderState *reader;
	XLogRecPtr	startptr;
	XLogRecPtr	first;
	CheckPoint	last_ckpt;
	XLogRecPtr	last_ckpt_lsn = InvalidXLogRecPtr;
	UpgradeWalWindow *window = NULL;
	char	   *errormsg = NULL;

	MemSet(&last_ckpt, 0, sizeof(CheckPoint));
	priv.tli = first_segment->tli;
	priv.history = NIL;
	strlcpy(priv.dir, waldir, sizeof(priv.dir));
	XLogSegNoOffsetToRecPtr(first_segment->segno, 0,
							first_segment->segsize, startptr);
	XLogSegNoOffsetToRecPtr(last_segment->segno, last_segment->size,
							first_segment->segsize, priv.endptr);

	reader = XLogReaderAllocate(first_segment->segsize, NULL,
								XL_ROUTINE(.page_read = UpgradeWalPageRead,
										   .segment_open = UpgradeWalSegOpen,
										   .segment_close = UpgradeWalSegClose),
								&priv);
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	reader->system_identifier = first_segment->sysid;
	first = XLogFindNextRecord(reader, startptr, &errormsg);
	if (XLogRecPtrIsInvalid(first))
	{
		XLogReaderFree(reader);
		return windows;
	}

	XLogBeginRead(reader, first);
	for (;;)
	{
		XLogRecord *record = XLogReadRecord(reader, &errormsg);
		uint8		rmid;
		uint8		info;

		if (record == NULL)
		{
			if (window != NULL && window->error == NULL && errormsg != NULL)
				window->error = pstrdup(errormsg);
			break;
		}

		rmid = XLogRecGetRmid(reader);
		info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		/*
		 * Track the most recent shutdown checkpoint as the upgrade replay
		 * start.
		 */
		if (rmid == RM_XLOG_ID && info == XLOG_CHECKPOINT_SHUTDOWN)
		{
			last_ckpt_lsn = InvalidXLogRecPtr;
			if (XLogRecGetDataLen(reader) == sizeof(CheckPoint))
			{
				memcpy(&last_ckpt, XLogRecGetData(reader), sizeof(CheckPoint));
				if (last_ckpt.redo == reader->ReadRecPtr &&
					last_ckpt.ThisTimeLineID != 0)
				{
					last_ckpt_lsn = reader->ReadRecPtr;
				}
			}
		}
		else if (rmid == RM_PG_UPGRADE_ID)
		{
			if (info == XLOG_UPGRADE_START)
			{
				window = palloc0_object(UpgradeWalWindow);
				window->file_tli = first_segment->tli;
				window->checkpoint = last_ckpt;
				window->replay_start_lsn = last_ckpt_lsn;
				window->start_lsn = reader->ReadRecPtr;
				window->emission_xid = XLogRecGetXid(reader);
				window->sysid = first_segment->sysid;
				window->segsize = first_segment->segsize;
				windows = lappend(windows, window);
				if (PgUpgradeReadMarker(info, XLogRecGetData(reader),
										XLogRecGetDataLen(reader), &window->start,
										&window->error))
				{
					if (window->start.new_major / 10000 != PG_VERSION_NUM / 10000 ||
						strncmp(window->start.pg_version, PG_MAJORVERSION "\n",
								sizeof(window->start.pg_version)) != 0)
						window->error = "START specifies a different target major version";
					if (!TransactionIdIsNormal(window->emission_xid))
						window->error = "upgrade START has no emitting transaction";
				}
			}
			else if (info == XLOG_UPGRADE_COMPLETE && window != NULL)
			{
				xl_pg_upgrade_marker complete;

				if (PgUpgradeReadMarker(info, XLogRecGetData(reader),
										XLogRecGetDataLen(reader), &complete,
										&window->error))
				{
					if (complete.old_major != window->start.old_major ||
						complete.new_major != window->start.new_major ||
						memcmp(complete.pg_version, window->start.pg_version,
							   sizeof(complete.pg_version)) != 0)
						window->error = "START and COMPLETE specify different major versions";
				}
				window->complete_record_end = reader->EndRecPtr;
				if (XLogRecGetXid(reader) != window->emission_xid)
					window->error = "upgrade COMPLETE belongs to another transaction";
			}
		}
		else if (window != NULL &&
				 rmid == RM_XACT_ID && XLogRecGetXid(reader) == window->emission_xid)
		{
			if ((info & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT &&
				!XLogRecPtrIsInvalid(window->complete_record_end) &&
				XLogRecGetDataLen(reader) >= MinSizeOfXactCommit)
				window->complete_end_lsn = reader->EndRecPtr;
			else
				window->error = "upgrade transaction did not commit a complete window";
			window = NULL;
		}
	}

	XLogReaderFree(reader);
	return windows;
}

static bool
ReadUpgradeWalSegment(const char *waldir, const char *filename,
					  UpgradeWalSegment * segment)
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
				 errhint("Stage the new-major shutdown checkpoint and upgrade window "
						 "before starting archive upgrade recovery.")));
	return segsize;
}

/* Scan contiguous new-major WAL by timeline, system identifier, and size. */
static List *
FindUpgradeWalWindows(const char *waldir)
{
	DIR		   *dir;
	struct dirent *de;
	UpgradeWalSegment *segments;
	int			nsegments = 0;
	int			capacity = 16;
	List	   *windows = NIL;

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
		windows = ScanUpgradeWalRun(waldir, &segments[begin], &segments[end], windows);
		begin = end + 1;
	}
	pfree(segments);
	return windows;
}

/*
 * Select the latest upgrade window in the requested timeline history.
 * Copies of a window in promoted segments must agree on its boundaries.
 */
static UpgradeWalWindow *
SelectUpgradeWalWindow(List *windows, List *history)
{
	UpgradeWalWindow *selected = NULL;
	ListCell   *lc;

	foreach(lc, windows)
	{
		UpgradeWalWindow *window = lfirst(lc);
		TimeLineID	start_tli;

		if (!tliInHistory(window->file_tli, history))
			continue;
		/* A promoted segment can include START from an earlier timeline. */
		start_tli = tliOfPointInHistory(window->start_lsn, history);
		if (start_tli > window->file_tli)
			continue;

		if (!XLogRecPtrIsInvalid(window->replay_start_lsn) &&
			(window->checkpoint.ThisTimeLineID != start_tli ||
			 tliOfPointInHistory(window->replay_start_lsn, history) != start_tli))
			window->error = "the checkpoint and START are on different "
				"recovery timelines";
		if (!XLogRecPtrIsInvalid(window->complete_end_lsn) &&
			tliOfPointInHistory(window->complete_end_lsn - 1, history) != start_tli)
			window->error = "the requested recovery history leaves the "
				"upgrade window before COMPLETE";

		if (selected == NULL || window->start_lsn > selected->start_lsn)
			selected = window;
		else if (window->start_lsn == selected->start_lsn)
		{
			if (window->sysid != selected->sysid ||
				(!XLogRecPtrIsInvalid(window->replay_start_lsn) &&
				 !XLogRecPtrIsInvalid(selected->replay_start_lsn) &&
				 window->replay_start_lsn != selected->replay_start_lsn) ||
				(!XLogRecPtrIsInvalid(window->complete_end_lsn) &&
				 !XLogRecPtrIsInvalid(selected->complete_end_lsn) &&
				 window->complete_end_lsn != selected->complete_end_lsn))
				ereport(FATAL,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("staged pg_upgrade WAL contains conflicting upgrade windows")));
			if ((XLogRecPtrIsInvalid(selected->replay_start_lsn) &&
				 !XLogRecPtrIsInvalid(window->replay_start_lsn)) ||
				(!XLogRecPtrIsInvalid(window->replay_start_lsn) &&
				 window->error == NULL &&
				 !XLogRecPtrIsInvalid(window->complete_end_lsn)))
				selected = window;
		}
	}

	if (selected == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not find a staged pg_upgrade window in the "
						"requested recovery history"),
				 errhint("Stage the upgrade checkpoint and "
						 "START-through-COMPLETE WAL for the requested "
						 "recovery timeline.")));
	if (XLogRecPtrIsInvalid(selected->replay_start_lsn))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_upgrade WAL is missing the shutdown checkpoint "
						"before START")));
	if (XLogRecPtrIsInvalid(selected->complete_end_lsn))
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_upgrade WAL is incomplete: found START without "
						"committed COMPLETE"),
				 errdetail_internal("%s", selected->error ? selected->error :
									"The staged WAL ends before upgrade "
									"completion."),
				 errhint("Discard this new-version attempt and retry from the "
						 "retained old cluster.")));
	if (selected->error != NULL)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid pg_upgrade WAL window"),
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

/* Validate the selected window's record chain and page timelines. */
static void
VerifyUpgradeWalWindow(const UpgradeWalWindow * window, List *history)
{
	UpgradeWalReadPrivate priv;
	XLogReaderState *reader;
	bool		found_checkpoint = false;
	bool		found_start = false;
	bool		found_complete = false;
	char	   *errormsg = NULL;

	strlcpy(priv.dir, XLOGDIR, sizeof(priv.dir));
	priv.tli = window->file_tli;
	priv.endptr = window->complete_end_lsn;
	priv.history = history;
	reader = XLogReaderAllocate(window->segsize, NULL,
								XL_ROUTINE(.page_read = UpgradeWalPageRead,
										   .segment_open = UpgradeWalSegOpen,
										   .segment_close = UpgradeWalSegClose),
								&priv);
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	reader->system_identifier = window->sysid;
	XLogBeginRead(reader, window->replay_start_lsn);
	while (XLogReadRecord(reader, &errormsg) != NULL)
	{
		uint8		rmid = XLogRecGetRmid(reader);
		uint8		info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (reader->ReadRecPtr == window->replay_start_lsn)
			found_checkpoint = rmid == RM_XLOG_ID && info == XLOG_CHECKPOINT_SHUTDOWN &&
				XLogRecGetDataLen(reader) == sizeof(CheckPoint) &&
				memcmp(XLogRecGetData(reader), &window->checkpoint, sizeof(CheckPoint)) == 0;
		if (reader->ReadRecPtr == window->start_lsn)
		{
			xl_pg_upgrade_marker marker;
			const char *marker_error = NULL;

			found_start = rmid == RM_PG_UPGRADE_ID && info == XLOG_UPGRADE_START &&
				PgUpgradeReadMarker(info, XLogRecGetData(reader), XLogRecGetDataLen(reader),
									&marker, &marker_error) &&
				memcmp(&marker, &window->start, sizeof(marker)) == 0;
		}
		if (reader->EndRecPtr >= window->complete_end_lsn)
		{
			found_complete = reader->EndRecPtr == window->complete_end_lsn &&
				rmid == RM_XACT_ID && (info & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT &&
				XLogRecGetXid(reader) == window->emission_xid;
			break;
		}
	}
	if (!found_checkpoint || !found_start || !found_complete)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not validate the selected pg_upgrade WAL window"),
				 errdetail_internal("%s", errormsg ? errormsg : "The staged "
									"upgrade records changed during discovery.")));
	XLogReaderFree(reader);
}


/* Startup selected an upgrade window for replay. */
static bool upgrade_replay_selected = false;
static List *upgrade_handoff_slot_names = NIL;

/* HANDOFF checkpoint state until its restartpoint is durable. */
static bool handoff_checkpoint_pending = false;
static bool handoff_checkpoint_replayed = false;
static XLogRecPtr handoff_checkpoint_lsn = InvalidXLogRecPtr;
static TimeLineID handoff_checkpoint_tli = 0;
static uint32 handoff_target_major = 0;

/*
 * Finalize the committed window after its shutdown checkpoint becomes a
 * durable restartpoint.
 */
static bool upgrade_complete_checkpoint_pending = false;
static bool upgrade_complete_checkpoint_replayed = false;
static XLogRecPtr upgrade_complete_end_lsn = InvalidXLogRecPtr;
static XLogRecPtr upgrade_complete_checkpoint_lsn = InvalidXLogRecPtr;
static TimeLineID upgrade_complete_checkpoint_tli = 0;

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

static uint32
ParseOldControlVersion(const char *line)
{
	static const char prefix[] = "pg_controldata (PostgreSQL) ";
	const char *p;
	uint32		parts[2] = {0, 0};

	if (strncmp(line, prefix, sizeof(prefix) - 1) != 0 ||
		OldControlLineHasWarning(line))
		elog(FATAL, "invalid version output from matching old pg_controldata");
	p = line + sizeof(prefix) - 1;
	for (int part = 0; part < 2; part++)
	{
		const char *start = p;

		while (*p >= '0' && *p <= '9')
		{
			unsigned	digit = *p++ - '0';

			if (parts[part] > (PG_UINT32_MAX - digit) / 10)
				elog(FATAL, "overflow in matching old pg_controldata version");
			parts[part] = parts[part] * 10 + digit;
		}
		if (p == start)
			elog(FATAL, "missing number in matching old pg_controldata version");
		if (part == 1 || parts[0] >= 10)
			break;
		if (*p++ != '.')
			elog(FATAL, "missing minor version from matching old pg_controldata");
	}
	if (parts[0] == 0 || parts[0] > PG_UINT32_MAX / 10000 ||
		parts[1] > 99)
		elog(FATAL, "invalid major version from matching old pg_controldata");
	return parts[0] * 10000 + parts[1] * 100;
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
	if (old_datadir == NULL)
		appendStringInfoString(&command, " --version");
	else
	{
		appendStringInfoString(&command, " -D ");
		AppendShellArg(&command, old_datadir);
	}
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
ReadOldControlVersion(const char *utility)
{
	FILE	   *output = OpenOldControlPipe(utility, NULL);
	char		line[MAXPGPATH * 2];
	char		extra[MAXPGPATH * 2];
	int			status;
	bool		got_line;
	bool		got_extra = false;

	got_line = fgets(line, sizeof(line), output) != NULL;
	while (fgets(extra, sizeof(extra), output) != NULL)
		got_extra = true;
	if (ferror(output))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read matching old pg_controldata version: %m")));
	status = ClosePipeStream(output);
	if (status != 0 || !got_line || got_extra ||
		(strlen(line) == sizeof(line) - 1 && line[sizeof(line) - 2] != '\n'))
		ereport(FATAL,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("matching old pg_controldata returned invalid version output")));
	pg_strip_crlf(line);
	return ParseOldControlVersion(line);
}

/*
 * Run the old installation's pg_controldata to read identity, layout, and
 * checkpoint fields.
 */
static void
ReadUpgradeControlData(const char *old_datadir,
					   OldUpgradeControlData * result, bool require_handoff)
{
	char		opts_path[MAXPGPATH];
	char		line[MAXPGPATH * 2];
	char		old_bindir[MAXPGPATH];
	char		utility[MAXPGPATH];
	char	   *first_arg;
	FILE	   *opts;
	FILE	   *output;
	char	   *control_warning = NULL;
	bool		got_system_identifier = false;
	bool		got_control_version = false;
	bool		got_catalog_version = false;
	bool		got_block_size = false;
	bool		got_blocks_per_segment = false;
	bool		got_wal_block_size = false;
	bool		got_state = false;
	bool		got_checkpoint = false;
	bool		got_redo = false;
	bool		got_checkpoint_end = false;
	bool		got_tli = false;
	bool		got_checkpoint_end_tli = false;
	bool		got_segsize = false;
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

	result->major_version = ReadOldControlVersion(utility);
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

		if (strcmp(line, "Database system identifier") == 0)
		{
			if (got_system_identifier)
				elog(FATAL, "duplicate system identifier in old pg_controldata "
					 "output");
			result->system_identifier = ParseOldControlNumber(value,
															  "system identifier",
															  PG_UINT64_MAX);
			got_system_identifier = true;
		}
		else if (strcmp(line, "pg_control version number") == 0)
		{
			if (got_control_version)
				elog(FATAL, "duplicate control version in old pg_controldata "
					 "output");
			result->control_version = (uint32) ParseOldControlNumber(value,
																	 "control version",
																	 PG_UINT32_MAX);
			got_control_version = true;
		}
		else if (strcmp(line, "Catalog version number") == 0)
		{
			if (got_catalog_version)
				elog(FATAL, "duplicate catalog version in old pg_controldata "
					 "output");
			result->catalog_version = (uint32) ParseOldControlNumber(value,
																	 "catalog version",
																	 PG_UINT32_MAX);
			got_catalog_version = true;
		}
		else if (strcmp(line, "Database block size") == 0)
		{
			if (got_block_size)
				elog(FATAL, "duplicate database block size in old pg_controldata "
					 "output");
			result->block_size = (uint32) ParseOldControlNumber(value,
																"database block size",
																PG_UINT32_MAX);
			got_block_size = true;
		}
		else if (strcmp(line, "Blocks per segment of large relation") == 0)
		{
			if (got_blocks_per_segment)
				elog(FATAL, "duplicate relation segment size in old "
					 "pg_controldata output");
			result->blocks_per_segment = (uint32) ParseOldControlNumber(value,
																		"relation segment size",
																		PG_UINT32_MAX);
			got_blocks_per_segment = true;
		}
		else if (strcmp(line, "WAL block size") == 0)
		{
			if (got_wal_block_size)
				elog(FATAL, "duplicate WAL block size in old pg_controldata "
					 "output");
			result->wal_block_size = (uint32) ParseOldControlNumber(value,
																	"WAL block size",
																	PG_UINT32_MAX);
			got_wal_block_size = true;
		}
		else if (strcmp(line, "Database cluster state") == 0)
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
								 "directory as the pg_upgrade RELINK source.")));
			got_state = true;
		}
		else if (strcmp(line, "Latest checkpoint location") == 0)
		{
			if (got_checkpoint)
				elog(FATAL, "duplicate checkpoint location in old "
					 "pg_controldata output");
			result->checkpoint_lsn = ParseControlLSN(value,
													 "checkpoint location");
			got_checkpoint = true;
		}
		else if (strcmp(line, "Latest checkpoint's REDO location") == 0)
		{
			if (got_redo)
				elog(FATAL, "duplicate checkpoint REDO in old pg_controldata "
					 "output");
			result->checkpoint_redo = ParseControlLSN(value,
													  "checkpoint REDO");
			got_redo = true;
		}
		else if (strcmp(line, "Latest checkpoint's TimeLineID") == 0)
		{
			if (got_tli)
				elog(FATAL, "duplicate checkpoint timeline in old "
					 "pg_controldata output");
			result->checkpoint_tli = (TimeLineID) ParseOldControlNumber(value,
																		"checkpoint timeline",
																		PG_UINT32_MAX);
			got_tli = true;
		}
		else if (strcmp(line, "Minimum recovery ending location") == 0)
		{
			if (got_checkpoint_end)
				elog(FATAL, "duplicate minimum recovery location in old "
					 "pg_controldata output");
			result->checkpoint_end_lsn =
				ParseControlLSN(value, "minimum recovery location");
			got_checkpoint_end = true;
		}
		else if (strcmp(line, "Min recovery ending loc's timeline") == 0)
		{
			if (got_checkpoint_end_tli)
				elog(FATAL, "duplicate minimum recovery timeline in old "
					 "pg_controldata output");
			result->checkpoint_end_tli = strcmp(value, "0") == 0 ? 0 :
				(TimeLineID) ParseOldControlNumber(value,
												   "minimum recovery timeline",
												   PG_UINT32_MAX);
			got_checkpoint_end_tli = true;
		}
		else if (strcmp(line, "Bytes per WAL segment") == 0)
		{
			uint64		parsed;

			if (got_segsize)
				elog(FATAL, "duplicate WAL segment size in old pg_controldata output");
			parsed = ParseOldControlNumber(value, "WAL segment size", INT_MAX);
			if (!IsValidWalSegSize((int) parsed))
				elog(FATAL, "invalid WAL segment size in old pg_controldata "
					 "output");
			result->wal_segment_size = (int) parsed;
			got_segsize = true;
		}
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

	if (!got_state || !got_checkpoint || !got_redo ||
		!got_checkpoint_end || !got_tli || !got_checkpoint_end_tli ||
		!got_segsize || !got_system_identifier || !got_control_version ||
		!got_catalog_version || !got_block_size || !got_blocks_per_segment ||
		!got_wal_block_size)
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
		elog(FATAL, "old source has invalid checkpoint identity or recovery "
			 "positions");
}

void
ReadOldUpgradeControlData(const char *old_datadir, OldUpgradeControlData * result)
{
	ReadUpgradeControlData(old_datadir, result, true);
}

#define OLD_REPLICATION_SLOT_MAGIC 0x1051CA1

/*
 * Source slot-state versions 2, 3, and 5 use native struct layouts with a
 * common prefix through restart_lsn. Version 2 stores invalidated_at after
 * that prefix. Versions 3 and 5 store an invalidation cause there.
 */
typedef struct OldReplicationSlotHeader
{
	uint32		magic;
	pg_crc32c	checksum;
	uint32		version;
	uint32		length;
}			OldReplicationSlotHeader;

typedef struct OldReplicationSlotPrefix
{
	NameData	name;
	Oid			database;
	ReplicationSlotPersistency persistency;
	TransactionId xmin;
	TransactionId catalog_xmin;
	XLogRecPtr	restart_lsn;
}			OldReplicationSlotPrefix;

typedef struct OldReplicationSlotV2Data
{
	OldReplicationSlotPrefix prefix;
	XLogRecPtr	invalidated_at;
	XLogRecPtr	confirmed_flush;
	XLogRecPtr	two_phase_at;
	bool		two_phase;
	NameData	plugin;
}			OldReplicationSlotV2Data;

typedef struct OldReplicationSlotV3Data
{
	OldReplicationSlotPrefix prefix;
	ReplicationSlotInvalidationCause invalidated;
	XLogRecPtr	confirmed_flush;
	XLogRecPtr	two_phase_at;
	bool		two_phase;
	NameData	plugin;
}			OldReplicationSlotV3Data;

static uint32
OldReplicationSlotVersion(uint32 major_version)
{
	switch (major_version / 10000)
	{
		case 14:
		case 15:
			return 2;
		case 16:
			return 3;
		case 17:
		case 18:
		case 19:
		case 20:
			return 5;
	}

	elog(FATAL, "unsupported old major version %u for replication slot migration",
		 major_version / 10000);
	pg_unreachable();
}

/*
 * Read persistent physical slot names from the retained old data directory.
 * Require each restart LSN to be at or beyond the final shutdown checkpoint
 * end.
 */
static List *
ReadOldPhysicalSlotNames(const char *old_datadir, uint32 major_version,
						 XLogRecPtr checkpoint_end_lsn)
{
	char		slotdir[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	List	   *names = NIL;
	uint32		old_major = major_version / 10000;
	uint32		expected_version = 0;
	Size		expected_length = 0;

	if (snprintf(slotdir, sizeof(slotdir), "%s/%s", old_datadir,
				 PG_REPLSLOT_DIR) >= sizeof(slotdir))
		elog(FATAL, "old replication slot directory path is too long");

	dir = AllocateDir(slotdir);
	if (dir == NULL && errno == ENOENT && major_version < 90400)
		return NIL;
	while ((de = ReadDirExtended(dir, slotdir, FATAL)) != NULL)
	{
		char		slotpath[MAXPGPATH];
		char		path[MAXPGPATH];
		int			fd;
		struct stat st;
		OldReplicationSlotHeader *stored_header;
		OldReplicationSlotPrefix *slot;
		char	   *contents;
		size_t		total_size;
		pg_crc32c	checksum;
		bool		invalidated;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0 ||
			pg_str_endswith(de->d_name, ".tmp"))
			continue;
		if (snprintf(slotpath, sizeof(slotpath), "%s/%s", slotdir,
					 de->d_name) >= sizeof(slotpath))
			elog(FATAL, "old replication slot path is too long");
		if (get_dirent_type(slotpath, de, false, FATAL) != PGFILETYPE_DIR)
			continue;
		if (expected_version == 0)
		{
			expected_version = OldReplicationSlotVersion(major_version);
			switch (expected_version)
			{
				case 2:
					expected_length = sizeof(OldReplicationSlotV2Data);
					break;
				case 3:
					expected_length = sizeof(OldReplicationSlotV3Data);
					break;
				case 5:
					expected_length = sizeof(ReplicationSlotPersistentData);
					break;
				default:
					pg_unreachable();
			}
		}
		if (snprintf(path, sizeof(path), "%s/state", slotpath) >= sizeof(path))
			elog(FATAL, "old replication slot state path is too long");

		fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read old replication slot file \"%s\": %m", path)));

		total_size = sizeof(OldReplicationSlotHeader) + expected_length;
		if (st.st_size != total_size)
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("old replication slot file \"%s\" has an invalid length", path)));
		contents = palloc(total_size);
		if (pg_pread(fd, contents, total_size, 0) != total_size ||
			CloseTransientFile(fd) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read old replication slot file \"%s\": %m", path)));

		stored_header = (OldReplicationSlotHeader *) contents;
		if (stored_header->magic != OLD_REPLICATION_SLOT_MAGIC ||
			stored_header->version != expected_version ||
			stored_header->length != expected_length)
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("old replication slot file \"%s\" has an invalid header", path)));

		INIT_CRC32C(checksum);
		COMP_CRC32C(checksum,
					contents + offsetof(OldReplicationSlotHeader, version),
					total_size - offsetof(OldReplicationSlotHeader, version));
		FIN_CRC32C(checksum);
		if (!EQ_CRC32C(checksum, stored_header->checksum))
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("old replication slot file \"%s\" has an invalid "
							"checksum", path)));

		slot = (OldReplicationSlotPrefix *) (contents + sizeof(*stored_header));
		if (strnlen(NameStr(slot->name), NAMEDATALEN) == NAMEDATALEN ||
			strcmp(NameStr(slot->name), de->d_name) != 0)
			ereport(FATAL,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("old replication slot file \"%s\" has an invalid "
							"name", path)));

		if (stored_header->version == 2)
		{
			XLogRecPtr	invalidated_at;

			memcpy(&invalidated_at, contents + sizeof(*stored_header) +
				   sizeof(*slot), sizeof(invalidated_at));
			invalidated = XLogRecPtrIsValid(invalidated_at);
		}
		else
		{
			uint32		invalidation_cause;

			memcpy(&invalidation_cause, contents + sizeof(*stored_header) +
				   sizeof(*slot), sizeof(invalidation_cause));
			invalidated = invalidation_cause != 0;
		}

		if (slot->persistency == RS_PERSISTENT &&
			slot->database == InvalidOid)
		{
			if (strcmp(NameStr(slot->name), CONFLICT_DETECTION_SLOT) == 0)
			{
				if (old_major >= 19)
				{
					pfree(contents);
					continue;
				}
				ereport(FATAL,
						(errcode(ERRCODE_RESERVED_NAME),
						 errmsg("old physical replication slot \"%s\" uses a "
								"reserved name", NameStr(slot->name)),
						 errhint("Drop or rename the slot before starting "
								 "pg_upgrade.")));
			}
			ReplicationSlotValidateName(NameStr(slot->name), false, FATAL);
			if (!XLogRecPtrIsValid(slot->restart_lsn) || invalidated)
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("old physical replication slot \"%s\" is not "
								"valid", NameStr(slot->name))));
			if (slot->restart_lsn < checkpoint_end_lsn)
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("old physical replication slot \"%s\" has not "
								"durably received the final shutdown checkpoint",
								NameStr(slot->name))));
			names = lappend(names, pstrdup(NameStr(slot->name)));
		}
		pfree(contents);
	}
	FreeDir(dir);

	return names;
}

/* Read old-major control data without requiring HANDOFF state. */
void
ReadArchiveUpgradeControlData(const char *old_datadir, OldUpgradeControlData * result)
{
	ReadUpgradeControlData(old_datadir, result, false);
}

static bool
UpgradeSignalStaged(void)
{
	char		path[MAXPGPATH];
	struct stat st;

	snprintf(path, sizeof(path), "%s/%s", DataDir, PG_UPGRADE_SIGNAL_FILE);
	return stat(path, &st) == 0;
}

/* With pg_upgrade.signal present, standby.signal takes precedence. */
UpgradeRecoveryMode
GetUpgradeRecoveryMode(void)
{
	char		path[MAXPGPATH];
	struct stat st;

	if (!UpgradeSignalStaged())
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
static bool
ArmFromLocalDerivationIfConfigured(UpgradeRecoveryMode mode)
{
	WalReceiverConn *conn;
	char	   *err = NULL;
	TimeLineID	primary_tli = 0;
	char	   *sysid_str;
	uint64		sysid = 0;
	const char *old_datadir;
	OldUpgradeControlData old_control;
	XLogSegNo	replay_start_segno;
	XLogRecPtr	replay_start_lsn;
	CheckPoint	replay_start_checkpoint;

	if (mode != UPGRADE_RECOVERY_STANDBY)
		return false;

	if (GetControlFileUpgradeFinalized())
		return false;

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
	upgrade_handoff_slot_names =
		ReadOldPhysicalSlotNames(old_datadir, old_control.major_version,
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
						 "the upgrade window.")));

	/* Start after the last segment occupied by the old shutdown checkpoint. */
	XLByteToPrevSeg(old_control.checkpoint_end_lsn, replay_start_segno,
					old_control.wal_segment_size);
	replay_start_segno++;

	/* pg_resetwal places the new checkpoint after the long page header. */
	XLogSegNoOffsetToRecPtr(replay_start_segno, SizeOfXLogLongPHD,
							old_control.wal_segment_size,
							replay_start_lsn);

	MemSet(&replay_start_checkpoint, 0, sizeof(replay_start_checkpoint));
	replay_start_checkpoint.redo = replay_start_lsn;
	/* Use the upgrade timeline even if the primary has since been promoted. */
	replay_start_checkpoint.ThisTimeLineID = old_control.checkpoint_tli;
	replay_start_checkpoint.PrevTimeLineID = old_control.checkpoint_tli;

	ereport(LOG,
			(errmsg("auto-armed streaming standby from the retained final "
					"checkpoint (sysid " UINT64_FORMAT ", upgrade replay "
					"start LSN %X/%08X, redo %X/%08X, TLI %u, upgrade "
					"replay start segment %llu)",
					sysid, LSN_FORMAT_ARGS(replay_start_lsn),
					LSN_FORMAT_ARGS(replay_start_checkpoint.redo),
					replay_start_checkpoint.ThisTimeLineID,
					(unsigned long long) replay_start_segno)));

	ArmControlFileForUpgradeRecovery(&replay_start_checkpoint,
									 replay_start_lsn, sysid, true);
	return true;
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
	bool		started = GetControlFileUpgradeStarted();
	bool		finalized = GetControlFileUpgradeFinalized();
	bool		archive_request;
	bool		crc_ok;
	ControlFileData *control;
	TimeLineID	base_tli;
	List	   *windows;
	List	   *history;
	ListCell   *lc;
	UpgradeWalWindow *window;

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
				 errmsg("the pg_upgrade window was only partially applied"),
				 errhint("Discard this new-version skeleton and retry the "
						 "upgrade with a fresh skeleton.")));

	mode = GetUpgradeRecoveryMode();
	if (mode == UPGRADE_RECOVERY_NONE)
		return;

	archive_request = mode == UPGRADE_RECOVERY_ARCHIVE;

	/* Restart a finalized standby without selecting its upgrade window again. */
	if (finalized && !archive_request)
		return;

	if (ArmFromLocalDerivationIfConfigured(mode))
	{
		upgrade_replay_selected = true;

		pgUpgradeReplayInProgress = true;

		return;
	}

	if (!archive_request)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("pg_upgrade.signal requires archive or streaming "
						"upgrade recovery"),
				 errhint("Use recovery.signal for archive recovery, or "
						 "standby.signal with primary_conninfo for streaming "
						 "recovery.")));

	control = get_controlfile(DataDir, &crc_ok);
	if (!crc_ok)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("incorrect checksum in control file")));
	base_tli = Max(control->checkPointCopy.ThisTimeLineID,
				   control->minRecoveryPointTLI);
	windows = FindUpgradeWalWindows(XLOGDIR);

	/* For synthesized pg_control, use the requested or staged checkpoint TLI. */
	if (base_tli == 0)
	{
		if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_NUMERIC)
			base_tli = recoveryTargetTLIRequested;
		else
		{
			foreach(lc, windows)
			{
				UpgradeWalWindow *candidate = lfirst(lc);

				if (XLogRecPtrIsInvalid(candidate->replay_start_lsn) ||
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
	window = SelectUpgradeWalWindow(windows, history);
	VerifyUpgradeWalWindow(window, history);

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

	if (finalized && control->system_identifier == window->sysid &&
		control->checkPointCopy.redo >= window->complete_end_lsn)
	{
		pfree(control);
		list_free_deep(history);
		list_free_deep(windows);
		return;
	}
	if (control->checkPoint >= window->replay_start_lsn ||
		control->minRecoveryPoint > window->replay_start_lsn)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("the retained recovery state does not precede the "
						"selected pg_upgrade window"),
				 errhint("Restore the old cluster to its pre-upgrade boundary "
						 "before retrying upgrade recovery.")));

	upgrade_replay_selected = true;
	pgUpgradeReplayInProgress = true;

	ereport(LOG,
			(errmsg("starting pg_upgrade archive recovery from checkpoint "
					"%X/%08X on timeline %u",
					LSN_FORMAT_ARGS(window->replay_start_lsn),
					window->checkpoint.ThisTimeLineID)));
	ArmControlFileForUpgradeRecovery(&window->checkpoint,
									 window->replay_start_lsn,
									 window->sysid, false);
	pfree(control);
	list_free_deep(history);
	list_free_deep(windows);
}

/*
 * Validate or recreate the retained standby's persistent physical slots at
 * the upgrade replay start LSN.
 */
void
PreparePgUpgradeStandbySlots(XLogRecPtr replay_start_lsn)
{
	int			used_slots = 0;
	int			missing_slots = 0;

	if (upgrade_handoff_slot_names == NIL)
		return;
	Assert(upgrade_replay_selected);
	if (!enableFsync)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("migrated physical replication slots require \"fsync\" to be "
						"enabled")));
	if (max_slot_wal_keep_size_mb != -1)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("migrated physical replication slots require "
						"\"max_slot_wal_keep_size\" to be -1")));
	if (idle_replication_slot_timeout_secs != 0)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("migrated physical replication slots require "
						"\"idle_replication_slot_timeout\" to be 0")));
	if (max_replication_slots == 0 || wal_level < WAL_LEVEL_REPLICA)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("migrated physical replication slots require replication slots and "
						"\"wal_level\" of \"replica\" or higher")));

	for (int i = 0; i < max_replication_slots; i++)
		if (ReplicationSlotCtl->replication_slots[i].in_use)
			used_slots++;

	foreach_ptr(char, name, upgrade_handoff_slot_names)
	{
		if (SearchNamedReplicationSlot(name, true) == NULL)
		{
			missing_slots++;
			continue;
		}

		ReplicationSlotAcquire(name, true, true);
		if (ReplicationSlotIndex(MyReplicationSlot) >= max_replication_slots ||
			!SlotIsPhysical(MyReplicationSlot) ||
			MyReplicationSlot->data.persistency != RS_PERSISTENT ||
			MyReplicationSlot->data.restart_lsn != replay_start_lsn ||
			MyReplicationSlot->last_saved_restart_lsn != replay_start_lsn)
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("replication slot \"%s\" is not an inactive "
							"persistent physical slot at the pg_upgrade replay "
							"start LSN", name)));
		ReplicationSlotRelease();
	}

	if (used_slots + missing_slots > max_replication_slots)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("not enough replication slots for the retained standby's physical slots"),
				 errhint("Increase \"max_replication_slots\".")));

	foreach_ptr(char, name, upgrade_handoff_slot_names)
	{
		if (SearchNamedReplicationSlot(name, true) != NULL)
			continue;

		ReplicationSlotCreate(name, false, RS_EPHEMERAL, false, false,
							  false, false);
		ReplicationSlotReserveWal();
		Assert(MyReplicationSlot->data.restart_lsn == replay_start_lsn);
		ReplicationSlotPersist();
		Assert(MyReplicationSlot->last_saved_restart_lsn == replay_start_lsn);
		ReplicationSlotRelease();
	}

	list_free_deep(upgrade_handoff_slot_names);
	upgrade_handoff_slot_names = NIL;
}

static xl_pg_upgrade_start upgrade_start;
static bool upgrade_active;
static bool complete_pending;
static HTAB *storage_roots;
static MemoryContext replay_context;
static bool source_inplace;
static bool source_at_boundary;
static const char *source_datadir;
static char source_version_directory[MAXFNAMELEN];
static TransactionId window_xid;

typedef struct ReplayRelinkState
{
	bool		active;
	bool		saw_directory;
	bool		saw_relation;
	bool		saw_inherited_file;
	xl_pg_upgrade_relink_entry relation;
	bool		rebuilt_forks[MAX_FORKNUM + 1];
	bool		inherited_forks[MAX_FORKNUM + 1];
	bool		short_forks[MAX_FORKNUM + 1];

	/*
	 * Derive segment numbers from FILE INHERIT order within each relation and
	 * fork.
	 */
	uint32		next_segment[MAX_FORKNUM + 1];
	uint32		place_segment[MAX_FORKNUM + 1];
	int			last_inherited_fork;
}			ReplayRelinkState;

static ReplayRelinkState relink_state;

static void
sync_parent(const char *path)
{
	char	   *parent = pstrdup(path);

	get_parent_directory(parent);
	fsync_fname(parent[0] != '\0' ? parent : ".", true);
	pfree(parent);
}

static bool
have_source(void)
{
	return pg_upgrade_standby_old_datadir != NULL &&
		pg_upgrade_standby_old_datadir[0] != '\0';
}

static void
make_directory(const char *path)
{
	struct stat st;
	char	   *writable = pstrdup(path);

	if (pg_mkdir_p(writable, pg_dir_create_mode) != 0 && errno != EEXIST)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not create upgrade directory \"%s\": %m", path)));
	if (lstat(path, &st) != 0 || !S_ISDIR(st.st_mode))
		elog(PANIC, "upgrade destination is not a directory: %s", path);
	fsync_fname(path, true);
	sync_parent(path);
	pfree(writable);
}

/* Map the target tablespace from the retained link or in-place path. */
static void
prepare_tablespace(const xl_pg_upgrade_relink_entry *entry)
{
	Oid			oid = entry->key.tablespace_oid;
	char		path[MAXPGPATH];
	struct stat st;

	if (oid == 0 || oid == DEFAULTTABLESPACE_OID ||
		oid == GLOBALTABLESPACE_OID)
		return;
	snprintf(path, sizeof(path), "pg_tblspc/%u", oid);
	if (lstat(path, &st) != 0)
	{
		char		source[MAXPGPATH];
		char		target[MAXPGPATH];
		ssize_t		length;

		if (errno != ENOENT)
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not stat \"%s\": %m", path)));
		if (entry->flags & UPGRADE_RELINK_INPLACE)
			make_directory(path);
		else
		{
			if (source_datadir == NULL)
				elog(PANIC, "upgrade tablespace %u requires a local "
					 "destination mapping", oid);
			snprintf(source, sizeof(source), "%s/%s", source_datadir, path);
			length = readlink(source, target, sizeof(target) - 1);
			if (length <= 0 || length >= sizeof(target) - 1)
				elog(PANIC, "could not read retained tablespace link %s",
					 source);
			target[length] = '\0';
			if (!is_absolute_path(target))
				elog(PANIC, "retained tablespace link must have an absolute "
					 "target: %s", source);
			if (symlink(target, path) != 0)
				ereport(PANIC, (errcode_for_file_access(),
								errmsg("could not create \"%s\": %m", path)));
			fsync_fname("pg_tblspc", true);
		}
	}
	else if (!S_ISDIR(st.st_mode) && !S_ISLNK(st.st_mode))
		elog(PANIC, "invalid upgrade tablespace mapping: %s", path);
}

static PGFileIdentity
directory_identity(const char *path, const struct stat *st)
{
	PGFileIdentity identity;

	if (!pg_get_file_identity(path, st, &identity))
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not read directory identity of \"%s\": %m",
							   path)));
	return identity;
}

typedef struct ReplayStorageRoot
{
	PGFileIdentity identity;
	Oid			tablespace;
	bool		source;
}			ReplayStorageRoot;

static void
storage_root_path(Oid tablespace, bool source, char *path)
{
	const char *datadir = source ? source_datadir : DataDir;

	if (tablespace == DEFAULTTABLESPACE_OID)
		snprintf(path, MAXPGPATH, "%s/base", datadir);
	else if (tablespace == GLOBALTABLESPACE_OID)
		snprintf(path, MAXPGPATH, "%s/global", datadir);
	else
		snprintf(path, MAXPGPATH, "%s/pg_tblspc/%u/%s", datadir, tablespace,
				 source ? source_version_directory : TABLESPACE_VERSION_DIRECTORY);
}

/* Reject distinct source or target roots that name the same directory. */
static void
check_storage_root(Oid tablespace, bool source)
{
	char		path[MAXPGPATH];
	struct stat st;
	PGFileIdentity identity;
	ReplayStorageRoot *root;
	bool		found;

	storage_root_path(tablespace, source, path);
	if (!source)
		make_directory(path);
	if (lstat(path, &st) != 0 || !S_ISDIR(st.st_mode))
		elog(PANIC, "upgrade storage root is not a local directory: %s",
			 path);
	identity = directory_identity(path, &st);
	root = hash_search(storage_roots, &identity, HASH_ENTER, &found);
	if (found && (root->tablespace != tablespace ||
				  (!source_inplace && root->source != source)))
		elog(PANIC, "upgrade destination aliases retained storage for "
			 "tablespace %u", tablespace);
	root->tablespace = tablespace;
	root->source = source;
}

/*
 * Validate START and bind transferred segments, when present, to retained
 * standby or archive-restored old storage.
 */
static void
bind_window(void)
{
	const xl_pg_upgrade_start *window = &upgrade_start;
	const xl_pg_upgrade_marker *marker = &upgrade_start.marker;
	OldUpgradeControlData old;

	if (marker->new_major != PG_MAJORVERSION_NUM * 10000 ||
		window->block_size != BLCKSZ ||
		window->relseg_blocks != RELSEG_SIZE ||
		window->wal_block_size != XLOG_BLCKSZ ||
		window->wal_segment_size != wal_segment_size ||
		window->slru_pages_per_segment != SLRU_PAGES_PER_SEGMENT ||
		window->transfer_mode > UPGRADE_RELINK_MODE_SWAP)
		elog(PANIC, "upgrade physical format does not match this server");
	if (have_source())
	{
		struct stat source;
		struct stat target;
		PGFileIdentity source_identity;
		PGFileIdentity target_identity;

		ReadOldUpgradeControlData(pg_upgrade_standby_old_datadir, &old);
		if (stat(pg_upgrade_standby_old_datadir, &source) != 0 ||
			stat(DataDir, &target) != 0)
			elog(PANIC, "upgrade requires a separate retained source "
				 "directory");
		source_identity = directory_identity(pg_upgrade_standby_old_datadir,
											 &source);
		target_identity = directory_identity(DataDir, &target);
		if (pg_compare_file_identity(source_identity, target_identity) == 0)
			elog(PANIC, "upgrade requires a separate retained source "
				 "directory");
		source_datadir = pg_upgrade_standby_old_datadir;
	}
	else if (GetUpgradeRecoveryMode() == UPGRADE_RECOVERY_ARCHIVE &&
			 GetArchiveUpgradeSource(&old) &&
			 old.system_identifier == window->old_system_identifier &&
			 old.major_version == window->marker.old_major &&
			 old.control_version == window->old_control_version &&
			 old.catalog_version == window->old_catalog_version)
	{
		source_datadir = DataDir;
		source_inplace = true;
	}
	if (source_datadir != NULL)
	{
		List	   *history = readTimeLineHistory(window->old_tli);

		if (old.system_identifier != window->old_system_identifier ||
			old.major_version != window->marker.old_major ||
			old.control_version != window->old_control_version ||
			old.catalog_version != window->old_catalog_version ||
			old.wal_segment_size != window->wal_segment_size ||
			old.block_size != window->block_size ||
			old.blocks_per_segment != window->relseg_blocks ||
			old.wal_block_size != window->wal_block_size ||
			old.checkpoint_lsn >= window->boundary_lsn ||
			old.checkpoint_end_lsn > window->boundary_lsn ||
			tliOfPointInHistory(old.checkpoint_lsn, history) !=
			old.checkpoint_tli ||
			(old.checkpoint_end_lsn != InvalidXLogRecPtr &&
			 tliOfPointInHistory(old.checkpoint_end_lsn - 1, history) !=
			 old.checkpoint_end_tli))
			elog(PANIC, "old source identity or recovery state does not "
				 "match the upgrade boundary");
		list_free_deep(history);

		/*
		 * Segment transfer requires a shutdown checkpoint ending at
		 * boundary_lsn on old_tli.
		 */
		source_at_boundary = old.checkpoint_lsn == old.checkpoint_redo &&
			old.checkpoint_end_lsn == window->boundary_lsn &&
			old.checkpoint_tli == window->old_tli &&
			old.checkpoint_end_tli == window->old_tli;
		if (!source_inplace && !source_at_boundary)
			elog(PANIC, "retained source does not match the upgrade "
				 "boundary");
		if (old.major_version >= 100000)
			snprintf(source_version_directory,
					 sizeof(source_version_directory),
					 "PG_%u_%u", old.major_version / 10000,
					 old.catalog_version);
		else
			snprintf(source_version_directory, sizeof(source_version_directory),
					 "PG_%u.%u_%u", old.major_version / 10000,
					 old.major_version / 100 % 100, old.catalog_version);
	}
	{
		HASHCTL		control = {0};

		control.keysize = sizeof(PGFileIdentity);
		control.entrysize = sizeof(ReplayStorageRoot);
		control.hcxt = replay_context;
		storage_roots = hash_create("upgrade storage roots", 32, &control,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	make_directory("pg_tblspc");
}

static void
inherited_filename(const xl_pg_upgrade_key *key, uint8 fork, uint32 segment,
				   char *path)
{
	RelPathStr	relative = GetRelationPath(key->database_oid, key->tablespace_oid,
										   key->filenumber, INVALID_PROC_NUMBER, fork);
	char		suffix[32] = "";
	char		root[MAXPGPATH];
	const char *filename = strrchr(relative.str, '/');

	if (source_datadir == NULL || !source_at_boundary)
		elog(PANIC, "segment transfer requires the old source at its final "
			 "HANDOFF checkpoint");
	if (segment != 0)
		snprintf(suffix, sizeof(suffix), ".%u", segment);
	storage_root_path(key->tablespace_oid, true, root);
	Assert(filename != NULL);
	if (key->tablespace_oid == GLOBALTABLESPACE_OID)
		snprintf(path, MAXPGPATH, "%s/%s%s", root, filename + 1, suffix);
	else
		snprintf(path, MAXPGPATH, "%s/%u/%s%s", root, key->database_oid,
				 filename + 1, suffix);
}

static bool
check_inherited_file(const char *path, const xl_pg_upgrade_relink_entry *entry)
{
	struct stat st;
	uint64		file_size;
	bool		optional;
	bool		valid_local_fsm_size;

	optional = entry->fork == FSM_FORKNUM ||
		entry->fork == VISIBILITYMAP_FORKNUM;
	if (lstat(path, &st) != 0)
	{
		if (errno == ENOENT && optional)
			return false;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not stat upgrade source \"%s\": %m",
							   path)));
	}
	if (!S_ISREG(st.st_mode))
		elog(PANIC, "inherited upgrade file is not a regular file: %s", path);
	file_size = (uint64) st.st_size;
	valid_local_fsm_size = entry->fork == FSM_FORKNUM &&
		st.st_size >= 0 && file_size % BLCKSZ == 0 &&
		file_size <= (uint64) RELSEG_SIZE * BLCKSZ;
	if (file_size != (uint64) entry->blocks * BLCKSZ &&
		!valid_local_fsm_size)
		elog(PANIC, "inherited upgrade file has the wrong size: %s", path);
	return true;
}

/* Remove each declared relation or database directory from the target. */
static void
ReplayUpgradeDeleteRoot(const xl_pg_upgrade_key *entries, int count)
{
	const xl_pg_upgrade_key *entry = entries;
	char	   *path;
	char	   *parent;
	struct stat st;

	path = GetDatabasePath(entry->database_oid, entry->tablespace_oid);
	if (lstat(path, &st) != 0)
	{
		if (errno != ENOENT)
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not stat upgrade DELETE target \"%s\": %m", path)));
		pfree(path);
		return;
	}
	if (!S_ISDIR(st.st_mode))
		elog(PANIC, "upgrade DELETE target \"%s\" is not a directory", path);
	if (entry->filenumber == 0)
	{
		DropDatabaseBuffers(entry->database_oid);
		ForgetDatabaseSyncRequests(entry->database_oid);
		XLogDropDatabase(entry->database_oid);
		WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE));
		if (!rmtree(path, true))
			elog(PANIC, "could not remove upgrade DELETE target \"%s\"", path);
		parent = strrchr(path, '/');
		Assert(parent != NULL);
		*parent = '\0';
	}
	else
	{
		RelFileLocator *locators = palloc_array(RelFileLocator, count);
		DIR		   *dir;
		struct dirent *de;

		for (int i = 0; i < count; i++)
			locators[i] = (RelFileLocator)
		{
			entries[i].tablespace_oid,
				entries[i].database_oid, entries[i].filenumber
		};

		/*
		 * Unlink every declared target relation file before
		 * DropRelationFiles().
		 */
		dir = AllocateDir(path);
		while ((de = ReadDirExtended(dir, path, PANIC)) != NULL)
		{
			xl_pg_upgrade_key key = *entry;
			ForkNumber	fork;
			unsigned	segno;
			char	   *filename;

			if (!parse_filename_for_nontemp_relation(de->d_name,
													 &key.filenumber, &fork, &segno) ||
				bsearch(&key, entries, count, sizeof(*entries),
						PgUpgradeCompareKeys) == NULL)
				continue;
			filename = psprintf("%s/%s", path, de->d_name);
			if (unlink(filename) != 0 && errno != ENOENT)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not remove upgrade DELETE target \"%s\": %m",
								filename)));
			pfree(filename);
		}
		FreeDir(dir);
		DropRelationFiles(locators, count, true);
		pfree(locators);
	}
	fsync_fname(path, true);
	pfree(path);
}

/*
 * Place one inherited relation file with the selected transfer mode and fsync
 * the resulting file.
 */
static void
PlaceInheritedFile(const char *oldfile, const char *newfile, uint8 mode)
{
	switch (mode)
	{
		case UPGRADE_RELINK_MODE_LINK:
			if (link(oldfile, newfile) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not link \"%s\" to \"%s\": %m",
								oldfile, newfile)));
			break;

		case UPGRADE_RELINK_MODE_SWAP:

			if (rename(oldfile, newfile) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not move \"%s\" to \"%s\": %m",
								oldfile, newfile)));
			break;

		case UPGRADE_RELINK_MODE_CLONE:
			{
				int			save_errno;

				switch (pg_clone_file(oldfile, newfile, &save_errno))
				{
					case PG_REFLINK_OK:
						break;
					case PG_REFLINK_ERROR:
						errno = save_errno;
						ereport(PANIC,
								(errcode_for_file_access(),
								 errmsg("could not clone \"%s\" to \"%s\": %m",
										oldfile, newfile),
								 errhint("The old data directory and the new "
										 "skeleton must be on the same "
										 "reflink-capable filesystem.")));
						break;
					case PG_REFLINK_UNSUPPORTED:
						ereport(PANIC,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("file cloning not supported on this "
										"platform"),
								 errhint("The primary used --clone; a standby on "
										 "this platform cannot reproduce it.")));
						break;
				}
				break;
			}

		case UPGRADE_RELINK_MODE_COPY_FILE_RANGE:
			{
				int			save_errno;

				switch (pg_copy_file_range_all(oldfile, newfile, &save_errno))
				{
					case PG_REFLINK_OK:
						break;
					case PG_REFLINK_ERROR:
						errno = save_errno;
						ereport(PANIC,
								(errcode_for_file_access(),
								 errmsg("could not copy_file_range \"%s\" to "
										"\"%s\": %m",
										oldfile, newfile)));
						break;
					case PG_REFLINK_UNSUPPORTED:
						ereport(PANIC,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("copy_file_range not supported on this "
										"platform"),
								 errhint("The primary used --copy-file-range; a "
										 "standby on this platform cannot "
										 "reproduce it.")));
						break;
				}
				break;
			}

		case UPGRADE_RELINK_MODE_COPY:
		default:
			copy_file(oldfile, newfile);
			break;
	}

	fsync_fname(newfile, false);
}


/* Remove the old fork before full-page replay recreates it. */
static void
PrepareUpgradeFileRecreate(const xl_pg_upgrade_relink_entry *entry)
{
	RelFileLocator locator = {entry->key.tablespace_oid,
	entry->key.database_oid, entry->key.filenumber};
	ForkNumber	fork = entry->fork;
	BlockNumber zero = 0;
	SMgrRelation rel = smgropen(locator, INVALID_PROC_NUMBER);
	RelPathStr	base = relpathperm(locator, fork);

	DropRelationBuffers(rel, &fork, 1, &zero);
	CacheInvalidateSmgr(rel->smgr_rlocator);
	smgrrelease(rel);
	XLogDropRelation(locator, fork);
	for (uint32 segment = 0;; segment++)
	{
		char	   *path = segment == 0 ? pstrdup(base.str) :
			psprintf("%s.%u", base.str, segment);
		FileTag		tag = {0};
		bool		missing;
		int			result;

		result = unlink(path);
		if (result != 0 && errno != ENOENT)
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not remove upgrade storage fork \"%s\": %m", path)));
		missing = result != 0;
		tag.handler = SYNC_HANDLER_MD;
		tag.rlocator = locator;
		tag.forknum = fork;
		tag.segno = segment;
		RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true);
		pfree(path);
		if (missing)
			break;
	}
	sync_parent(base.str);
}

static void
validate_relink_entry(const xl_pg_upgrade_relink_entry *entry)
{
	if (entry->entry_type < UPGRADE_RELINK_DIRECTORY ||
		entry->entry_type > UPGRADE_RELINK_FILE ||
		entry->operation < UPGRADE_RELINK_INHERIT ||
		entry->operation > UPGRADE_RELINK_DELETE ||
		entry->key.tablespace_oid == InvalidOid)
		elog(PANIC, "unsupported RELINK filesystem operation");

	switch (entry->entry_type)
	{
		case UPGRADE_RELINK_DIRECTORY:
			if (relink_state.saw_relation ||
				entry->key.filenumber != 0 || entry->fork != 0 ||
				entry->blocks != 0 ||
				(entry->flags & ~UPGRADE_RELINK_INPLACE) != 0 ||
				((entry->flags & UPGRADE_RELINK_INPLACE) != 0 &&
				 (entry->operation == UPGRADE_RELINK_DELETE ||
				  entry->key.tablespace_oid == DEFAULTTABLESPACE_OID ||
				  entry->key.tablespace_oid == GLOBALTABLESPACE_OID)))
				elog(PANIC, "invalid RELINK directory operation");
			relink_state.saw_directory = true;
			break;

		case UPGRADE_RELINK_RELATION:
			if (!relink_state.saw_directory ||
				entry->key.filenumber == 0 ||
				entry->fork != 0 || entry->flags != 0 ||
				entry->blocks != 0)
				elog(PANIC, "invalid RELINK relation operation");
			relink_state.saw_relation = true;
			relink_state.relation = *entry;
			MemSet(relink_state.rebuilt_forks, 0,
				   sizeof(relink_state.rebuilt_forks));
			MemSet(relink_state.inherited_forks, 0,
				   sizeof(relink_state.inherited_forks));
			MemSet(relink_state.short_forks, 0,
				   sizeof(relink_state.short_forks));
			MemSet(relink_state.next_segment, 0,
				   sizeof(relink_state.next_segment));
			relink_state.saw_inherited_file = false;
			relink_state.last_inherited_fork = -1;
			break;

		case UPGRADE_RELINK_FILE:
			if (!relink_state.saw_relation || entry->fork > MAX_FORKNUM ||
				PgUpgradeCompareKeys(&entry->key,
									 &relink_state.relation.key) != 0 ||
				entry->operation == UPGRADE_RELINK_DELETE)
				elog(PANIC, "invalid RELINK file operation");
			if (entry->operation == UPGRADE_RELINK_INHERIT)
			{
				if (relink_state.relation.operation != UPGRADE_RELINK_INHERIT ||
					upgrade_start.transfer_mode > UPGRADE_RELINK_MODE_SWAP ||
					entry->flags != 0 ||
					entry->blocks > RELSEG_SIZE ||
					(entry->blocks == 0 &&
					 relink_state.next_segment[entry->fork] != 0) ||
					relink_state.rebuilt_forks[entry->fork] ||
					relink_state.short_forks[entry->fork] ||
					entry->fork < relink_state.last_inherited_fork)
					elog(PANIC, "invalid inherited RELINK file");
				relink_state.saw_inherited_file = true;
				relink_state.inherited_forks[entry->fork] = true;
				relink_state.short_forks[entry->fork] =
					entry->blocks < RELSEG_SIZE;
				relink_state.next_segment[entry->fork]++;
				relink_state.last_inherited_fork = entry->fork;
			}
			else
			{
				bool		valid_parent =
					(entry->operation == UPGRADE_RELINK_CREATE &&
					 relink_state.relation.operation == UPGRADE_RELINK_CREATE) ||
					(entry->operation == UPGRADE_RELINK_RECREATE &&
					 (relink_state.relation.operation == UPGRADE_RELINK_RECREATE ||
					  relink_state.relation.operation == UPGRADE_RELINK_INHERIT));

				if (!valid_parent || relink_state.saw_inherited_file ||
					entry->flags != 0 ||
					entry->blocks != 0 ||
					relink_state.rebuilt_forks[entry->fork] ||
					relink_state.inherited_forks[entry->fork])
					elog(PANIC, "invalid recreated RELINK file");
				relink_state.rebuilt_forks[entry->fork] = true;
			}
			break;
	}
}

/* Validate and apply one bounded RELINK record at its WAL position. */
static void
ReplayUpgradeRelink(XLogReaderState *record)
{
	Size		length = XLogRecGetDataLen(record);
	const char *data = XLogRecGetData(record);
	uint32		flags;
	int			count;
	xl_pg_upgrade_key *deletes;
	int			ndeletes = 0;
	char		parent[MAXPGPATH] = {0};

	if (!upgrade_active || length < SizeOfPgUpgradeRelink ||
		(length - SizeOfPgUpgradeRelink) % SizeOfPgUpgradeRelinkEntry != 0)
		elog(PANIC, "invalid upgrade RELINK length");
	memcpy(&flags, data, sizeof(flags));
	count = (length - SizeOfPgUpgradeRelink) / SizeOfPgUpgradeRelinkEntry;
	if (count > UPGRADE_RELINK_MAX_ENTRIES ||
		(flags & ~(UPGRADE_RELINK_BEGIN | UPGRADE_RELINK_END)) != 0 ||
		((flags & UPGRADE_RELINK_BEGIN) != 0) == relink_state.active)
		elog(PANIC, "invalid upgrade RELINK batch");
	if (flags & UPGRADE_RELINK_BEGIN)
	{
		MemSet(&relink_state, 0, sizeof(relink_state));
		relink_state.active = true;
		relink_state.last_inherited_fork = -1;
	}
	deletes = palloc_array(xl_pg_upgrade_key, count);
	for (int i = 0; i < count; i++)
	{
		xl_pg_upgrade_relink_entry entry;

		memcpy(&entry, data + SizeOfPgUpgradeRelink + i * sizeof(entry), sizeof(entry));
		validate_relink_entry(&entry);
	}
	if ((flags & UPGRADE_RELINK_END) != 0 && !relink_state.saw_directory)
		elog(PANIC, "incomplete upgrade RELINK scope");
	if (flags & UPGRADE_RELINK_END)
		relink_state.active = false;

	/* Prepare destination tablespaces before removing stale directories. */
	for (int i = 0; i < count; i++)
	{
		xl_pg_upgrade_relink_entry entry;

		memcpy(&entry, data + SizeOfPgUpgradeRelink + i * sizeof(entry), sizeof(entry));
		if (entry.entry_type != UPGRADE_RELINK_DIRECTORY ||
			entry.operation == UPGRADE_RELINK_DELETE)
			continue;
		prepare_tablespace(&entry);
		check_storage_root(entry.key.tablespace_oid, false);
		if (source_datadir != NULL && entry.operation != UPGRADE_RELINK_CREATE)
			check_storage_root(entry.key.tablespace_oid, true);
	}

	for (int i = 0; i < count; i++)
	{
		xl_pg_upgrade_relink_entry entry;

		memcpy(&entry, data + SizeOfPgUpgradeRelink + i * sizeof(entry), sizeof(entry));
		if (entry.entry_type != UPGRADE_RELINK_DIRECTORY &&
			entry.entry_type != UPGRADE_RELINK_RELATION)
			continue;
		if (entry.operation == UPGRADE_RELINK_DELETE ||
			entry.operation == UPGRADE_RELINK_CREATE ||
			entry.operation == UPGRADE_RELINK_RECREATE)
			deletes[ndeletes++] = entry.key;
	}
	qsort(deletes, ndeletes, sizeof(*deletes), PgUpgradeCompareKeys);
	for (int first = 0; first < ndeletes;)
	{
		int			end = first + 1;

		while (end < ndeletes &&
			   deletes[end].tablespace_oid == deletes[first].tablespace_oid &&
			   deletes[end].database_oid == deletes[first].database_oid)
			end++;
		ReplayUpgradeDeleteRoot(deletes + first, end - first);
		first = end;
	}
	pfree(deletes);

	for (int i = 0; i < count; i++)
	{
		xl_pg_upgrade_relink_entry entry;
		char		oldfile[MAXPGPATH];
		char	   *newfile;
		RelPathStr	base;
		uint8		mode;
		uint32		segment;

		memcpy(&entry, data + SizeOfPgUpgradeRelink + i * sizeof(entry), sizeof(entry));
		if (entry.entry_type == UPGRADE_RELINK_RELATION)
		{
			MemSet(relink_state.place_segment, 0,
				   sizeof(relink_state.place_segment));
			continue;
		}
		if (entry.entry_type == UPGRADE_RELINK_DIRECTORY)
		{
			if (entry.operation != UPGRADE_RELINK_DELETE)
			{
				newfile = GetDatabasePath(entry.key.database_oid,
										  entry.key.tablespace_oid);
				make_directory(newfile);
				pfree(newfile);
			}
			continue;
		}
		if (entry.operation == UPGRADE_RELINK_CREATE ||
			entry.operation == UPGRADE_RELINK_RECREATE)
		{
			PrepareUpgradeFileRecreate(&entry);
			continue;
		}
		Assert(entry.entry_type == UPGRADE_RELINK_FILE &&
			   entry.operation == UPGRADE_RELINK_INHERIT);
		segment = relink_state.place_segment[entry.fork]++;
		inherited_filename(&entry.key, entry.fork, segment, oldfile);
		if (!check_inherited_file(oldfile, &entry))
			continue;
		base = GetRelationPath(entry.key.database_oid,
							   entry.key.tablespace_oid,
							   entry.key.filenumber, INVALID_PROC_NUMBER,
							   entry.fork);
		newfile = segment == 0 ? psprintf("%s/%s", DataDir, base.str) :
			psprintf("%s/%s.%u", DataDir, base.str, segment);
		if (strcmp(oldfile, newfile) != 0)
		{
			mode = pg_upgrade_standby_transfer_mode == PG_UPGRADE_XFER_MIRROR ?
				upgrade_start.transfer_mode : pg_upgrade_standby_transfer_mode;
			if (mode > UPGRADE_RELINK_MODE_SWAP)
				elog(PANIC, "invalid filesystem RELINK transfer mode");
			if (unlink(newfile) != 0 && errno != ENOENT)
				ereport(PANIC, (errcode_for_file_access(),
								errmsg("could not remove RELINK destination \"%s\": %m", newfile)));
			PlaceInheritedFile(oldfile, newfile, mode);
			if (mode == UPGRADE_RELINK_MODE_SWAP)
			{
				sync_parent(newfile);
				sync_parent(oldfile);
			}
			else
			{
				get_parent_directory(newfile);
				if (strcmp(parent, newfile) != 0)
				{
					if (parent[0] != '\0')
						fsync_fname(parent, true);
					strlcpy(parent, newfile, sizeof(parent));
				}
			}
		}
		pfree(newfile);
	}
	if (parent[0] != '\0')
		fsync_fname(parent, true);
}

/* Accept COMPLETE only when the same transaction reaches COMMIT. */
void
PgUpgradeReplayCommit(XLogReaderState *record)
{
	uint8		rmid = XLogRecGetRmid(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (!upgrade_active)
		return;
	if (complete_pending && rmid == RM_PG_UPGRADE_ID)
		elog(PANIC, "upgrade record follows COMPLETE");
	if (XLogRecGetXid(record) != window_xid)
		return;
	if (rmid == RM_XACT_ID && (info & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT)
		elog(PANIC, "upgrade transaction aborted");
	if (!complete_pending)
		return;
	if (rmid != RM_XACT_ID || (info & XLOG_XACT_OPMASK) != XLOG_XACT_COMMIT ||
		XLogRecGetDataLen(record) < MinSizeOfXactCommit)
		elog(PANIC, "upgrade COMPLETE has no matching transaction commit");
	PgUpgradeReplayComplete(record);
	MemoryContextDelete(replay_context);
	upgrade_active = complete_pending = source_inplace = source_at_boundary = false;
	MemSet(&relink_state, 0, sizeof(relink_state));
	storage_roots = NULL;
	replay_context = NULL;
	source_datadir = NULL;
	window_xid = InvalidTransactionId;
}

/*
 * Replay upgrade records into the local data directory.  During upgrade
 * recovery, START persists the upgrade state.  The transaction COMMIT after
 * COMPLETE leaves finalization pending until the next shutdown checkpoint
 * becomes a durable restartpoint.
 */
void
pg_upgrade_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	xl_pg_upgrade_marker marker;
	const char *marker_error = NULL;

	if (upgrade_active && info != XLOG_UPGRADE_HANDOFF &&
		XLogRecGetXid(record) != window_xid)
		elog(PANIC, "upgrade record has no matching START");
	if (relink_state.active && info != XLOG_UPGRADE_RELINK)
		elog(PANIC, "record interrupts an upgrade RELINK scope");

	if (info == XLOG_UPGRADE_START)
	{
		xl_pg_upgrade_marker *xlrec = &marker;
		int			fd;
		int			len;

		if (!PgUpgradeReadMarker(info, XLogRecGetData(record),
								 XLogRecGetDataLen(record), &marker,
								 &marker_error))
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid pg_upgrade start record: %s", marker_error)));

		len = strnlen(xlrec->pg_version, sizeof(xlrec->pg_version));

		if (!upgrade_replay_selected)
		{
			if (StandbyMode)
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("reached pg_upgrade boundary on standby; halting "
								"to apply the upgrade"),
						 errdetail("A --wal-upgrade was performed on the primary; "
								   "the standby cannot apply it while streaming."),
						 errhint("Install the new-version binaries and restart "
								 "this standby; it will replay the upgrade from the "
								 "end-of-upgrade checkpoint.")));
			else
				ereport(FATAL,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("pg_upgrade WAL encountered during replay"),
						 errhint("Restart this server to apply the pg_upgrade; "
								 "the upgrade cannot be replayed on a running standby.")));
		}

		if (upgrade_active)
			elog(PANIC, "duplicate upgrade START");
		memcpy(&upgrade_start, XLogRecGetData(record), SizeOfPgUpgradeStart);
		window_xid = XLogRecGetXid(record);
		if (!TransactionIdIsNormal(window_xid))
			elog(PANIC, "upgrade START requires an emitting transaction");
		replay_context = AllocSetContextCreate(TopMemoryContext, "WAL upgrade replay",
											   ALLOCSET_DEFAULT_SIZES);
		MemSet(&relink_state, 0, sizeof(relink_state));
		bind_window();
		upgrade_active = true;
		pgUpgradeReplayInProgress = true;

		SetControlFileUpgradeStarted();

		fd = OpenTransientFile("PG_VERSION",
							   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
		if (fd < 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not open PG_VERSION: %m")));
		if (pg_pwrite(fd, xlrec->pg_version, len, 0) != len)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write PG_VERSION: %m")));
		if (pg_fsync(fd) != 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not fsync PG_VERSION: %m")));
		CloseTransientFile(fd);
	}
	else if (info == XLOG_UPGRADE_COMPLETE)
	{
		if (!upgrade_active || relink_state.active ||
			XLogRecGetDataLen(record) != SizeOfPgUpgradeMarker ||
			memcmp(XLogRecGetData(record), &upgrade_start.marker, SizeOfPgUpgradeMarker) != 0)
			elog(PANIC, "invalid upgrade COMPLETE");
		complete_pending = true;
	}
	else if (info == XLOG_UPGRADE_RELINK)
		ReplayUpgradeRelink(record);
	else if (info == XLOG_UPGRADE_HANDOFF)
	{
		xl_pg_upgrade_handoff *xlrec =
			(xl_pg_upgrade_handoff *) XLogRecGetData(record);

		if (XLogRecGetDataLen(record) != SizeOfPgUpgradeHandoff)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid pg_upgrade handoff record length")));
		if (xlrec->old_major_version != PG_VERSION_NUM / 10000)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("pg_upgrade handoff record has old major version %u, expected %u",
							xlrec->old_major_version,
							PG_VERSION_NUM / 10000)));

		if (StandbyMode)
		{
			BeginPgUpgradeHandoff(record->ReadRecPtr);
			handoff_checkpoint_pending = true;
			handoff_checkpoint_replayed = false;
			handoff_target_major = xlrec->target_major_version;
			ereport(LOG,
					(errmsg("reached pg_upgrade handoff on standby; waiting for "
							"the final shutdown checkpoint")));
		}
	}
	else if (info == XLOG_UPGRADE_RAWFILE)
	{
		/*
		 * Write this chunk at its recorded offset. Offset zero truncates the
		 * destination before the first chunk.
		 */
		xl_pg_upgrade_rawfile *xlrec =
			(xl_pg_upgrade_rawfile *) XLogRecGetData(record);
		char	   *payload = (char *) xlrec + SizeOfPgUpgradeRawFile;
		char		path[MAXPGPATH];
		char	   *data;
		int			fd;
		char	   *slash;

		if (XLogRecGetDataLen(record) < SizeOfPgUpgradeRawFile)
			elog(PANIC, "truncated rawfile record");
		if (xlrec->path_len >= MAXPGPATH)
			elog(PANIC, "rawfile path too long");
		if ((uint64) SizeOfPgUpgradeRawFile + xlrec->path_len + xlrec->data_len >
			XLogRecGetDataLen(record))
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("rawfile record overruns the record")));
		data = payload + xlrec->path_len;
		memcpy(path, payload, xlrec->path_len);
		path[xlrec->path_len] = '\0';
		if (strlen(path) != xlrec->path_len)
			elog(PANIC, "embedded NUL in upgrade rawfile path");

		/* Adopt control settings without replacing local recovery progress. */
		if (xlrec->path_len == strlen(XLOG_CONTROL_FILE) &&
			memcmp(payload, XLOG_CONTROL_FILE, xlrec->path_len) == 0)
		{
			if (xlrec->offset != 0 ||
				xlrec->data_len != PG_CONTROL_FILE_SIZE ||
				(uint64) SizeOfPgUpgradeRawFile + xlrec->path_len +
				xlrec->data_len != XLogRecGetDataLen(record))
				ereport(PANIC,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("invalid pg_control image in pg_upgrade WAL")));

			AdoptUpgradeControlFile(data, xlrec->data_len);
			return;
		}

		if (!PgUpgradeDirectoryPathIsSafe(path))
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("unsafe rawfile path \"%s\"", path)));

		slash = strrchr(path, '/');
		if (slash != NULL)
		{
			*slash = '\0';
			if (mkdir(path, pg_dir_create_mode) != 0 && errno != EEXIST)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not create directory \"%s\": %m",
								path)));
			*slash = '/';
		}

		fd = OpenTransientFile(path, O_WRONLY | O_CREAT | PG_BINARY |
							   (xlrec->offset == 0 ? O_TRUNC : 0));
		if (fd < 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not open \"%s\": %m", path)));
		if (pg_pwrite(fd, data, xlrec->data_len, (off_t) xlrec->offset) !=
			(ssize_t) xlrec->data_len)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write \"%s\": %m", path)));
		if (pg_fsync(fd) != 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not fsync \"%s\": %m", path)));
		CloseTransientFile(fd);
		/* Fsync the containing directory after writing the first chunk. */
		if (xlrec->offset == 0)
		{
			if (slash != NULL)
				*slash = '\0';
			fsync_fname(slash != NULL ? path : ".", true);
		}
	}
	else
		elog(PANIC, "unknown op code %u", info);
}

/* Record committed completion and mark its shutdown restartpoint pending. */
static void
PgUpgradeReplayComplete(XLogReaderState *record)
{
	TimeLineID	replay_tli;

	if (upgrade_complete_checkpoint_pending)
		elog(PANIC, "duplicate pg_upgrade COMPLETE before its checkpoint");
	(void) GetCurrentReplayRecPtr(&replay_tli);
	SetControlFileUpgradeComplete(record->EndRecPtr, replay_tli);
	upgrade_complete_end_lsn = record->EndRecPtr;
	upgrade_complete_checkpoint_pending = true;
	upgrade_complete_checkpoint_replayed = false;
}

/*
 * Remember the pending HANDOFF or completion shutdown checkpoint for
 * restartpoint verification.
 */
void
PgUpgradeCheckpointReplayed(const CheckPoint *checkpoint,
							XLogReaderState *record)
{
	if (handoff_checkpoint_pending)
	{
		XLogRecPtr	handoff_lsn = GetPgUpgradeHandoffRetention();

		if (XLogRecGetPrev(record) != handoff_lsn)
		{
			ereport(LOG,
					(errmsg("shutdown checkpoint supersedes an incomplete pg_upgrade handoff")));
			handoff_checkpoint_pending = false;
			handoff_checkpoint_replayed = false;
			handoff_checkpoint_lsn = InvalidXLogRecPtr;
			handoff_checkpoint_tli = 0;
			handoff_target_major = 0;
			CancelPgUpgradeHandoff();
			return;
		}
		if (checkpoint->redo != record->ReadRecPtr)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("shutdown checkpoint after pg_upgrade handoff has an "
							"invalid REDO location")));

		handoff_checkpoint_lsn = record->ReadRecPtr;
		handoff_checkpoint_tli = checkpoint->ThisTimeLineID;
		handoff_checkpoint_replayed = true;
	}

	if (upgrade_complete_checkpoint_pending)
	{
		if (checkpoint->redo != record->ReadRecPtr ||
			record->ReadRecPtr < upgrade_complete_end_lsn)
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("shutdown checkpoint after pg_upgrade COMPLETE is invalid")));

		upgrade_complete_checkpoint_lsn = record->ReadRecPtr;
		upgrade_complete_checkpoint_tli = checkpoint->ThisTimeLineID;
		upgrade_complete_checkpoint_replayed = true;
	}
}

/*
 * Persist and verify a restartpoint for the replayed shutdown checkpoint.
 * HANDOFF then persists each physical slot's receipt LSN and either cancels on
 * a resume request or enters durable pause. COMPLETE removes pg_upgrade.signal
 * and marks replay finalized.
 */
void
PgUpgradeCheckpointApplied(void)
{
	if (!handoff_checkpoint_replayed &&
		!upgrade_complete_checkpoint_replayed)
		return;

	RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_FAST | CHECKPOINT_WAIT);

	if (handoff_checkpoint_replayed)
	{
		VerifyUpgradeRestartPoint(handoff_checkpoint_lsn,
								  handoff_checkpoint_tli);
		handoff_checkpoint_pending = false;
		handoff_checkpoint_replayed = false;

		if (!HotStandbyActive())
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("reached the final pg_upgrade checkpoint before hot standby was active"),
					 errdetail("The primary is upgrading to major version %u.",
							   handoff_target_major)));

		if (!WaitForPgUpgradeHandoffSlots(GetXLogReplayRecPtr(NULL)))
		{
			CancelPgUpgradeHandoff();
			handoff_checkpoint_lsn = InvalidXLogRecPtr;
			handoff_checkpoint_tli = 0;
			handoff_target_major = 0;
			ereport(LOG,
					(errmsg("cancelled the pg_upgrade handoff at operator request")));
			return;
		}
		CompletePgUpgradeHandoff();
		ereport(LOG,
				(errmsg("reached the final old-major shutdown checkpoint; "
						"pausing recovery for pg_upgrade"),
				 errhint("Provision the new-version standby from this retained "
						 "data directory, or resume recovery to roll back the "
						 "upgrade attempt.")));
	}

	if (upgrade_complete_checkpoint_replayed)
	{
		VerifyUpgradeRestartPoint(upgrade_complete_checkpoint_lsn,
								  upgrade_complete_checkpoint_tli);

		/*
		 * Persist removal of pg_upgrade.signal before marking replay
		 * finalized.
		 */
		durable_unlink(PG_UPGRADE_SIGNAL_FILE, PANIC);
		SetControlFileUpgradeFinalized();
		upgrade_complete_checkpoint_pending = false;
		upgrade_complete_checkpoint_replayed = false;
		pgUpgradeReplayInProgress = false;
		ereport(LOG,
				(errmsg("persisted the post-upgrade restartpoint; pg_upgrade replay is complete")));
	}
}
