/*-------------------------------------------------------------------------
 *
 * pg_control.h
 *	  The system control file "pg_control" is not a heap relation.
 *	  However, we define it here so that the format is documented.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_control.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CONTROL_H
#define PG_CONTROL_H

#include "access/transam.h"
#include "access/xlogdefs.h"
#include "common/relpath.h"		/* for RelFileNumber */
#include "pgtime.h"				/* for pg_time_t */
#include "port/pg_crc32c.h"


/* Version identifier for this pg_control format */
#define PG_CONTROL_VERSION	1903

/* Nonce key length, see below */
#define MOCK_AUTH_NONCE_LEN		32

/*
 * Body of CheckPoint XLOG records.  This is declared here because we keep
 * a copy of the latest one in pg_control for possible disaster recovery.
 * Changing this struct requires a PG_CONTROL_VERSION bump.
 */
typedef struct CheckPoint
{
	XLogRecPtr	redo;			/* next RecPtr available when we began to
								 * create CheckPoint (i.e. REDO start point) */
	TimeLineID	ThisTimeLineID; /* current TLI */
	TimeLineID	PrevTimeLineID; /* previous TLI, if this record begins a new
								 * timeline (equals ThisTimeLineID otherwise) */
	bool		fullPageWrites; /* current full_page_writes */
	int			wal_level;		/* current wal_level */
	bool		logicalDecodingEnabled; /* current logical decoding status */
	FullTransactionId nextXid;	/* next free transaction ID */
	Oid			nextOid;		/* next free OID */
	MultiXactId nextMulti;		/* next free MultiXactId */
	MultiXactOffset nextMultiOffset;	/* next free MultiXact offset */
	TransactionId oldestXid;	/* cluster-wide minimum datfrozenxid */
	Oid			oldestXidDB;	/* database with minimum datfrozenxid */
	MultiXactId oldestMulti;	/* cluster-wide minimum datminmxid */
	Oid			oldestMultiDB;	/* database with minimum datminmxid */
	pg_time_t	time;			/* time stamp of checkpoint */
	TransactionId oldestCommitTsXid;	/* oldest Xid with valid commit
										 * timestamp */
	TransactionId newestCommitTsXid;	/* newest Xid with valid commit
										 * timestamp */

	/*
	 * Oldest XID still running. This is only needed to initialize hot standby
	 * mode from an online checkpoint, so we only bother calculating this for
	 * online checkpoints and only when wal_level is replica. Otherwise it's
	 * set to InvalidTransactionId.
	 */
	TransactionId oldestActiveXid;

	/* data checksums state at the time of the checkpoint  */
	uint32		dataChecksumState;
} CheckPoint;

/* XLOG info values for XLOG rmgr */
#define XLOG_CHECKPOINT_SHUTDOWN		0x00
#define XLOG_CHECKPOINT_ONLINE			0x10
#define XLOG_NOOP						0x20
#define XLOG_NEXTOID					0x30
#define XLOG_SWITCH						0x40
#define XLOG_BACKUP_END					0x50
#define XLOG_PARAMETER_CHANGE			0x60
#define XLOG_RESTORE_POINT				0x70
#define XLOG_FPW_CHANGE					0x80
#define XLOG_END_OF_RECOVERY			0x90
#define XLOG_FPI_FOR_HINT				0xA0
#define XLOG_FPI						0xB0
#define XLOG_ASSIGN_LSN					0xC0
#define XLOG_OVERWRITE_CONTRECORD		0xD0

/*
 * LEE: pg_upgrade WAL record types in RM_PG_UPGRADE_ID.
 * Using a dedicated rmgr gives us a clean 0x10-aligned namespace free from
 * the XLR_RMGR_INFO_MASK = 0xF0 constraint that limits RM_XLOG_ID to one
 * record type per 0x10 bucket.
 */
#define XLOG_UPGRADE_START			0x00	/* upgrade window start marker */
#define XLOG_UPGRADE_COMPLETE		0x10	/* upgrade window complete marker */
#define XLOG_UPGRADE_SLRU_DATA			0x20	/* bulk SLRU segment image */
#define XLOG_UPGRADE_RELFILE_DATA		0x30	/* bulk relation file segment image */
#define XLOG_UPGRADE_RAWFILE			0x50	/* verbatim non-relation file image
												 * (pg_filenode.map, PG_VERSION) */
#define XLOG_UPGRADE_DIRTREE			0x40	/* logged after-image of the initdb
												 * directory tree */
#define XLOG_UPGRADE_HANDOFF			0x60	/* OLD-format streaming-handoff
												 * trigger, emitted in the OLD
												 * cluster's own WAL just before
												 * pg_upgrade shuts it down */
#define XLOG_UPGRADE_DELETE_AUTHORIZE 0x70	/* set-wide "old cluster may now be
												 * deleted" signal, emitted on the
												 * live NEW primary by --delete-old
												 * and replayed by NEW standbys */
#define XLOG_CHECKPOINT_REDO			0xE0
#define XLOG_LOGICAL_DECODING_STATUS_CHANGE	0xF0

/* XLOG info values for XLOG2 rmgr */
#define XLOG2_CHECKSUMS					0x00

/*
 * LEE: WAL record written at the start and completion of pg_upgrade.
 * Allows crash recovery and tooling (pg_waldump) to identify the upgrade
 * window and verify atomicity.
 *
 * pg_version[] carries the new cluster's PG_MAJORVERSION string (e.g. "18")
 * so that redo can write $PGDATA/PG_VERSION, which initdb created outside
 * the server and is therefore not otherwise WAL-logged.
 */
typedef struct xl_pg_upgrade
{
	uint32		old_major_version;	/* old cluster PG_VERSION_NUM major */
	uint32		new_major_version;	/* new cluster PG_VERSION_NUM major */
	pg_time_t	upgrade_time;		/* wall-clock time of this record */
	char		pg_version[8];		/* new cluster PG_MAJORVERSION, e.g. "18\n" */
} xl_pg_upgrade;

#define SizeOfXLPgUpgrade	sizeof(xl_pg_upgrade)

/*
 * LEE: XLOG_UPGRADE_HANDOFF — the streaming-standby handoff TRIGGER.
 *
 * Unlike every other pg_upgrade WAL record (which is written by the NEW cluster
 * in the NEW WAL page format and is only readable by the new binary), this
 * record is emitted into the OLD cluster's OWN WAL stream, in the OLD format,
 * just before pg_upgrade shuts the old primary down.  Because it is chained onto
 * the old stream in the old page format, a physical standby still streaming the
 * old primary READS it normally -- which the new-format upgrade burst can never
 * be (see pgupgrade_wal.c: the major-version WAL page magic differs, so a v18
 * standby cannot read v20 WAL and vice versa).
 *
 * It carries NO upgrade data.  Its only job is to be a control signal: when a
 * StandbyMode server replays it, the standby stops cleanly at this LSN and
 * reports that an upgrade handoff is beginning, so the operator/automation can
 * swap to the new-version binary/host and re-provision from the delivered
 * new-version upgrade window (which is replayed from CN, out of band).  It is a
 * TRIGGER, not a TRANSPORT.
 *
 * target_major_version is informational (for the log message); the standby does
 * not act on it beyond reporting.
 */
typedef struct xl_pg_upgrade_handoff
{
	uint32		old_major_version;	/* this (old) cluster's major version */
	uint32		target_major_version;	/* major version being upgraded to */
	pg_time_t	handoff_time;		/* wall-clock time of this record */
} xl_pg_upgrade_handoff;

#define SizeOfXLPgUpgradeHandoff	sizeof(xl_pg_upgrade_handoff)

/*
 * LEE: XLOG_UPGRADE_DELETE_AUTHORIZE — set-wide "the old cluster may now be
 * deleted" signal.
 *
 * Emitted on the LIVE, committed NEW primary by "pg_upgrade --delete-old" (in the
 * NEW WAL format, so NEW-version streaming standbys read it).  It carries NO
 * data; it is a control signal.  When a NEW standby replays it, the standby marks
 * its OWN superseded old cluster as "delete authorized" -- it does NOT rm
 * anything from the redo handler (an irreversible rm in WAL replay would also
 * re-run on crash recovery, and the primary does not know the standby's
 * host-specific old-dir path).  The physical removal stays a local
 * "pg_upgrade --delete-old" on the standby, which the authorization unblocks.
 *
 * A standby honors it only if its own old dir is already superseded (a commit ran
 * there), so a stray/replayed signal can never remove a still-live cluster.
 */
typedef struct xl_pg_upgrade_delete_authorize
{
	uint32		new_major_version;	/* the committed new cluster's major version */
	pg_time_t	authorize_time;		/* wall-clock time of this record */
} xl_pg_upgrade_delete_authorize;

#define SizeOfXLPgUpgradeDeleteAuthorize	sizeof(xl_pg_upgrade_delete_authorize)

/*
 * LEE: XLOG_UPGRADE_SLRU_DATA — full page image of one SLRU segment, emitted
 * by pg_upgrade before stop_postmaster() so SLRU content is WAL-replayable.
 *
 * slru_type identifies which SLRU directory the segment belongs to:
 *   0 = pg_xact            (commit status)
 *   1 = pg_multixact/offsets
 *   2 = pg_multixact/members
 *
 * The record payload is this header followed by npages * BLCKSZ bytes of raw
 * page data for the segment.  npages <= SLRU_PAGES_PER_SEGMENT (32).
 * One record is emitted per segment file.
 */
typedef struct xl_upgrade_slru_data
{
	uint8		slru_type;		/* which SLRU: 0=pg_xact, 1=mxoff, 2=mxmem */
	int64		first_seg;		/* first segment file number in this record */
	int64		last_seg;		/* last segment file number in this record */
	uint32		total_bytes;	/* total bytes of raw SLRU data that follow */
	/* followed by total_bytes of raw segment file data (consecutive segments) */
} xl_upgrade_slru_data;

#define SizeOfXLUpgradeSlruData		offsetof(xl_upgrade_slru_data, total_bytes) + sizeof(uint32)

/* slru_type values for xl_upgrade_slru_data */
#define UPGRADE_SLRU_XACT		0
#define UPGRADE_SLRU_MXOFF		1
#define UPGRADE_SLRU_MXMEM		2

/* directory paths corresponding to the slru_type values above */
#define UPGRADE_SLRU_DIRS		{ "pg_xact", "pg_multixact/offsets", "pg_multixact/members" }

/*
 * LEE: XLOG_UPGRADE_RELFILE_DATA — full page images of relation files.
 *
 * To use WAL records at the coarsest granularity, one record BATCHES many
 * relation-file chunks packed up to the 1020MB max WAL payload.  The record
 * payload is a sequence of entries, each being an xl_upgrade_relfile_entry
 * header immediately followed by its nbytes of raw page data:
 *
 *     [entry_0][data_0][entry_1][data_1] ... [entry_k][data_k]
 *
 * A relation-file segment larger than the payload cap is split into several
 * page-aligned chunks (see blockoff), each its own entry, possibly across
 * several records.  Redo walks the entries until the record data is consumed.
 *
 * ForkNumber: 0=main, 1=FSM, 2=VM, 3=init
 */
typedef struct xl_upgrade_relfile_entry
{
	Oid			tablespace_oid;		/* tablespace containing the file */
	Oid			database_oid;		/* database OID (0 for shared relations) */
	RelFileNumber relfilenumber;	/* relation file number */
	uint8		forknum;			/* fork: 0=main, 1=FSM, 2=VM, 3=init */
	uint32		segno;				/* 1GB segment number (0 = base segment) */
	uint32		blockoff;			/* first block within the segment for this
									 * chunk */
	uint32		nbytes;				/* bytes of raw page data that follow */
	/* followed by nbytes bytes of raw file data for this chunk */
} xl_upgrade_relfile_entry;

#define SizeOfXLUpgradeRelfileEntry		sizeof(xl_upgrade_relfile_entry)

/*
 * LEE: XLOG_UPGRADE_RAWFILE — verbatim image of a non-relation file that is
 * not reachable through the buffer manager, so that the cluster can be rebuilt
 * from an empty data directory (only the folder skeleton + pg_control need to
 * pre-exist).  Currently used for pg_filenode.map (the relation-map that points
 * catalog OIDs at their relfilenodes) and PG_VERSION files.
 *
 * Redo creates any missing parent directories and writes the file verbatim.
 * The record payload is this header, then path_len bytes of the PGDATA-relative
 * path (no trailing NUL), then data_len bytes of file contents.
 */
typedef struct xl_upgrade_rawfile
{
	uint32		path_len;		/* length of the PGDATA-relative path */
	uint32		data_len;		/* length of the file contents that follow */
	/* followed by path_len bytes of path, then data_len bytes of contents */
} xl_upgrade_rawfile;

#define SizeOfXLUpgradeRawfile	offsetof(xl_upgrade_rawfile, data_len) + sizeof(uint32)

/*
 * LEE: XLOG_UPGRADE_DIRTREE — the logged "after-image" of the new cluster's
 * directory tree as it exists once pg_upgrade --initdb has finished all its
 * work (schema restore, file transfer, counter transplant).  initdb creates
 * this tree outside the server, so it is not otherwise WAL-logged; capturing it
 * as one record lets recovery rebuild the entire directory skeleton from WAL
 * alone -- without relying on a surviving on-disk skeleton, and (for a standby)
 * without the standby ever running initdb.
 *
 * Redo mkdir()s each path in order, idempotently (EEXIST is fine): on the
 * primary's crash recovery the dirs were wiped and must be recreated; on a
 * standby they largely already exist (it is a physical copy of the old cluster,
 * whose DB OIDs pg_upgrade preserves) so most creations are no-ops.
 *
 * The payload is this header followed by:
 *   1. ndirs NUL-terminated PGDATA-relative directory paths (dir_bytes long),
 *      emitted parent-before-child so a plain mkdir() suffices; then
 *   2. nsymlinks symlink entries (sym_bytes long), each two NUL-terminated
 *      strings: the PGDATA-relative link path, then its target.  These capture
 *      user-tablespace symlinks (pg_tblspc/<spcoid> -> external location) so a
 *      fresh target / standby recreates them before tablespace RELFILE images
 *      replay -- without this, external-location tablespaces are unrecoverable.
 *
 * Redo mkdir()s each directory, then symlink()s each entry (creating the target
 * directory too), all idempotently (EEXIST is fine).
 */
typedef struct xl_upgrade_dirtree
{
	uint32		ndirs;			/* number of directory paths */
	uint32		dir_bytes;		/* total bytes of directory-path data */
	uint32		nsymlinks;		/* number of symlink entries */
	uint32		sym_bytes;		/* total bytes of symlink-entry data */
	/* followed by dir_bytes of dir paths, then sym_bytes of symlink entries */
} xl_upgrade_dirtree;

#define SizeOfXLUpgradeDirtree	(offsetof(xl_upgrade_dirtree, sym_bytes) + sizeof(uint32))


/*
 * System status indicator.  Note this is stored in pg_control; if you change
 * it, you must bump PG_CONTROL_VERSION
 */
typedef enum DBState
{
	DB_STARTUP = 0,
	DB_SHUTDOWNED,
	DB_SHUTDOWNED_IN_RECOVERY,
	DB_SHUTDOWNING,
	DB_IN_CRASH_RECOVERY,
	DB_IN_ARCHIVE_RECOVERY,
	DB_IN_PRODUCTION,

	/*
	 * INFORMATIONAL: a --wal-upgrade cluster that is CURRENTLY replaying its
	 * upgrade window (between XLOG_UPGRADE_START and _COMPLETE).  The arm still
	 * uses DB_IN_PRODUCTION to trigger crash recovery; the redo path flips to this
	 * state while the window is being applied and back to DB_IN_PRODUCTION when it
	 * finishes, purely so pg_controldata / diagnostics show "in pg_upgrade" rather
	 * than the misleading "in production" for a half-reconstructed cluster.  It is
	 * NOT a recovery-mode trigger.  Appended last to keep on-disk values stable.
	 */
	DB_IN_UPGRADE,
} DBState;

/*
 * Contents of pg_control.
 */

typedef struct ControlFileData
{
	/*
	 * Unique system identifier --- to ensure we match up xlog files with the
	 * installation that produced them.
	 */
	uint64		system_identifier;

	/*
	 * Version identifier information.  Keep these fields at the same offset,
	 * especially pg_control_version; they won't be real useful if they move
	 * around.  (For historical reasons they must be 8 bytes into the file
	 * rather than immediately at the front.)
	 *
	 * pg_control_version identifies the format of pg_control itself.
	 * catalog_version_no identifies the format of the system catalogs.
	 *
	 * There are additional version identifiers in individual files; for
	 * example, WAL logs contain per-page magic numbers that can serve as
	 * version cues for the WAL log.
	 */
	uint32		pg_control_version; /* PG_CONTROL_VERSION */
	uint32		catalog_version_no; /* see catversion.h */

	/*
	 * System status data
	 */
	DBState		state;			/* see enum above */
	pg_time_t	time;			/* time stamp of last pg_control update */
	XLogRecPtr	checkPoint;		/* last check point record ptr */

	CheckPoint	checkPointCopy; /* copy of last check point record */

	XLogRecPtr	unloggedLSN;	/* current fake LSN value, for unlogged rels */

	/*
	 * These two values determine the minimum point we must recover up to
	 * before starting up:
	 *
	 * minRecoveryPoint is updated to the latest replayed LSN whenever we
	 * flush a data change during archive recovery. That guards against
	 * starting archive recovery, aborting it, and restarting with an earlier
	 * stop location. If we've already flushed data changes from WAL record X
	 * to disk, we mustn't start up until we reach X again. Zero when not
	 * doing archive recovery.
	 *
	 * backupStartPoint is the redo pointer of the backup start checkpoint, if
	 * we are recovering from an online backup and haven't reached the end of
	 * backup yet. It is reset to zero when the end of backup is reached, and
	 * we mustn't start up before that. A boolean would suffice otherwise, but
	 * we use the redo pointer as a cross-check when we see an end-of-backup
	 * record, to make sure the end-of-backup record corresponds the base
	 * backup we're recovering from.
	 *
	 * backupEndPoint is the backup end location, if we are recovering from an
	 * online backup which was taken from the standby and haven't reached the
	 * end of backup yet. It is initialized to the minimum recovery point in
	 * pg_control which was backed up last. It is reset to zero when the end
	 * of backup is reached, and we mustn't start up before that.
	 *
	 * If backupEndRequired is true, we know for sure that we're restoring
	 * from a backup, and must see a backup-end record before we can safely
	 * start up.
	 */
	XLogRecPtr	minRecoveryPoint;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	backupStartPoint;
	XLogRecPtr	backupEndPoint;
	bool		backupEndRequired;

	/*
	 * Parameter settings that determine if the WAL can be used for archival
	 * or hot standby.
	 */
	int			wal_level;
	bool		wal_log_hints;
	int			MaxConnections;
	int			max_worker_processes;
	int			max_wal_senders;
	int			max_prepared_xacts;
	int			max_locks_per_xact;
	bool		track_commit_timestamp;

	/*
	 * This data is used to check for hardware-architecture compatibility of
	 * the database and the backend executable.  We need not check endianness
	 * explicitly, since the pg_control version will surely look wrong to a
	 * machine of different endianness, but we do need to worry about MAXALIGN
	 * and floating-point format.  (Note: storage layout nominally also
	 * depends on SHORTALIGN and INTALIGN, but in practice these are the same
	 * on all architectures of interest.)
	 *
	 * Testing just one double value is not a very bulletproof test for
	 * floating-point compatibility, but it will catch most cases.
	 */
	uint32		maxAlign;		/* alignment requirement for tuples */
	double		floatFormat;	/* constant 1234567.0 */
#define FLOATFORMAT_VALUE	1234567.0

	/*
	 * This data is used to make sure that configuration of this database is
	 * compatible with the backend executable.
	 */
	uint32		blcksz;			/* data block size for this DB */
	uint32		relseg_size;	/* blocks per segment of large relation */

	uint32		slru_pages_per_segment; /* size of each SLRU segment */

	uint32		xlog_blcksz;	/* block size within WAL files */
	uint32		xlog_seg_size;	/* size of each WAL segment */

	uint32		nameDataLen;	/* catalog name field width */
	uint32		indexMaxKeys;	/* max number of columns in an index */

	uint32		toast_max_chunk_size;	/* chunk size in TOAST tables */
	uint32		loblksize;		/* chunk size in pg_largeobject */

	bool		float8ByVal;	/* float8, int8, etc pass-by-value? */

	/* Are data pages protected by checksums? Zero if no checksum version */
	uint32		data_checksum_version;

	/*
	 * True if the default signedness of char is "signed" on a platform where
	 * the cluster is initialized.
	 */
	bool		default_char_signedness;

	/*
	 * Random nonce, used in authentication requests that need to proceed
	 * based on values that are cluster-unique, like a SASL exchange that
	 * failed at an early stage.
	 */
	char		mock_authentication_nonce[MOCK_AUTH_NONCE_LEN];

	/* CRC of all above ... MUST BE LAST! */
	pg_crc32c	crc;
} ControlFileData;

/*
 * Maximum safe value of sizeof(ControlFileData).  For reliability's sake,
 * it's critical that pg_control updates be atomic writes.  That generally
 * means the active data can't be more than one disk sector, which is 512
 * bytes on common hardware.  Be very careful about raising this limit.
 */
#define PG_CONTROL_MAX_SAFE_SIZE	512

/*
 * Physical size of the pg_control file.  Note that this is considerably
 * bigger than the actually used size (ie, sizeof(ControlFileData)).
 * The idea is to keep the physical size constant independent of format
 * changes, so that ReadControlFile will deliver a suitable wrong-version
 * message instead of a read error if it's looking at an incompatible file.
 */
#define PG_CONTROL_FILE_SIZE		8192

/*
 * Ensure that the size of the pg_control data structure is sane.
 */
StaticAssertDecl(sizeof(ControlFileData) <= PG_CONTROL_MAX_SAFE_SIZE,
				 "pg_control is too large for atomic disk writes");
StaticAssertDecl(sizeof(ControlFileData) <= PG_CONTROL_FILE_SIZE,
				 "sizeof(ControlFileData) exceeds PG_CONTROL_FILE_SIZE");

#endif							/* PG_CONTROL_H */
