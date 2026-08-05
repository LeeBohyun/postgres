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
#define PG_CONTROL_VERSION	1905

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
 * pg_upgrade WAL record types in RM_PG_UPGRADE_ID.
 * Using a dedicated rmgr gives us a clean 0x10-aligned namespace free from
 * the XLR_RMGR_INFO_MASK = 0xF0 constraint that limits RM_XLOG_ID to one
 * record type per 0x10 bucket.
 */
#define XLOG_UPGRADE_START			0x00	/* upgrade window start marker */
#define XLOG_UPGRADE_COMPLETE		0x10	/* upgrade window complete marker */
/*
 * 0x20 and 0x30 are unused: SLRU segments are captured as
 * XLOG_UPGRADE_RAWFILE, and relation pages as ordinary XLOG_FPI records.
 */
#define XLOG_UPGRADE_RAWFILE			0x50	/* verbatim non-relation file
												 * image (PG_VERSION, SLRU
												 * segments) */
#define XLOG_UPGRADE_DIRTREE			0x40	/* logged after-image of the
												 * initdb directory tree */
#define XLOG_UPGRADE_HANDOFF			0x60	/* OLD-format
												 * streaming-handoff trigger,
												 * emitted in the old
												 * cluster's own WAL just
												 * before pg_upgrade shuts it
												 * down */
#define XLOG_UPGRADE_RELINK				0x70	/* manifest of user relations
												 * to link from the standby's
												 * old datadir (not carried in
												 * the window) */
#define XLOG_CHECKPOINT_REDO			0xE0
#define XLOG_LOGICAL_DECODING_STATUS_CHANGE	0xF0

/* XLOG info values for XLOG2 rmgr */
#define XLOG2_CHECKSUMS					0x00

/*
 * XLOG_UPGRADE_START / XLOG_UPGRADE_COMPLETE -- mark the upgrade window so
 * crash recovery and tooling (pg_waldump) can identify it and verify
 * atomicity.  pg_version[] carries $PGDATA/PG_VERSION, which initdb writes
 * outside the server and is not otherwise WAL-logged.  See pg_upgrade_redo().
 */
typedef struct xl_pg_upgrade
{
	uint32		old_major_version;	/* old cluster PG_VERSION_NUM major */
	uint32		new_major_version;	/* new cluster PG_VERSION_NUM major */
	pg_time_t	upgrade_time;	/* wall-clock time of this record */
	char		pg_version[8];	/* new cluster PG_MAJORVERSION, e.g. "18\n" */
} xl_pg_upgrade;

#define SizeOfXLPgUpgrade	sizeof(xl_pg_upgrade)

/*
 * XLOG_UPGRADE_HANDOFF -- streaming-standby handoff trigger.  Carries no data;
 * it is a control signal.  Unlike the other pg_upgrade records (new-format WAL
 * readable only by the new binary), this is emitted into the old cluster's own
 * WAL in the old format just before shutdown, so a physical standby still
 * streaming the old primary can read it.  On replay a StandbyMode server stops
 * cleanly at this LSN and reports that a handoff is beginning.
 * target_major_version is informational (log message only).
 * See pg_upgrade_redo().
 */
typedef struct xl_pg_upgrade_handoff
{
	uint32		old_major_version;	/* this (old) cluster's major version */
	uint32		target_major_version;	/* major version being upgraded to */
	pg_time_t	handoff_time;	/* wall-clock time of this record */
} xl_pg_upgrade_handoff;

#define SizeOfXLPgUpgradeHandoff	sizeof(xl_pg_upgrade_handoff)

/*
 * SLRU directories captured by the upgrade window.  Their segments are emitted
 * as XLOG_UPGRADE_RAWFILE records like any other non-relation file.
 */
#define UPGRADE_SLRU_DIRS		{ "pg_xact", "pg_multixact/offsets", "pg_multixact/members" }

/*
 * XLOG_UPGRADE_RELINK -- manifest of user relation files that pg_upgrade
 * transferred verbatim (and the window therefore omits, keeping it
 * schema-sized).  Each entry names one file by identity; no file data follows.
 * One record batches many entries: [entry_0][entry_1] ... [entry_k].  Redo is a
 * no-op on the primary and places each file from the standby's retained old
 * datadir on a streaming standby.  See pg_upgrade_redo().
 */
typedef struct xl_upgrade_relink_entry
{
	Oid			tablespace_oid; /* tablespace containing the file */
	Oid			database_oid;	/* database OID (0 for shared relations) */
	RelFileNumber relfilenumber;	/* relation file number */
	uint8		forknum;		/* fork: 0=main, 1=FSM, 2=VM, 3=init */
	uint8		transfer_mode;	/* selects the placement primitive; see
								 * UPGRADE_RELINK_MODE_* below and
								 * relink_place_file() */
	uint32		segno;			/* 1GB segment number (0 = base segment) */
} xl_upgrade_relink_entry;

/*
 * transfer_mode values, mirroring pg_upgrade's transferMode (see
 * src/bin/pg_upgrade/pg_upgrade.h); duplicated here because the record is read
 * by the backend, which does not include pg_upgrade headers.
 */
#define UPGRADE_RELINK_MODE_CLONE			0
#define UPGRADE_RELINK_MODE_COPY			1
#define UPGRADE_RELINK_MODE_COPY_FILE_RANGE	2
#define UPGRADE_RELINK_MODE_LINK			3
#define UPGRADE_RELINK_MODE_SWAP			4

#define SizeOfXLUpgradeRelinkEntry		sizeof(xl_upgrade_relink_entry)

/*
 * XLOG_UPGRADE_RAWFILE -- verbatim image of a non-relation file not reachable
 * through the buffer manager (PG_VERSION, and the SLRU segments under pg_xact/
 * and pg_multixact/), so the cluster can be rebuilt from an empty data
 * directory.  Payload is this header, then path_len bytes of PGDATA-relative
 * path (no trailing NUL), then data_len bytes of contents to be written at
 * "offset" within that file.
 *
 * A file too large for one WAL record is emitted as several records with
 * increasing offsets, so this single record type covers both the small
 * verbatim files and whole SLRU segments.  See pg_upgrade_redo().
 */
typedef struct xl_upgrade_rawfile
{
	uint32		path_len;		/* length of the PGDATA-relative path */
	uint32		data_len;		/* length of the file contents that follow */
	uint64		offset;			/* byte offset in the file to write at */
	/* followed by path_len bytes of path, then data_len bytes of contents */
} xl_upgrade_rawfile;

#define SizeOfXLUpgradeRawfile	(offsetof(xl_upgrade_rawfile, offset) + sizeof(uint64))

/*
 * XLOG_UPGRADE_DIRTREE -- logged after-image of the new cluster's directory
 * tree once pg_upgrade has finished, so recovery can rebuild the skeleton from
 * WAL without a surviving on-disk skeleton and without running initdb.  Payload
 * is this header followed by:
 *   1. ndirs NUL-terminated PGDATA-relative directory paths (dir_bytes long),
 *      emitted parent-before-child; then
 *   2. nsymlinks symlink entries (sym_bytes long), each two NUL-terminated
 *      strings: PGDATA-relative link path, then target.  These capture
 *      user-tablespace symlinks (pg_tblspc/<spcoid> -> external location).
 * See pg_upgrade_redo().
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
	 * A --wal-upgrade cluster replaying its upgrade window (between
	 * XLOG_UPGRADE_START and _COMPLETE).  Informational only, so
	 * pg_controldata shows "in pg_upgrade" for a half-reconstructed cluster;
	 * it does not drive the recovery-mode decision.  Appended last to keep
	 * on-disk values stable.
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
	 * --wal-upgrade: true once the upgrade window has been replayed to
	 * XLOG_UPGRADE_COMPLETE.  Durable "upgrade finalized" signal that lets
	 * first-startup tell a fully-upgraded cluster from a crashed partial one.
	 * See PerformWalUpgradeIfNeeded().
	 */
	bool		upgrade_finalized;

	/*
	 * --wal-upgrade: true once the burst server has begun emitting the window
	 * (set just before XLOG_UPGRADE_START, cleared only at finalize). Durable
	 * evidence that an upgrade started here, so a crashed partial upgrade is
	 * detectable even when the START-bearing WAL did not survive. started &&
	 * !finalized => refuse to auto-serve.  See PerformWalUpgradeIfNeeded().
	 */
	bool		upgrade_started;

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
