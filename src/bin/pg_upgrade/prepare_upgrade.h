/*
 * prepare_upgrade.h
 *
 * Prepare the relink file, START record data, and old WAL for recovery.
 * Invoke upgrade WAL emission in a backend of the new cluster.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/prepare_upgrade.h
 */
#ifndef PG_UPGRADE_PREPARE_H
#define PG_UPGRADE_PREPARE_H

typedef enum RelinkInputSide
{
	RELINK_INPUT_OLD,
	RELINK_INPUT_NEW
} RelinkInputSide;

typedef struct UpgradePreparation UpgradePreparation;

extern UpgradePreparation *create_upgrade_preparation(
													  ClusterInfo *old_cluster, ClusterInfo *new_cluster, transferMode mode);
extern void prepare_upgrade_relink_input(UpgradePreparation *preparation,
										 ClusterInfo *cluster,
										 RelinkInputSide side,
										 const UpgradeTask *task);
extern void prepare_upgrade_old_cluster(UpgradePreparation *preparation);
extern void finish_upgrade_preparation(UpgradePreparation *preparation);
extern void emit_upgrade_wal(UpgradePreparation *preparation, PGconn *conn);
extern void free_upgrade_preparation(UpgradePreparation *preparation);
extern void prepare_upgrade_archive(bool stage_checkpoint);
extern char *get_old_cluster_archive_command(void);
extern void configure_upgrade_archive(const char *archive_command);

#endif							/* PG_UPGRADE_PREPARE_H */
