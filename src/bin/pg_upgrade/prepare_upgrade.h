/*
 * prepare_upgrade.h
 *
 * Store catalog observations and emit upgrade WAL.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/prepare_upgrade.h
 */
#ifndef PG_UPGRADE_PREPARE_H
#define PG_UPGRADE_PREPARE_H

#include "upgrade_catalogs.h"

typedef struct UpgradePreparation UpgradePreparation;

extern UpgradePreparation * create_upgrade_preparation(
													   ClusterInfo *old_cluster, ClusterInfo *new_cluster, transferMode mode);
extern void prepare_upgrade_catalogs(UpgradePreparation * preparation,
									 ClusterInfo *cluster,
									 UpgradeCatalogSide side);
extern void prepare_upgrade_source(UpgradePreparation * preparation);
extern void finish_upgrade_preparation(UpgradePreparation * preparation);
extern void bind_upgrade_preparation(UpgradePreparation * preparation);
extern void emit_upgrade_wal(UpgradePreparation * preparation, PGconn *conn);
extern void free_upgrade_preparation(UpgradePreparation * preparation);

#endif							/* PG_UPGRADE_PREPARE_H */
