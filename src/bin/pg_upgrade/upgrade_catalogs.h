/*
 * upgrade_catalogs.h
 *
 * Catalog observations for upgrade validation.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/upgrade_catalogs.h
 */
#ifndef PG_UPGRADE_CATALOGS_H
#define PG_UPGRADE_CATALOGS_H

/* Include pg_upgrade.h first. */

typedef enum UpgradeCatalogSide
{
	UPGRADE_CATALOG_OLD,
	UPGRADE_CATALOG_NEW
}			UpgradeCatalogSide;

typedef struct UpgradeCatalogRelation
{
	Oid			relation_oid;
	/* Physical filenumber, including mapped catalogs. */
	Oid			filenumber;
	/* Physical tablespace, including the database default. */
	Oid			tablespace_oid;
	char		relkind;
	char		persistence;
	bool		transferred;
}			UpgradeCatalogRelation;

typedef struct UpgradeCatalogScope
{
	/*
	 * Per-database new-major relations remain OID-sorted until operation
	 * preparation.
	 */
	UpgradeCatalogRelation *relations;
	size_t		nrelations;
	size_t		relations_capacity;
}			UpgradeCatalogScope;

typedef struct UpgradeCatalogDatabase
{
	Oid			oid;
	Oid			tablespace_oid;
	const char *name;
	DbInfo	   *dbinfo;			/* borrowed stock transfer information */
	/* Old template0 has no per-relation catalog inventory. */
	bool		relations_collected;
	bool		template0;
	UpgradeCatalogScope scope;
}			UpgradeCatalogDatabase;

typedef void (*UpgradeCatalogSink) (void *arg, UpgradeCatalogSide side,
									const UpgradeCatalogDatabase * database,
									UpgradeCatalogScope * scope);

/* The sink owns each scope until it returns; collection then releases it. */
extern void collect_upgrade_catalogs(ClusterInfo *cluster,
									 UpgradeCatalogSide side,
									 UpgradeCatalogSink sink, void *sink_arg);

#endif							/* PG_UPGRADE_CATALOGS_H */
