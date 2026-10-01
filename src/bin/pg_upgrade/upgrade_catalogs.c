/*
 * upgrade_catalogs.c
 *
 * Collect catalog observations for upgrade validation.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * src/bin/pg_upgrade/upgrade_catalogs.c
 */

#include "postgres_fe.h"

#include "catalog/pg_tablespace_d.h"
#include "pg_upgrade.h"
#include "upgrade_catalogs.h"

#define CATALOG_CHUNK_ROWS 1024

static const char *
required_value(PGresult *result, int row, int col)
{
	if (PQgetisnull(result, row, col))
		pg_fatal("NULL in catalog query column \"%s\"", PQfname(result, col));
	return PQgetvalue(result, row, col);
}

static int
compare_databases(const void *a, const void *b)
{
	Oid			left = ((const UpgradeCatalogDatabase *) a)->oid;
	Oid			right = ((const UpgradeCatalogDatabase *) b)->oid;

	return (left > right) - (left < right);
}

static int
compare_relations(const void *a, const void *b)
{
	Oid			left = ((const UpgradeCatalogRelation *) a)->relation_oid;
	Oid			right = ((const UpgradeCatalogRelation *) b)->relation_oid;

	return (left > right) - (left < right);
}

/*
 * Collect non-temporary physical relation keys, placing shared relations in
 * the shared scope.
 */
static void
collect_database_relations(UpgradeCatalogSide side,
						   UpgradeCatalogDatabase * database,
						   UpgradeCatalogScope * shared,
						   PGconn *conn, bool include_shared)
{
	PGresult   *result;
	bool		completed = false;
	const char *params[] = {include_shared ? "true" : "false"};
	char	   *query = psprintf(
								 "SELECT c.oid, pg_catalog.pg_relation_filenode(c.oid) AS filenumber, "
								 "CASE WHEN c.reltablespace = 0 THEN %u ELSE c.reltablespace END AS tablespace, "
								 "c.relkind, c.relpersistence, c.relisshared "
								 "FROM pg_catalog.pg_class c "
								 "WHERE c.relkind IN ('r', 'i', 'S', 't', 'm') AND c.relpersistence <> 't' "
								 "AND (NOT c.relisshared OR $1::pg_catalog.bool)",
								 database->tablespace_oid);

	if (!PQsendQueryParams(conn, query, lengthof(params), NULL, params, NULL, NULL, 0) ||
		!PQsetChunkedRowsMode(conn, CATALOG_CHUNK_ROWS))
		pg_fatal("could not start catalog query for database \"%s\": %s",
				 PQdb(conn), PQerrorMessage(conn));
	pg_free(query);
	while ((result = PQgetResult(conn)) != NULL)
	{
		ExecStatusType status = PQresultStatus(result);

		if (completed || (status != PGRES_TUPLES_CHUNK && status != PGRES_TUPLES_OK))
			pg_fatal("catalog query failed for database \"%s\" (%s): %s",
					 PQdb(conn), PQresStatus(status), PQresultErrorMessage(result));
		if (PQnfields(result) != 6 ||
			(status == PGRES_TUPLES_OK && PQntuples(result) != 0) ||
			PQntuples(result) > CATALOG_CHUNK_ROWS)
			pg_fatal("invalid catalog query result chunk in database \"%s\"", PQdb(conn));
		for (int row = 0; row < PQntuples(result); row++)
		{
			UpgradeCatalogScope *scope = required_value(result, row, 5)[0] == 't' ?
				shared : &database->scope;
			UpgradeCatalogRelation relation = {0};

			relation.relation_oid = atooid(required_value(result, row, 0));
			relation.filenumber = atooid(required_value(result, row, 1));
			relation.tablespace_oid = atooid(required_value(result, row, 2));
			relation.relkind = required_value(result, row, 3)[0];
			relation.persistence = required_value(result, row, 4)[0];
			scope->relations = upgrade_reserve_array(scope->relations,
													 &scope->relations_capacity,
													 add_size(scope->nrelations, 1),
													 CATALOG_CHUNK_ROWS, sizeof(relation));
			scope->relations[scope->nrelations++] = relation;
		}
		completed = status == PGRES_TUPLES_OK;
		PQclear(result);
	}
	if (!completed || PQstatus(conn) != CONNECTION_OK)
		pg_fatal("catalog query did not complete in database \"%s\": %s",
				 PQdb(conn), PQerrorMessage(conn));
	if (side == UPGRADE_CATALOG_NEW && database->scope.nrelations > 1)
		qsort(database->scope.relations, database->scope.nrelations,
			  sizeof(*database->scope.relations), compare_relations);
}

static void
free_scope(UpgradeCatalogScope * scope)
{
	pg_free(scope->relations);
	scope->relations = NULL;
	scope->nrelations = scope->relations_capacity = 0;
}

static void
collect_one_database(ClusterInfo *cluster, UpgradeCatalogSide side,
					 UpgradeCatalogDatabase * database,
					 UpgradeCatalogScope * shared, bool include_shared)
{
	PGconn	   *admin = NULL;
	PGconn	   *conn;

	if (database->dbinfo == NULL)
	{
		admin = connectToServer(cluster, "template1");
		PQclear(executeQueryOrDie(admin,
								  "ALTER DATABASE template0 ALLOW_CONNECTIONS = true"));
	}
	conn = connectToServer(cluster, database->name);
	collect_database_relations(side, database, shared, conn, include_shared);
	PQfinish(conn);
	if (admin != NULL)
	{
		PQclear(executeQueryOrDie(admin,
								  "ALTER DATABASE template0 ALLOW_CONNECTIONS = false"));
		PQfinish(admin);
	}
}

void
collect_upgrade_catalogs(ClusterInfo *cluster, UpgradeCatalogSide side,
						 UpgradeCatalogSink sink, void *sink_arg)
{
	UpgradeCatalogDatabase *databases;
	UpgradeCatalogScope shared = {0};
	size_t		ndatabases = 0;
	size_t		shared_source = SIZE_MAX;
	bool		have_template0 = false;

	/*
	 * Build and OID-sort the database list before collecting relation
	 * metadata.
	 */
	databases = pg_malloc0_array(UpgradeCatalogDatabase,
								 cluster->dbarr.ndbs + 1);
	for (int i = 0; i < cluster->dbarr.ndbs; i++)
	{
		DbInfo	   *db = &cluster->dbarr.dbs[i];

		databases[ndatabases++] = (UpgradeCatalogDatabase)
		{
			.oid = db->db_oid, .tablespace_oid = db->db_tablespace_oid,
				.name = db->db_name, .dbinfo = db,
				.relations_collected = side != UPGRADE_CATALOG_OLD ||
				db->db_oid != cluster->template0->db_oid,
				.template0 = db->db_oid == cluster->template0->db_oid
		};
		have_template0 |= db->db_oid == cluster->template0->db_oid;
	}
	if (!have_template0)
		databases[ndatabases++] = (UpgradeCatalogDatabase)
	{
		.oid = cluster->template0->db_oid,
			.tablespace_oid = cluster->template0->db_tablespace_oid,
			.name = "template0",
			.relations_collected = side != UPGRADE_CATALOG_OLD,
			.template0 = true
	};
	qsort(databases, ndatabases, sizeof(*databases), compare_databases);

	for (size_t i = 0; i < ndatabases; i++)
		if (databases[i].relations_collected)
		{
			shared_source = i;
			break;
		}
	if (shared_source == SIZE_MAX)
		pg_fatal("no database is available to collect shared catalog relations");

	/* Collect and emit shared storage before the OID-ordered database scopes. */
	collect_one_database(cluster, side, &databases[shared_source], &shared, true);
	{
		UpgradeCatalogDatabase shared_database =
		{
			.oid = InvalidOid,
			.tablespace_oid = GLOBALTABLESPACE_OID,
			.name = "global",
			.relations_collected = true
		};

		sink(sink_arg, side, &shared_database, &shared);
		free_scope(&shared);
	}

	for (size_t i = 0; i < ndatabases; i++)
	{
		UpgradeCatalogDatabase *db = &databases[i];

		if (db->relations_collected && i != shared_source)
			collect_one_database(cluster, side, db, &shared, false);
		sink(sink_arg, side, db, &db->scope);
		free_scope(&db->scope);
	}
	pg_free(databases);
}
