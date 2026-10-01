\getenv libdir PG_LIBDIR
\getenv dlsuffix PG_DLSUFFIX
\set regresslib :libdir '/regress' :dlsuffix

CREATE FUNCTION test_pg_upgrade_directory_paths()
    RETURNS bool
    AS :'regresslib'
    LANGUAGE C;
SELECT test_pg_upgrade_directory_paths() AS ok;
