/*
 *	server.c
 *
 *	database server functions
 *
 *	Copyright (c) 2010-2026, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/server.c
 */

#include "postgres_fe.h"

#ifndef WIN32
#include <signal.h>
#include <sys/wait.h>
#endif

#include "common/connect.h"
#include "fe_utils/string_utils.h"
#include "libpq/pqcomm.h"
#include "pg_upgrade.h"

static PGconn *get_db_conn(ClusterInfo *cluster, const char *db_name);
static PGconn *pg_upgrade_handoff_shutdown_guard = NULL;
#ifndef WIN32
static void stop_postmaster_for_handoff(ClusterInfo *cluster);
#endif


/*
 * connectToServer()
 *
 *	Connects to the desired database on the designated server.
 *	If the connection attempt fails, this function logs an error
 *	message and calls exit() to kill the program.
 */
PGconn *
connectToServer(ClusterInfo *cluster, const char *db_name)
{
	PGconn	   *conn = get_db_conn(cluster, db_name);

	if (conn == NULL || PQstatus(conn) != CONNECTION_OK)
	{
		pg_log(PG_REPORT, "%s", PQerrorMessage(conn));

		if (conn)
			PQfinish(conn);

		printf(_("Failure, exiting\n"));
		exit(1);
	}

	PQclear(executeQueryOrDie(conn, ALWAYS_SECURE_SEARCH_PATH_SQL));

	return conn;
}


/*
 * get_db_conn()
 *
 * get database connection, using named database + standard params for cluster
 *
 * Caller must check for connection failure!
 */
static PGconn *
get_db_conn(ClusterInfo *cluster, const char *db_name)
{
	PQExpBufferData conn_opts;
	PGconn	   *conn;

	/* Build connection string with proper quoting */
	initPQExpBuffer(&conn_opts);
	appendPQExpBufferStr(&conn_opts, "dbname=");
	appendConnStrVal(&conn_opts, db_name);
	appendPQExpBufferStr(&conn_opts, " user=");
	appendConnStrVal(&conn_opts, os_info.user);
	appendPQExpBuffer(&conn_opts, " port=%d", cluster->port);
	if (cluster->sockdir)
	{
		appendPQExpBufferStr(&conn_opts, " host=");
		appendConnStrVal(&conn_opts, cluster->sockdir);
	}

	if (!protocol_negotiation_supported(cluster))
		appendPQExpBufferStr(&conn_opts, " max_protocol_version=3.0");

	conn = PQconnectdb(conn_opts.data);
	termPQExpBuffer(&conn_opts);
	return conn;
}


/*
 * cluster_conn_opts()
 *
 * Return standard command-line options for connecting to this cluster when
 * using psql, pg_dump, etc.  Ideally this would match what get_db_conn()
 * sets, but the utilities we need aren't very consistent about the treatment
 * of database name options, so we leave that out.
 *
 * Result is valid until the next call to this function.
 */
char *
cluster_conn_opts(ClusterInfo *cluster)
{
	static PQExpBuffer buf;

	if (buf == NULL)
		buf = createPQExpBuffer();
	else
		resetPQExpBuffer(buf);

	if (cluster->sockdir)
	{
		appendPQExpBufferStr(buf, "--host ");
		appendShellString(buf, cluster->sockdir);
		appendPQExpBufferChar(buf, ' ');
	}
	appendPQExpBuffer(buf, "--port %d --username ", cluster->port);
	appendShellString(buf, os_info.user);

	return buf->data;
}


/*
 * executeQueryOrDie()
 *
 *	Formats a query string from the given arguments and executes the
 *	resulting query.  If the query fails, this function logs an error
 *	message and calls exit() to kill the program.
 */
PGresult *
executeQueryOrDie(PGconn *conn, const char *fmt, ...)
{
	static char query[QUERY_ALLOC];
	va_list		args;
	PGresult   *result;
	ExecStatusType status;

	va_start(args, fmt);
	vsnprintf(query, sizeof(query), fmt, args);
	va_end(args);

	pg_log(PG_VERBOSE, "executing: %s", query);
	result = PQexec(conn, query);
	status = PQresultStatus(result);

	if ((status != PGRES_TUPLES_OK) && (status != PGRES_COMMAND_OK))
	{
		pg_log(PG_REPORT, "SQL command failed\n%s\n%s", query,
			   PQerrorMessage(conn));
		PQclear(result);
		if (conn == pg_upgrade_handoff_shutdown_guard)
			pg_upgrade_handoff_shutdown_guard = NULL;
		PQfinish(conn);
		printf(_("Failure, exiting\n"));
		exit(1);
	}
	else
		return result;
}


static void
stop_postmaster_atexit(void)
{
	stop_postmaster(true);
	cleanup_pg_upgrade_handoff_after_stop();
}


static bool postmaster_stop_started = false;


/*
 * Start smart shutdown with an open old-primary connection holding it before
 * the shutdown checkpoint.
 */
PGconn *
begin_postmaster_stop_for_handoff(ClusterInfo *cluster)
{
	Assert(cluster == &old_cluster);
	Assert(pg_upgrade_handoff_shutdown_guard == NULL);

	pg_upgrade_handoff_shutdown_guard = connectToServer(cluster, "template1");
	exec_prog(SERVER_STOP_LOG_FILE, NULL, true, true,
			  "\"%s/pg_ctl\" -W -D \"%s\" -o \"%s\" -m smart stop",
			  cluster->bindir, cluster->pgconfig,
			  cluster->pgopts ? cluster->pgopts : "");

	return pg_upgrade_handoff_shutdown_guard;
}


bool
start_postmaster(ClusterInfo *cluster, bool report_and_exit_on_error)
{
	PGconn	   *conn;
	bool		pg_ctl_return = false;
	PQExpBufferData cmd;
	PQExpBufferData postmaster_options;
	PQExpBufferData socket_options;
	PQExpBufferData pgoptions;

	static bool exit_hook_registered = false;

	if (!exit_hook_registered)
	{
		atexit(stop_postmaster_atexit);
		exit_hook_registered = true;
	}

	initPQExpBuffer(&socket_options);

#if !defined(WIN32)
	if (!(cluster == &old_cluster && old_cluster.listen_addresses != NULL))
		appendPQExpBufferStr(&socket_options, " -c listen_addresses=''");
	appendPQExpBufferStr(&socket_options,
						 " -c unix_socket_permissions=0700");

	/* Have a sockdir?	Tell the postmaster. */
	if (cluster->sockdir)
		appendPQExpBuffer(&socket_options,
						  " -c unix_socket_directories='%s'",
						  cluster->sockdir);
#endif

	if (cluster == &old_cluster && old_cluster.listen_addresses != NULL)
	{
		PQExpBufferData listen_option;

		initPQExpBuffer(&listen_option);
		appendPQExpBuffer(&listen_option, "listen_addresses=%s",
						  old_cluster.listen_addresses);
		appendPQExpBufferStr(&socket_options, " -c ");
		appendShellString(&socket_options, listen_option.data);
		termPQExpBuffer(&listen_option);
	}

	initPQExpBuffer(&pgoptions);

	/*
	 * Construct a parameter string which is passed to the server process.
	 *
	 * Turn off durability requirements to improve object creation speed, and
	 * we only modify the new cluster, so only use it there.  If there is a
	 * crash, the new cluster has to be recreated anyway.  fsync=off is a big
	 * win on ext4.
	 */
	if (cluster == &new_cluster)
	{
		appendPQExpBufferStr(&pgoptions,
							 " -c synchronous_commit=off -c fsync=off -c full_page_writes=off");

		if (user_opts.wal_upgrade)
		{
			/*
			 * Keep slot-retained WAL unbounded and raise automatic checkpoint
			 * thresholds during window emission.
			 */
			appendPQExpBufferStr(&pgoptions,
								 " -c max_slot_wal_keep_size=-1"
								 " -c max_wal_size=1TB"
								 " -c checkpoint_timeout=1h");

		}
	}

	/*
	 * Use -b to disable autovacuum and logical replication launcher
	 * (effective in PG17 or later for the latter).
	 */
	initPQExpBuffer(&postmaster_options);
	appendPQExpBuffer(&postmaster_options, "-b%s %s%s -p %d",
					  pgoptions.data,
					  cluster->pgopts ? cluster->pgopts : "",
					  socket_options.data, cluster->port);

	termPQExpBuffer(&pgoptions);
	termPQExpBuffer(&socket_options);

	initPQExpBuffer(&cmd);
	appendPQExpBuffer(&cmd,
					  "\"%s/pg_ctl\" -w -l \"%s/%s\" -D \"%s\" -o ",
					  cluster->bindir, log_opts.logdir, SERVER_LOG_FILE,
					  cluster->pgconfig);
	appendShellString(&cmd, postmaster_options.data);
	appendPQExpBufferStr(&cmd, " start");
	termPQExpBuffer(&postmaster_options);

	/*
	 * Don't throw an error right away, let connecting throw the error because
	 * it might supply a reason for the failure.
	 */
	pg_ctl_return = exec_prog(SERVER_START_LOG_FILE,
	/* pass both file names if they differ */
							  (strcmp(SERVER_LOG_FILE,
									  SERVER_START_LOG_FILE) != 0) ?
							  SERVER_LOG_FILE : NULL,
							  report_and_exit_on_error, false,
							  "%s", cmd.data);

	/* Did it fail and we are just testing if the server could be started? */
	if (!pg_ctl_return && !report_and_exit_on_error)
	{
		termPQExpBuffer(&cmd);
		return false;
	}

	/*
	 * We set this here to make sure atexit() shuts down the server, but only
	 * if we started the server successfully.  We do it before checking for
	 * connectivity in case the server started but there is a connectivity
	 * failure.  If pg_ctl did not return success, we will exit below.
	 *
	 * Pre-9.1 servers do not have PQping(), so we could be leaving the server
	 * running if authentication was misconfigured, so someday we might went
	 * to be more aggressive about doing server shutdowns even if pg_ctl
	 * fails, but now (2013-08-14) it seems prudent to be cautious.  We don't
	 * want to shutdown a server that might have been accidentally started
	 * during the upgrade.
	 */
	if (pg_ctl_return)
	{
		os_info.running_cluster = cluster;
		postmaster_stop_started = false;
	}

	/*
	 * pg_ctl -w might have failed because the server couldn't be started, or
	 * there might have been a connection problem in _checking_ if the server
	 * has started.  Therefore, even if pg_ctl failed, we continue and test
	 * for connectivity in case we get a connection reason for the failure.
	 */
	if ((conn = get_db_conn(cluster, "template1")) == NULL ||
		PQstatus(conn) != CONNECTION_OK)
	{
		pg_log(PG_REPORT, "\n%s", PQerrorMessage(conn));
		if (conn)
			PQfinish(conn);
		if (cluster == &old_cluster)
			pg_fatal("could not connect to source postmaster started with the command:\n"
					 "%s",
					 cmd.data);
		else
			pg_fatal("could not connect to target postmaster started with the command:\n"
					 "%s",
					 cmd.data);
	}
	PQfinish(conn);

	/*
	 * If pg_ctl failed, and the connection didn't fail, and
	 * report_and_exit_on_error is enabled, fail now.  This could happen if
	 * the server was already running.
	 */
	if (!pg_ctl_return)
	{
		if (cluster == &old_cluster)
			pg_fatal("pg_ctl failed to start the source server, or connection failed");
		else
			pg_fatal("pg_ctl failed to start the target server, or connection failed");
	}

	termPQExpBuffer(&cmd);
	return true;
}


void
stop_postmaster(bool in_atexit)
{
	ClusterInfo *cluster;
	bool		handoff_guarded_stop = false;

	if (os_info.running_cluster == &old_cluster)
		cluster = &old_cluster;
	else if (os_info.running_cluster == &new_cluster)
		cluster = &new_cluster;
	else
		return;					/* no cluster running */

	/* During atexit, repeat only an incomplete HANDOFF stop. */
	if (in_atexit && postmaster_stop_started &&
		!pg_upgrade_handoff_requires_immediate_stop())
		return;
	postmaster_stop_started = true;

	if (pg_upgrade_handoff_shutdown_guard != NULL)
	{
		Assert(cluster == &old_cluster);
		handoff_guarded_stop = true;

		/*
		 * Outside atexit, request fast shutdown before closing the connection
		 * that holds smart shutdown before its checkpoint.
		 */
		if (!in_atexit)
			exec_prog(SERVER_STOP_LOG_FILE, NULL, true, true,
					  "\"%s/pg_ctl\" -W -D \"%s\" -o \"%s\" -m fast stop",
					  cluster->bindir, cluster->pgconfig,
					  cluster->pgopts ? cluster->pgopts : "");
		PQfinish(pg_upgrade_handoff_shutdown_guard);
		pg_upgrade_handoff_shutdown_guard = NULL;
	}

#ifndef WIN32
	if (!in_atexit && pg_upgrade_handoff_requires_immediate_stop())
		stop_postmaster_for_handoff(cluster);
	else
#endif
	if (!in_atexit && handoff_guarded_stop)
	{
		bool		stopped;

		stopped = exec_prog(SERVER_STOP_LOG_FILE, NULL, true, false,
							"\"%s/pg_ctl\" -w -D \"%s\" -o \"%s\" -m fast stop",
							cluster->bindir, cluster->pgconfig,
							cluster->pgopts ? cluster->pgopts : "");
		if (!stopped && pid_lock_file_exists(cluster->pgdata))
			pg_fatal("could not stop the source postmaster during pg_upgrade handoff");
	}
	else
		exec_prog(SERVER_STOP_LOG_FILE, NULL, !in_atexit, !in_atexit,
				  "\"%s/pg_ctl\" -w -D \"%s\" -o \"%s\" %s stop",
				  cluster->bindir, cluster->pgconfig,
				  cluster->pgopts ? cluster->pgopts : "",
				  in_atexit ?
				  (pg_upgrade_handoff_requires_immediate_stop() ||
				   (cluster == &old_cluster &&
					old_cluster.listen_addresses != NULL) ?
				   "-m immediate" : "-m fast") : "-m smart");

	os_info.running_cluster = NULL;
}

#ifndef WIN32
/*
 * Run pg_ctl in a subprocess and kill its process group after a frontend
 * signal.
 */
static void
stop_postmaster_for_handoff(ClusterInfo *cluster)
{
	pid_t		child;
	int			status;

	fflush(NULL);
	child = fork();
	if (child < 0)
		pg_fatal("could not create process to stop the source postmaster: %m");
	if (child == 0)
	{
		if (setpgid(0, 0) != 0)
			_exit(EXIT_FAILURE);
		os_info.running_cluster = NULL;
		_exit(exec_prog(SERVER_STOP_LOG_FILE, NULL, true, false,
						"\"%s/pg_ctl\" -w -D \"%s\" -o \"%s\" -m fast stop",
						cluster->bindir, cluster->pgconfig,
						cluster->pgopts ? cluster->pgopts : "") ?
			  EXIT_SUCCESS : EXIT_FAILURE);
	}

	/* Place the subprocess in its process group from this process too. */
	if (setpgid(child, child) != 0 && errno != EACCES && errno != ESRCH)
	{
		int			save_errno = errno;

		(void) kill(child, SIGKILL);
		(void) waitpid(child, &status, 0);
		errno = save_errno;
		pg_fatal("could not create process group to stop the source postmaster: %m");
	}

	for (;;)
	{
		pid_t		result = waitpid(child, &status, WNOHANG);
		int			signal_status = pg_upgrade_handoff_signal_status();

		if (result == child)
			break;
		if (result < 0 && errno != EINTR)
			pg_fatal("could not wait for process stopping the source postmaster: %m");
		if (signal_status != 0)
			(void) kill(-child, SIGKILL);
		pg_usleep(10000L);
	}

	if ((!WIFEXITED(status) || WEXITSTATUS(status) != EXIT_SUCCESS) &&
		pid_lock_file_exists(cluster->pgdata))
		pg_fatal("could not stop the source postmaster during pg_upgrade handoff");
}
#endif

/*
 * check_pghost_envvar()
 *
 * Tests that PGHOST does not point to a non-local server
 */
void
check_pghost_envvar(void)
{
	PQconninfoOption *option;
	PQconninfoOption *start;

	/* Get valid libpq env vars from the PQconndefaults function */

	start = PQconndefaults();

	if (!start)
		pg_fatal("out of memory");

	for (option = start; option->keyword != NULL; option++)
	{
		if (option->envvar && (strcmp(option->envvar, "PGHOST") == 0 ||
							   strcmp(option->envvar, "PGHOSTADDR") == 0))
		{
			const char *value = getenv(option->envvar);

			if (value && strlen(value) > 0 &&
			/* check for 'local' host values */
				(strcmp(value, "localhost") != 0 && strcmp(value, "127.0.0.1") != 0 &&
				 strcmp(value, "::1") != 0 && !is_unixsock_path(value)))
				pg_fatal("libpq environment variable %s has a non-local server value: %s",
						 option->envvar, value);
		}
	}

	/* Free the memory that libpq allocated on our behalf */
	PQconninfoFree(start);
}
