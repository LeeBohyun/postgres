/*-------------------------------------------------------------------------
 *
 * config.c
 *    Server configuration for WAL-logged pg_upgrade recovery.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/backend/access/pgupgrade/config.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/pgupgrade_config.h"
#include "common/pg_upgrade_data.h"
#include "common/pg_upgrade_records.h"
#include "libpq/libpq-be.h"
#include "postmaster/postmaster.h"
#include "replication/walsender.h"
#include "storage/fd.h"

int			pg_upgrade_standby_transfer_mode = PG_UPGRADE_XFER_MIRROR;
char	   *pg_upgrade_standby_old_datadir = NULL;

const struct config_enum_entry pg_upgrade_standby_transfer_mode_options[] = {
	{"mirror", PG_UPGRADE_XFER_MIRROR, false},
	{"clone", UPGRADE_RELINK_MODE_CLONE, false},
	{"copy", UPGRADE_RELINK_MODE_COPY, false},
	{"copy_file_range", UPGRADE_RELINK_MODE_COPY_FILE_RANGE, false},
	{"link", UPGRADE_RELINK_MODE_LINK, false},
	{"swap", UPGRADE_RELINK_MODE_SWAP, false},
	{NULL, 0, false}
};

#ifdef WIN32
static bool
is_loopback_address(const SockAddr *address)
{
	if (address->addr.ss_family == AF_INET)
	{
		const struct sockaddr_in *addr =
			(const struct sockaddr_in *) &address->addr;

		return (pg_ntoh32(addr->sin_addr.s_addr) & 0xff000000U) ==
			0x7f000000U;
	}
	else if (address->addr.ss_family == AF_INET6)
	{
		static const unsigned char ipv6_loopback[16] =
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
		static const unsigned char ipv4_mapped_prefix[12] =
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
		const struct sockaddr_in6 *addr =
			(const struct sockaddr_in6 *) &address->addr;
		const unsigned char *bytes = addr->sin6_addr.s6_addr;

		return memcmp(bytes, ipv6_loopback, sizeof(ipv6_loopback)) == 0 ||
			(memcmp(bytes, ipv4_mapped_prefix,
					sizeof(ipv4_mapped_prefix)) == 0 && bytes[12] == 127);
	}

	return false;
}
#endif

bool
PgUpgradeValidateConnection(const struct Port *port)
{
	bool		physical_replication = am_walsender && !am_db_walsender;
	bool		control;

#ifndef WIN32
	control = !am_walsender && port->raddr.addr.ss_family == AF_UNIX;
#else
	control = !am_walsender && is_loopback_address(&port->raddr);
#endif
	if (!control && !physical_replication)
		ereport(FATAL,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only local control connections and physical replication connections are allowed during upgrade handoff")));

	return physical_replication;
}

static bool
create_endpoint_file(void)
{
	const char *path = PG_UPGRADE_ENDPOINT_FILE;
	const char *tmppath = PG_UPGRADE_ENDPOINT_FILE ".tmp";
	const char *addresses = ListenAddresses ? ListenAddresses : "";
	FILE	   *fp;

	if ((fp = fopen(tmppath, PG_BINARY_W)) == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmppath)));
		return false;
	}
	if (fprintf(fp, "%d\n", PostPortNumber) < 0 ||
		fwrite(addresses, 1, strlen(addresses), fp) != strlen(addresses) ||
		fflush(fp) != 0 || pg_fsync(fileno(fp)) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", tmppath)));
		fclose(fp);
		unlink(tmppath);
		return false;
	}
	if (fclose(fp) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tmppath)));
		unlink(tmppath);
		return false;
	}
	return durable_rename(tmppath, path, LOG) == 0;
}

bool
PgUpgradeRefreshEndpointFile(void)
{
	const char *path = PG_UPGRADE_ENDPOINT_FILE;
	struct stat st;

	if (create_endpoint_file())
		return true;
	if (lstat(path, &st) != 0)
	{
		if (errno == ENOENT)
			return true;
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not access file \"%s\": %m", path)));
		return false;
	}
	return durable_unlink(path, LOG) == 0;
}
