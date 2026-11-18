/*
 *	file.c
 *
 *	file system operations
 *
 *	Copyright (c) 2010-2026, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/file.c
 */

#include "postgres_fe.h"

#include <sys/stat.h>
#include <fcntl.h>

#include "common/file_perm.h"
#include "common/file_utils.h"
#include "pg_upgrade.h"


/*
 * cloneFile()
 *
 * Clones/reflinks a relation file from src to dst.
 *
 * schemaName/relName are relation's SQL name (used for error messages only).
 */
void
cloneFile(const char *source, const char *dst,
		  const char *schemaName, const char *relName)
{
	int			save_errno;

	switch (pg_clone_file(source, dst, &save_errno))
	{
		case PG_REFLINK_OK:
		case PG_REFLINK_UNSUPPORTED:
			break;
		case PG_REFLINK_ERROR:
			pg_fatal("error while cloning relation \"%s.%s\" (\"%s\" to \"%s\"): %s",
					 schemaName, relName, source, dst, strerror(save_errno));
	}
}


/*
 * copyFile()
 *
 * Copies a relation file from src to dst.
 * schemaName/relName are relation's SQL name (used for error messages only).
 */
void
copyFile(const char *src, const char *dst,
		 const char *schemaName, const char *relName)
{
#ifndef WIN32
	int			src_fd;
	int			dest_fd;
	char	   *buffer;

	if ((src_fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
		pg_fatal("error while copying relation \"%s.%s\": could not open file \"%s\": %m",
				 schemaName, relName, src);

	if ((dest_fd = open(dst, O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
						pg_file_create_mode)) < 0)
		pg_fatal("error while copying relation \"%s.%s\": could not create file \"%s\": %m",
				 schemaName, relName, dst);

	/* copy in fairly large chunks for best efficiency */
#define COPY_BUF_SIZE (50 * BLCKSZ)

	buffer = (char *) pg_malloc(COPY_BUF_SIZE);

	/* perform data copying i.e read src source, write to destination */
	while (true)
	{
		ssize_t		nbytes = read(src_fd, buffer, COPY_BUF_SIZE);

		if (nbytes < 0)
			pg_fatal("error while copying relation \"%s.%s\": could not read file \"%s\": %m",
					 schemaName, relName, src);

		if (nbytes == 0)
			break;

		errno = 0;
		if (write(dest_fd, buffer, nbytes) != nbytes)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			pg_fatal("error while copying relation \"%s.%s\": could not write file \"%s\": %m",
					 schemaName, relName, dst);
		}
	}

	pg_free(buffer);
	close(src_fd);
	close(dest_fd);

#else							/* WIN32 */

	if (CopyFile(src, dst, true) == 0)
	{
		_dosmaperr(GetLastError());
		pg_fatal("error while copying relation \"%s.%s\" (\"%s\" to \"%s\"): %m",
				 schemaName, relName, src, dst);
	}

#endif							/* WIN32 */
}


/*
 * copyFileByRange()
 *
 * Copies a relation file from src to dst.
 * schemaName/relName are relation's SQL name (used for error messages only).
 */
void
copyFileByRange(const char *source, const char *dst,
				const char *schemaName, const char *relName)
{
	int			save_errno;

	switch (pg_copy_file_range_all(source, dst, &save_errno))
	{
		case PG_REFLINK_OK:
		case PG_REFLINK_UNSUPPORTED:
			break;
		case PG_REFLINK_ERROR:
			pg_fatal("error while copying relation \"%s.%s\": could not copy file range from \"%s\" to \"%s\": %s",
					 schemaName, relName, source, dst, strerror(save_errno));
	}
}


/*
 * linkFile()
 *
 * Hard-links a relation file from src to dst.
 * schemaName/relName are relation's SQL name (used for error messages only).
 */
void
linkFile(const char *src, const char *dst,
		 const char *schemaName, const char *relName)
{
	if (link(src, dst) < 0)
		pg_fatal("error while creating link for relation \"%s.%s\" (\"%s\" to \"%s\"): %m",
				 schemaName, relName, src, dst);
}


void
check_file_clone(void)
{
	char		existing_file[MAXPGPATH];
	char		new_link_file[MAXPGPATH];
	int			save_errno;

	snprintf(existing_file, sizeof(existing_file), "%s/PG_VERSION", old_cluster.pgdata);
	snprintf(new_link_file, sizeof(new_link_file), "%s/PG_VERSION.clonetest", new_cluster.pgdata);
	unlink(new_link_file);		/* might fail */

	switch (pg_clone_file(existing_file, new_link_file, &save_errno))
	{
		case PG_REFLINK_OK:
			break;
		case PG_REFLINK_UNSUPPORTED:
			pg_fatal("file cloning not supported on this platform");
		case PG_REFLINK_ERROR:
			errno = save_errno;
			pg_fatal("could not clone file between old and new data directories: %m");
	}

	unlink(new_link_file);
}

void
check_copy_file_range(void)
{
	char		existing_file[MAXPGPATH];
	char		new_link_file[MAXPGPATH];
	int			save_errno;

	snprintf(existing_file, sizeof(existing_file), "%s/PG_VERSION", old_cluster.pgdata);
	snprintf(new_link_file, sizeof(new_link_file), "%s/PG_VERSION.copy_file_range_test", new_cluster.pgdata);
	unlink(new_link_file);		/* might fail */

	switch (pg_copy_file_range_all(existing_file, new_link_file, &save_errno))
	{
		case PG_REFLINK_OK:
			break;
		case PG_REFLINK_UNSUPPORTED:
			pg_fatal("copy_file_range not supported on this platform");
		case PG_REFLINK_ERROR:
			errno = save_errno;
			pg_fatal("could not copy file range between old and new data directories: %m");
	}

	unlink(new_link_file);
}

void
check_hard_link(transferMode transfer_mode)
{
	char		existing_file[MAXPGPATH];
	char		new_link_file[MAXPGPATH];

	snprintf(existing_file, sizeof(existing_file), "%s/PG_VERSION", old_cluster.pgdata);
	snprintf(new_link_file, sizeof(new_link_file), "%s/PG_VERSION.linktest", new_cluster.pgdata);
	unlink(new_link_file);		/* might fail */

	if (link(existing_file, new_link_file) < 0)
	{
		if (transfer_mode == TRANSFER_MODE_LINK)
			pg_fatal("could not create hard link between old and new data directories: %m\n"
					 "In link mode the old and new data directories must be on the same file system.");
		else if (transfer_mode == TRANSFER_MODE_SWAP)
			pg_fatal("could not create hard link between old and new data directories: %m\n"
					 "In swap mode the old and new data directories must be on the same file system.");
		else
			pg_fatal("unrecognized transfer mode");
	}

	unlink(new_link_file);
}
