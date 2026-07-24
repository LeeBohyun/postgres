WAL-Logging pg_upgrade (`--wal-upgrade`)
========================================

This branch is a fork of PostgreSQL that adds `pg_upgrade --wal-upgrade`, which
captures a major-version upgrade as WAL so it can be streamed to physical
standbys and replayed across the backup/PITR boundary — closing the replication
and durability gaps of stock `pg_upgrade`.

For the design rationale, interface, workflows, tests, and an example
integration, see:

**[The Case For WAL-Logging pg_upgrade](The_Case_For_WAL-Logging_pg_upgrade.pdf)**

---

PostgreSQL Database Management System
=====================================

This directory contains the source code distribution of the PostgreSQL
database management system.

PostgreSQL is an advanced object-relational database management system
that supports an extended subset of the SQL standard, including
transactions, foreign keys, subqueries, triggers, user-defined types
and functions.  This distribution also contains C language bindings.

Copyright and license information can be found in the file COPYRIGHT.

General documentation about this version of PostgreSQL can be found at
<https://www.postgresql.org/docs/devel/>.  In particular, information
about building PostgreSQL from the source code can be found at
<https://www.postgresql.org/docs/devel/installation.html>.

The latest version of this software, and related software, may be
obtained at <https://www.postgresql.org/download/>.  For more information
look at our web site located at <https://www.postgresql.org/>.
