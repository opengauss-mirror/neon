#!/usr/bin/env python3
"""Check running openGauss Neon endpoints (requires psycopg2).

Usage: check_read_replica.py --primary 'host=... port=... dbname=postgres user=...'
                           --replica 'host=... port=... dbname=postgres user=...'
Repeat --replica to check multiple replicas. Only a uniquely named test table
is created and removed; use a dedicated test database.
"""

import argparse
import subprocess
import time
import uuid
from contextlib import closing

import psycopg2


def query(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall() if cur.description else None


def lsn(value):
    hi, lo = value.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def caught_up(primary, replicas):
    target = lsn(query(primary, "SELECT pg_current_xlog_location()")[0][0])
    deadline = time.monotonic() + 30
    for replica in replicas:
        while True:
            # openGauss returns (term, location), unlike PostgreSQL's scalar.
            row = query(replica, "SELECT * FROM pg_last_xlog_replay_location()")[0]
            if lsn(row[1]) >= target:
                break
            if time.monotonic() > deadline:
                raise AssertionError("replica did not replay the committed WAL within 30s")
            time.sleep(0.1)


def rejected(conn, sql, states):
    try:
        query(conn, sql)
    except psycopg2.Error as exc:
        assert exc.pgcode in states, (sql, exc.pgcode, str(exc))
    else:
        raise AssertionError("statement unexpectedly succeeded: " + sql)


def check(primary, replicas):
    table = "neon_replica_check_" + uuid.uuid4().hex[:12]
    assert query(primary, "SELECT pg_is_in_recovery()") == [(False,)]
    for replica in replicas:
        assert query(replica, "SELECT pg_is_in_recovery()") == [(True,)]
    try:
        query(primary, "CREATE TABLE %s (id int PRIMARY KEY, v int, payload text)" % table)
        query(primary, "INSERT INTO %s SELECT i, i, repeat('payload', 100) "
              "FROM generate_series(1, 5000) AS i" % table)
        caught_up(primary, replicas)
        expected = query(primary, "SELECT count(*), sum(v) FROM " + table)
        for replica in replicas:
            assert query(replica, "SELECT count(*), sum(v) FROM " + table) == expected
            rejected(replica, "INSERT INTO %s VALUES (6000, 1, '')" % table, {"25006"})
            rejected(replica, "CREATE TABLE %s_forbidden (id int)" % table, {"25006"})
            # A session setting must not make a recovering server writable.
            try:
                query(replica, "SET default_transaction_read_only = off")
            except psycopg2.Error as exc:
                assert exc.pgcode == "25006", str(exc)
            rejected(replica, "UPDATE %s SET v=0 WHERE id=1" % table, {"25006"})

        # Reuse connections and cached heap/index pages across WAL updates.
        for iteration in range(1, 6):
            query(primary, "UPDATE %s SET v=v+1 WHERE id <= 2500" % table)
            caught_up(primary, replicas)
            for replica in replicas:
                assert query(replica, "SELECT v FROM %s WHERE id=1" % table) == [(1 + iteration,)]
                assert query(replica, "SELECT sum(v) FROM " + table) == [
                    (12502500 + 2500 * iteration,)
                ]

        # Force WAL containing an uncommitted change out with a second commit.
        query(primary, "BEGIN")
        query(primary, "INSERT INTO %s VALUES (6000, 6000, 'uncommitted')" % table)
        with closing(psycopg2.connect(primary.dsn)) as other:
            other.autocommit = True
            query(other, "INSERT INTO %s VALUES (6001, 6001, 'committed')" % table)
            caught_up(other, replicas)
        for replica in replicas:
            assert query(replica, "SELECT id FROM %s WHERE id>=6000 ORDER BY id" % table) == [(6001,)]
        query(primary, "ROLLBACK")
        query(primary, "DELETE FROM %s WHERE id > 4500" % table)
        query(primary, "ALTER TABLE %s ADD COLUMN added int DEFAULT 7" % table)
        caught_up(primary, replicas)
        for replica in replicas:
            assert query(replica, "SELECT count(*), min(added), max(added) FROM " + table) == [
                (4500, 7, 7)
            ]

        # A transaction snapshot must remain stable as committed WAL advances.
        for replica in replicas:
            caught_up(primary, [replica])
            query(replica, "BEGIN ISOLATION LEVEL REPEATABLE READ")
            before = query(replica, "SELECT v FROM %s WHERE id=1" % table)
            query(primary, "UPDATE %s SET v=v+1 WHERE id=1" % table)
            caught_up(primary, [replica])
            assert query(replica, "SELECT v FROM %s WHERE id=1" % table) == before
            query(replica, "COMMIT")
            assert query(replica, "SELECT v FROM %s WHERE id=1" % table) == [(before[0][0] + 1,)]

        query(primary, "DROP TABLE " + table)
        caught_up(primary, replicas)
        for replica in replicas:
            rejected(replica, "SELECT * FROM " + table, {"42P01"})
    finally:
        if not primary.closed:
            query(primary, "ROLLBACK")
        for replica in replicas:
            if not replica.closed:
                query(replica, "ROLLBACK")
        if not primary.closed:
            query(primary, "DROP TABLE IF EXISTS " + table)


def check_restart(primary, replica_dsns, endpoint, neon_local):
    """Restart the first replica while a transaction is open on the primary."""
    table = "neon_replica_restart_" + uuid.uuid4().hex[:12]
    replicas = []
    old_snapshot = None
    try:
        query(primary, "CREATE TABLE %s (id int PRIMARY KEY, v int)" % table)
        query(primary, "INSERT INTO %s VALUES (1, 10)" % table)
        # Also retain an older xmin without assigning a transaction id.
        old_snapshot = psycopg2.connect(primary.dsn)
        old_snapshot.autocommit = True
        query(old_snapshot, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        assert query(old_snapshot, "SELECT v FROM %s WHERE id=1" % table) == [(10,)]
        query(primary, "BEGIN")
        query(primary, "UPDATE %s SET v=20 WHERE id=1" % table)
        with closing(psycopg2.connect(primary.dsn)) as other:
            other.autocommit = True
            query(other, "INSERT INTO %s VALUES (2, 30)" % table)
            query(other, "CHECKPOINT")
            for action in ("stop", "start"):
                subprocess.run([neon_local, "endpoint", action, endpoint],
                               check=True, timeout=60)
            for dsn in replica_dsns:
                conn = psycopg2.connect(dsn, connect_timeout=5)
                conn.autocommit = True
                replicas.append(conn)
                query(conn, "SET statement_timeout = '30s'")
            caught_up(other, replicas)
        for replica in replicas:
            actual = query(replica, "SELECT * FROM %s ORDER BY id" % table)
            assert actual == [(1, 10), (2, 30)], actual
        # A pre-backup transaction that commits after startup must receive its
        # real replayed CSN, so an existing snapshot still sees the old value.
        query(replicas[0], "BEGIN ISOLATION LEVEL REPEATABLE READ")
        assert query(replicas[0], "SELECT v FROM %s WHERE id=1" % table) == [(10,)]
        query(primary, "COMMIT")
        caught_up(primary, replicas)
        assert query(replicas[0], "SELECT v FROM %s WHERE id=1" % table) == [(10,)]
        query(replicas[0], "COMMIT")
        for replica in replicas:
            actual = query(replica, "SELECT * FROM %s ORDER BY id" % table)
            assert actual == [(1, 20), (2, 30)], actual
        print("PASS: restart with active transaction, bootstrap visibility, snapshot isolation")
    finally:
        if old_snapshot is not None:
            old_snapshot.close()
        for replica in replicas:
            try:
                query(replica, "ROLLBACK")
            finally:
                replica.close()
        query(primary, "ROLLBACK")
        query(primary, "DROP TABLE IF EXISTS " + table)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--primary", required=True)
    parser.add_argument("--replica", action="append", required=True)
    parser.add_argument("--restart-endpoint",
                        help="also stop/start the first replica by its local endpoint name")
    parser.add_argument("--neon-local", default="neon_local",
                        help="CLI path for --restart-endpoint; uses NEON_REPO_DIR")
    args = parser.parse_args()
    connections = []
    try:
        for dsn in [args.primary] + args.replica:
            conn = psycopg2.connect(dsn, connect_timeout=5)
            conn.autocommit = True
            connections.append(conn)
            query(conn, "SET statement_timeout = '30s'")
        check(connections[0], connections[1:])
        print("PASS: read-only recovery, WAL replay, cached pages/indexes, transactions, DDL")
        if args.restart_endpoint:
            check_restart(connections[0], args.replica, args.restart_endpoint, args.neon_local)
    finally:
        for conn in connections:
            conn.close()


if __name__ == "__main__":
    main()
