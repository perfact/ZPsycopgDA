# ZPsycopgDA/db.py - query execution
#
# Copyright (C) 2004-2010 Federico Di Gregorio  <fog@debian.org>
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.

# Import modules needed by _psycopg to allow tools like py2exe to do
# their work without bothering about the module dependencies.

from Shared.DC.ZRDB.TM import TM
from Shared.DC.ZRDB import dbi_db

import psycopg2
from psycopg2.extensions import INTEGER, LONGINTEGER, BOOLEAN, DATE, TIME
from psycopg2.extensions import register_type
from psycopg2 import NUMBER, STRING, ROWID, DATETIME
from psycopg2.pool import AbstractConnectionPool, ThreadedConnectionPool

try:
    from Zope2.App.startup import RetryError, RetryDelayError
except ImportError:
    # Fallback declarations for graceful degradation,
    # Zope will not retry as intended!
    from Zope2.App.startup import ConflictError
    RetryError = ConflictError
    RetryDelayError = ConflictError

from logging import getLogger
LOG = getLogger('ZPsycopgDA.db')


def get_thread_id():
    '''Global function to retrieve the current thread id.'''
    try:
        import thread
        threading = thread
    except ImportError:
        import threading
    return threading.get_ident()


class CustomConnectionPool(ThreadedConnectionPool):

    def _connect(self, key=None):
        """Patched version of AbstractConnectionPool._connect(), which adds
        isolation levels, read-only mode, and encoding settings."""
        # Only use the 'dsn' keyword argument
        LOG.debug("Making a new connection to PostgreSQL "
                  "for pool %s, key %s" % (repr(self), repr(key)))
        args = self._kwargs
        conn = psycopg2.connect(dsn=args['dsn'])

        # Patch JJ 2016-05-05: This is the moment to set the correct
        # transaction isolation level, encoding, and types.
        conn.set_session(isolation_level=int(args['tilevel']),
                         readonly=bool(args['readonlymode']))
        if 'encoding' in args:
            conn.set_client_encoding(args['encoding'])
        if 'typecasts' in args:
            for tc in args['typecasts']:
                register_type(tc, conn)

        # The following code is identical to the code in
        # AbstractConnectionPool
        if key is not None:
            self._used[key] = conn
            self._rused[id(conn)] = key
        else:
            self._pool.append(conn)
        return conn

    def _getkey(self):
        """Return the thread identifier as a key."""
        return get_thread_id()

    def _putconn(self, conn, key=None, close=False):
        """Patched version of AbstractConnectionPool._putconn(), which closes
        the connection only if 'close' is set, not if pool size is greater
        than minimum or 'close' is set."""
        stored_minconn = self.minconn
        self.minconn = 10  # magic number: number of connections to keep
        retval = AbstractConnectionPool._putconn(
            self, conn=conn, key=key, close=close)
        self.minconn = stored_minconn
        return retval


# the DB object, managing all the real query work


class DB(TM, dbi_db.DB):

    _p_oid = _p_changed = _registered = None

    def __init__(self, dsn, tilevel, typecasts, enc='utf-8',
                 autocommit=False, readonlymode=False, physical_path='',
                 use_tpc=False):
        self.dsn = dsn
        self.tilevel = tilevel
        self.typecasts = typecasts
        if enc is None or enc == "":
            self.encoding = "utf-8"
        else:
            self.encoding = enc
        self.failures = 0
        self.calls = 0

        self.physical_path = physical_path

        # Patch JJ 2017-01-27: Add an autocommit feature which commits
        # every query in this instance
        self.autocommit = autocommit

        # Patch JJ 2017-09-26: Add read-only mode
        self.readonlymode = readonlymode

        # Patch JJ 2018-10-15: Use Two-Phase Commit
        self.use_tpc = use_tpc

        # Connectors with tainted transactions
        self.tainted = []

        self.make_mappings()

        self.pool = CustomConnectionPool(
            # 100 = sufficiently high magic number > max number of threads
            minconn=0, maxconn=100,
            dsn=self.dsn, tilevel=self.tilevel, typecasts=self.typecasts,
            readonlymode=self.readonlymode)

    def getconn(self, init=True):
        conn = self.pool.getconn()
        return conn

    def putconn(self, close=False):
        conn = self.pool.getconn()
        return self.pool.putconn(conn, close=close)

    def getcursor(self):
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
        except psycopg2.InterfaceError:
            # Connection is broken. Put away, then raise.
            conn = self.pool.getconn()
            self.pool.putconn(conn, close=True)
            raise
        return cursor

    def _commit(self, put_connection=False):
        conn = self.getconn(False)
        conn.commit()
        if put_connection:
            self.putconn()

    # Two-Phase Commit (TPC) is needed in order to be able to cope with
    # errors during "COMMIT".
    # dbapi and psycopg2 have TPC support.
    # The method should be included into the Zope Transaction Manager so:
    # - When calling _register(), also do a dbconn2.Connection.tpc_begin().
    # - implement commit() to perform the first phase (tpc_prepare())
    # - implement _finish() to perform the second phase (tpc_commit())
    def xid(self):
        '''generate a valid transaction ID for two-phase commit.'''
        conn = self.getconn(False)
        xid = conn.xid(1, str(get_thread_id()), self.physical_path)
        return xid

    def _begin(self):
        if self.use_tpc:
            conn = self.getconn(False)
            xid = self.xid()
            conn.tpc_begin(xid)

    def commit(self, *ignored):
        conn = self.getconn()
        if conn in self.tainted:
            return
        if self.use_tpc:
            try:
                conn = self.getconn(False)
                conn.tpc_prepare()
            except psycopg2.Error as error:
                self.handle_retry(error)
                raise error

    def _finish(self, *ignored):
        conn = self.getconn(False)
        if conn in self.tainted:
            self._abort()
            return
        if self.use_tpc:
            conn.tpc_commit()
        else:
            self._commit(put_connection=True)

    def _abort(self, *ignored):
        # In cases where the _abort() occurs because the connection to the
        # database failed, getconn() will fail also.
        try:
            conn = self.getconn(False)
        except psycopg2.Error:
            LOG.error('getconn() failed during abort.')
            return

        try:
            if self.use_tpc:
                # TODO An error can occur if this connector
                # has not yet started a transaction. Maybe
                # possible to fix using tpc_abort() rather than
                # abort().
                try:
                    conn.tpc_rollback()
                except psycopg2.ProgrammingError:
                    pass
            else:
                conn.rollback()
        except psycopg2.InterfaceError:
            LOG.error('Rollback failed, just closing connection.')
        if conn in self.tainted:
            self.tainted.remove(conn)
        self.putconn()

    def open(self):
        pass

    def close(self):
        pass

    def sortKey(self):
        return 1

    def make_mappings(self):
        """Generate the mappings used later by self.convert_description()."""
        self.type_mappings = {}
        for t, s in [(INTEGER, 'i'), (LONGINTEGER, 'i'), (NUMBER, 'n'),
                     (BOOLEAN, 'n'), (ROWID, 'i'),
                     (DATETIME, 'd'), (DATE, 'd'), (TIME, 'd')]:
            for v in t.values:
                self.type_mappings[v] = (t, s)

    def convert_description(self, desc, use_psycopg_types=False):
        """Convert DBAPI-2.0 description field to Zope format."""
        items = []
        for name, typ, width, ds, p, scale, null_ok in desc:
            m = self.type_mappings.get(typ, (STRING, 's'))
            items.append({
                'name': name,
                'type': use_psycopg_types and m[0] or m[1],
                'width': width,
                'precision': p,
                'scale': scale,
                'null': null_ok,
            })
        return items

    # tables and rows

    def tables(self, rdb=0, _care=('TABLE', 'VIEW')):
        self._register()
        c = self.getcursor()
        c.execute(
            "SELECT t.tablename AS NAME, 'TABLE' AS TYPE "
            "  FROM pg_tables t WHERE tableowner <> 'postgres' "
            "UNION SELECT v.viewname AS NAME, 'VIEW' AS TYPE "
            "  FROM pg_views v WHERE viewowner <> 'postgres' "
            "UNION SELECT t.tablename AS NAME, 'SYSTEM_TABLE\' AS TYPE "
            "  FROM pg_tables t WHERE tableowner = 'postgres' "
            "UNION SELECT v.viewname AS NAME, 'SYSTEM_TABLE' AS TYPE "
            "FROM pg_views v WHERE viewowner = 'postgres'")
        res = []
        for name, typ in c.fetchall():
            if typ in _care:
                res.append({'TABLE_NAME': name, 'TABLE_TYPE': typ})
        self.putconn()
        return res

    def columns(self, table_name):
        self._register()
        c = self.getcursor()
        try:
            c.execute('SELECT * FROM "%s" WHERE 1=0' % table_name)
        except:
            return ()
        self.putconn()
        return self.convert_description(c.description, True)

    def handle_retry(self, error):
        '''Find out if an error deserves a retry.'''
        name = error.__class__.__name__
        value = repr(error)
        serialization_error = (
            name == 'TransactionRollbackError' and
            'could not serialize' in value
        )
        if serialization_error:
            raise RetryError

        connection_closed_error = (
            name == 'OperationalError' and (
                'server closed the connection' in value or
                'could not connect to server' in value or
                'the database system is shutting down' in value or
                'the database system is starting up' in value or
                'terminating connection due to administrator command' in value
            )
        ) or (
            name == 'InterfaceError' and (
                'connection already closed' in value
            )
        ) or (
            name == 'NotSupportedError' and (
                'cannot set transaction read-write mode' in value
            )
        )
        if connection_closed_error:
            # Close all connections in the pool if there
            # was a connection problem.
            LOG.error("Operational error on connection, "
                      "closing all in this pool: %s." %
                      repr(self.pool))
            for key, conn in self.pool._used.items():
                self.pool.putconn(conn=conn, key=key, close=True)
            raise RetryDelayError

    # query execution

    def query(self, query_string, max_rows=None, query_data=None):
        try:
            self._register()
            self.calls = self.calls+1

            conn = self.getconn()
            if conn in self.tainted:
                raise ValueError("Query attempted on tainted transaction.")

            desc = ()
            res = []
            nselects = 0

            c = self.getcursor()

            for qs in [x for x in query_string.split('\0') if x]:
                # LOG.info("Trying to execute statement %s" % qs)
                if query_data:
                    c.execute(qs, query_data)
                else:
                    c.execute(qs)
                if self.autocommit:
                    # LOG.info('Autocommitting.')
                    self._commit()

                if c.description is not None:
                    nselects += 1
                    if c.description != desc and nselects > 1:
                        raise psycopg2.ProgrammingError(
                            'multiple selects in single query not allowed')
                    if max_rows:
                        res = c.fetchmany(max_rows)
                        # JJ 2017-07-20: Danger ahead. We might
                        # have many more rows in the database,
                        # which are truncated by max_rows. In that
                        # case, we should be able to react, by
                        # raising or logging.
                        if len(res) == max_rows:
                            try:
                                overshoot_result = c.fetchone()
                            except:
                                overshoot_result = None
                            if overshoot_result:
                                assert False, (
                                    "This query has returned more than "
                                    "max_rows results. Please raise "
                                    "max_rows or limit in SQL.")

                    else:
                        res = c.fetchall()
                    desc = c.description

            self.failures = 0

        except Exception as err:
            self.handle_retry(err)
            self._abort()

            # Taint this transaction
            conn = self.getconn()
            LOG.warning('query() tainting: {} in {}'.format(
                conn, self.tainted))
            if conn not in self.tainted:
                self.tainted.append(conn)
            raise err

        return self.convert_description(desc), res
