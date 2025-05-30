# ZPsycopgDA/DA.py - ZPsycopgDA Zope product: Database Connection
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


import time
import re

import Acquisition
import Shared.DC.ZRDB.Connection

# Python2 backward compatibility
try:
    from .db import DB
except SyntaxError:
    from db import DB

from App.special_dtml import HTMLFile
from ExtensionClass import Base
from DateTime import DateTime

# ImageFile is deprecated in Zope >= 2.9
try:
    from App.ImageFile import ImageFile
except ImportError:
    # Zope < 2.9.  If PIL's installed with a .pth file, we're probably
    # hosed.
    from ImageFile import ImageFile

# import psycopg and functions/singletons needed for date/time conversions

import psycopg2
from psycopg2 import NUMBER, STRING, ROWID, DATETIME
from psycopg2.extensions import INTEGER, FLOAT, BOOLEAN, DATE
from psycopg2.extensions import TIME, INTERVAL
from psycopg2.extensions import new_type

import psycopg2.extensions
DEFAULT_TILEVEL = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ

# add a new connection to a folder

manage_addZPsycopgConnectionForm = HTMLFile('dtml/add', globals())


def manage_addZPsycopgConnection(
        self, id, title, connection_string,
        zdatetime=None, tilevel=DEFAULT_TILEVEL, encoding='', check=None,
        autocommit=None, readonlymode=None, use_tpc=False,
        datetime_str=None,
        REQUEST=None):
    """Add a DB connection to a folder."""
    self._setObject(id, Connection(
        id=id,
        title=title,
        connection_string=connection_string,
        zdatetime=zdatetime,
        check=check,
        tilevel=tilevel,
        encoding=encoding,
        autocommit=autocommit,
        readonlymode=readonlymode,
        use_tpc=use_tpc,
        datetime_str=datetime_str,
    ))
    if REQUEST is not None:
        return self.manage_main(self, REQUEST)


# the connection object

class Connection(Shared.DC.ZRDB.Connection.Connection):
    """ZPsycopg Connection."""
    _isAnSQLConnection = 1

    id = 'Psycopg2_database_connection'
    database_type = 'Psycopg2'
    meta_type = title = 'Z Psycopg 2 Database Connection'
    icon = 'misc_/conn'
    zmi_icon = 'fas fa-database text-info'
    zmi_show_add_dialog = True

    def __init__(
            self, id, title, connection_string,
            zdatetime, check=None, tilevel=DEFAULT_TILEVEL,
            encoding='UTF-8',
            autocommit=False, readonlymode=False,
            use_tpc=False, datetime_str=False):
        self.zdatetime = zdatetime
        self.id = str(id)
        self.edit(
            title=title,
            connection_string=connection_string,
            zdatetime=zdatetime,
            check=check,
            tilevel=tilevel,
            encoding=encoding,
            autocommit=autocommit,
            readonlymode=readonlymode,
            use_tpc=use_tpc,
            datetime_str=datetime_str,
        )

    def factory(self):
        return DB

    # connection parameters editing

    def edit(
            self, title, connection_string,
            zdatetime, check=None, tilevel=DEFAULT_TILEVEL, encoding='UTF-8',
            autocommit=False, readonlymode=False, use_tpc=False,
            datetime_str=False):
        self.title = title
        self.connection_string = connection_string
        self.zdatetime = zdatetime
        self.tilevel = tilevel
        self.encoding = encoding
        self.autocommit = autocommit
        self.readonlymode = readonlymode
        self.use_tpc = use_tpc
        self.datetime_str = datetime_str

        if check:
            self.connect(self.connection_string)

    manage_properties = HTMLFile('dtml/edit', globals())

    def manage_edit(
            self, title, connection_string,
            zdatetime=None, check=None, tilevel=DEFAULT_TILEVEL,
            encoding='UTF-8',
            autocommit=False,
            readonlymode=False,
            use_tpc=False,
            datetime_str=False,
            REQUEST=None):
        """Edit the DB connection."""
        self.edit(
            title=title,
            connection_string=connection_string,
            zdatetime=zdatetime,
            check=check,
            tilevel=tilevel,
            encoding=encoding,
            autocommit=autocommit,
            readonlymode=readonlymode,
            use_tpc=use_tpc,
            datetime_str=datetime_str,
        )
        if REQUEST is not None:
            msg = "Connection edited."
            return self.manage_main(self, REQUEST, manage_tabs_message=msg)

    def connect(self, s):
        try:
            self._v_database_connection.close()
        except:
            pass

        # check psycopg version and raise exception if does not match
        check_psycopg_version(psycopg2.__version__)

        self._v_connected = ''
        dbf = self.factory()

        # Safety catch for migrations from systems without autocommit
        # or readonly feature:
        try:
            self.autocommit
        except AttributeError:
            self.autocommit = False
        try:
            self.readonlymode
        except AttributeError:
            self.readonlymode = False

        try:
            self.use_tpc
        except AttributeError:
            self.use_tpc = False

        if len(self.getPhysicalPath()) == 1:
            # We do not have our physical path (yet)
            # But we need the path to uniquely identify the connection!
            # So, we raise a really ugly error here.
            assert False, "No physical path available yet"
        physical_path = ('/'.join(self.getPhysicalPath()))

        # TODO: let the psycopg exception propagate, or not?
        self._v_database_connection = dbf(
            self.connection_string, self.tilevel, self.get_type_casts(),
            self.encoding, self.autocommit, self.readonlymode, physical_path,
            self.use_tpc)
        self._v_database_connection.open()
        self._v_connected = DateTime()

        return self

    def get_type_casts(self):
        # note that in both cases order *is* important
        if getattr(self, 'datetime_str', False):
            return STRDATETIME, STRDATE, STRTIME, STRINTERVAL
        if self.zdatetime:
            return ZDATETIME, ZDATE, ZTIME, ZINTERVAL
        else:
            return DATETIME, DATE, TIME, INTERVAL

    # browsing and table/column management

    manage_options = Shared.DC.ZRDB.Connection.Connection.manage_options
    # + (
    #    {'label': 'Browse', 'action':'manage_browse'},)

    # manage_tables = HTMLFile('dtml/tables', globals())
    # manage_browse = HTMLFile('dtml/browse', globals())

    info = None

    def table_info(self):
        return self._v_database_connection.table_info()

    def __getitem__(self, name):
        if name == 'tableNamed':
            if not hasattr(self, '_v_tables'):
                self.tpValues()
            return self._v_tables.__of__(self)
        raise KeyError(name)

    def tpValues(self):
        res = []
        conn = self._v_database_connection
        for d in conn.tables(rdb=0):
            try:
                name = d['TABLE_NAME']
                b = TableBrowser()
                b.__name__ = name
                b._d = d
                b._c = conn
                try:
                    b.icon = table_icons[d['TABLE_TYPE']]
                except:
                    pass
                res.append(b)
            except:
                pass
        return res


def check_psycopg_version(version):
    """
    Check that the psycopg version used is compatible with the zope adpter.
    """
    try:
        m = re.match(r'\d+\.\d+(\.\d+)?', version.split(' ')[0])
        tver = tuple(map(int, m.group().split('.')))
    except:
        raise ImportError("failed to parse psycopg version %s" % version)

    if tver < (2, 4):
        raise ImportError("psycopg version %s is too old" % version)

    if tver in ((2, 4, 2), (2, 4, 3)):
        raise ImportError("psycopg version %s is known to be buggy" % version)


# database connection registration data

classes = (Connection,)

meta_types = ({'name': 'Z Psycopg 2 Database Connection',
               'action': 'manage_addZPsycopgConnectionForm'},)

folder_methods = {
    'manage_addZPsycopgConnection': manage_addZPsycopgConnection,
    'manage_addZPsycopgConnectionForm': manage_addZPsycopgConnectionForm}

__ac_permissions__ = (
    ('Add Z Psycopg Database Connections',
     ('manage_addZPsycopgConnectionForm', 'manage_addZPsycopgConnection')),)

# add icons

misc_ = {'conn': ImageFile('icons/DBAdapterFolder_icon.gif', globals())}

for icon in ('table', 'view', 'stable', 'what', 'field', 'text', 'bin',
             'int', 'float', 'date', 'time', 'datetime'):
    misc_[icon] = ImageFile('icons/%s.gif' % icon, globals())


# zope-specific psycopg typecasters

# convert an ISO timestamp string from postgres to a Zope DateTime object
def _cast_DateTime(iso, curs):
    if iso:
        if iso in ['-infinity', 'infinity']:
            return iso
        else:
            return DateTime(iso)


# convert an ISO date string from postgres to a Zope DateTime object
def _cast_Date(iso, curs):
    if iso:
        if iso in ['-infinity', 'infinity']:
            return iso
        else:
            return DateTime(iso)


# Convert a time string from postgres to a Zope DateTime object.
# NOTE: we set the day as today before feeding to DateTime so
# that it has the same DST settings.
def _cast_Time(iso, curs):
    if iso:
        if iso in ['-infinity', 'infinity']:
            return iso
        else:
            return DateTime(
                time.strftime('%Y-%m-%d %H:%M:%S',
                              time.localtime(time.time())[:3] +
                              time.strptime(iso[:8], "%H:%M:%S")[3:]))


# NOTE: we don't cast intervals anymore because they are passed
# untouched to Zope.
def _cast_Interval(iso, curs):
    return iso

def _cast_Str(iso, curs):
    return iso

ZDATETIME = new_type((1184, 1114), "ZDATETIME", _cast_DateTime)
ZINTERVAL = new_type((1186,), "ZINTERVAL", _cast_Interval)
ZDATE = new_type((1082,), "ZDATE", _cast_Date)
ZTIME = new_type((1083, 1266), "ZTIME", _cast_Time)

STRDATETIME = new_type((1184, 1114), "STRDATETIME", _cast_Str)
STRINTERVAL = new_type((1186,), "ZINTERVAL", _cast_Str)
STRDATE = new_type((1082,), "STRDATE", _cast_Str)
STRTIME = new_type((1083, 1266), "STRTIME", _cast_Str)


# table browsing helpers

class TableBrowserCollection(Acquisition.Implicit):
    pass


class Browser(Base):
    def __getattr__(self, name):
        try:
            return self._d[name]
        except KeyError:
            raise AttributeError(name)


class values:
    def len(self):
        return 1

    def __getitem__(self, i):
        try:
            return self._d[i]
        except AttributeError:
            pass
        self._d = self._f()
        return self._d[i]


class TableBrowser(Browser, Acquisition.Implicit):
    icon = 'what'
    Description = check = ''
    info = HTMLFile('table_info', globals())
    menu = HTMLFile('table_menu', globals())

    def tpValues(self):
        v = values()
        v._f = self.tpValues_
        return v

    def tpValues_(self):
        r = []
        tname = self.__name__
        for d in self._c.columns(tname):
            b = ColumnBrowser()
            b._d = d
            try:
                b.icon = field_icons[d['Type']]
            except AttributeError:
                pass
            b.TABLE_NAME = tname
            r.append(b)
        return r

    def tpId(self):
        return self._d['TABLE_NAME']

    def tpURL(self):
        return "Table/%s" % self._d['TABLE_NAME']

    def Name(self):
        return self._d['TABLE_NAME']

    def Type(self):
        return self._d['TABLE_TYPE']

    manage_designInput = HTMLFile('designInput', globals())

    @staticmethod
    def vartype(inVar):
        "Get a type name for a variable suitable for use with dtml-sqlvar"
        outVar = type(inVar)
        if outVar == 'str':
            outVar = 'string'
        return outVar

    def manage_buildInput(self, id, source, default, REQUEST=None):
        "Create a database method for an input form"
        args = []
        values = []
        names = []
        columns = self._columns
        for i in range(len(source)):
            s = source[i]
            if s == 'Null':
                continue
            c = columns[i]
            d = default[i]
            t = c['Type']
            n = c['Name']
            names.append(n)
            if s == 'Argument':
                values.append("<dtml-sqlvar %s type=%s>'" %
                              (n, self.vartype(t)))
                a = '%s%s' % (n, self.vartype(t).title())
                if d:
                    a = "%s=%s" % (a, d)
                args.append(a)
            elif s == 'Property':
                values.append("<dtml-sqlvar %s type=%s>'" %
                              (n, self.vartype(t)))
            else:
                if isinstance(t, (type(''), type(u''))):
                    if d.find("\'") >= 0:
                        d = "''".join(d.split("\'"))
                    values.append("'%s'" % d)
                elif d:
                    values.append(str(d))
                else:
                    raise ValueError(
                        'no default was given for <em>%s</em>' % n)


class ColumnBrowser(Browser):
    icon = 'field'

    def check(self):
        return ('\t<input type=checkbox name="%s.%s">' %
                (self.TABLE_NAME, self._d['Name']))

    def tpId(self):
        return self._d['Name']

    def tpURL(self):
        return "Column/%s" % self._d['Name']

    def Description(self):
        d = self._d
        if d['Scale']:
            return " %(Type)s(%(Precision)s,%(Scale)s) %(Nullable)s" % d
        else:
            return " %(Type)s(%(Precision)s) %(Nullable)s" % d


table_icons = {
    'TABLE': 'table',
    'VIEW': 'view',
    'SYSTEM_TABLE': 'stable',
}

field_icons = {
    NUMBER.name: 'i',
    STRING.name: 'text',
    DATETIME.name: 'date',
    INTEGER.name: 'int',
    FLOAT.name: 'float',
    BOOLEAN.name: 'bin',
    ROWID.name: 'int'
}
