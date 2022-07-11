ZPsycopgDA -- psycopg2 Zope adapter
===================================

- `Project page`__
- `Download`__
- `Psycopg mailing list`__

.. __: https://github.com/psycopg/ZPsycopgDA
.. __: https://pypi.python.org/pypi/ZPsycopgDA/
.. __: http://mail.postgresql.org/mj/mj_wwwusr/domain=postgresql.org?func=lists-long-full&extra=psycopg


This is the PostgreSQL adapter for Zope 2 and 3 based on psycopg2__.

As of version 2.4.6, ZPsycopgDA has the same content of the ZPsycopgDA module
included in Psycopg 2.4.6. Future psycopg2 versions will likely not include
ZPsycopgDA, which should be installed separately.

.. __: http://initd.org/psycopg/


Prerequisites
-------------

ZPsycopgDA depends on the psycopg2 module version at least 2.4. Don't use
versions 2.4.2 or 2.4.3: they are not compatible with ZPsycopgDA. Install the
latest version available.


Installation
------------

Unfortunately I'm no Zope expert, so these installation instructions are quite
approximative. Please contact us if you want to improve them.

- Make sure to have your Zope ``lib`` directory in the ``$PYTHONPATH``. If
  some command fails with::

    [...]
    File "[...]/ZPsycopgDA/db.py", line 18, in <module>
      from Shared.DC.ZRDB.TM import TM
    ImportError: No module named Shared.DC.ZRDB.TM

  you are probably missing it.

- Download the ZPsycopgDA package, unpack it and copy the ``ZPsycopgDA``
  directory into the ``Products`` directory of your Zope instance.

- Alternatively run ``easy_install ZPsycopgDA`` or ``pip install ZPsycopgDA``,
  then symlink or copy the ``ZPsycopgDA`` directory from the installed
  location to the ``Products`` directory of your Zope instance.

- Another reason for errors like ``ModuleNotFoundError: No module named 'Shared.DC.ZRDB'``
  might be missing ``Products.ZSQLMethods``
