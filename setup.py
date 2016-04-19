from distutils.core import setup
from setuptools import setup, find_packages

tests_require = [
    'pytz',
    'oslo.utils',
    'oslo.db',
    'pandas',
    'pandasql',
    'redis',
    'redis-py-cluster',
    'redlock-py',
    'ujson',
    'numexpr',
    'sqlalchemy',
    'gevent'
    ]

setup(
    name='rome',
    version='0.1',
    tests_require=tests_require,
    test_suite="execute_tests",
    packages=['lib', 'lib.rome', 'lib.rome.core', 'lib.rome.core.orm', 'lib.rome.core.dataformat', 'lib.rome.driver', 'lib.rome.driver.redis', 'lib.rome.utils', 'lib.rome.conf', 'lib.rome.engine',
              'lib.rome.core.expression', 'lib.rome.core.rows', 'lib.rome.core.terms', 'lib.rome.core.session',
              'test', 'lib.rome.driver.cassandra'],
    url='https://github.com/badock/rome',
    license='',
    author='jonathan',
    author_email='',
    description='Relational Object Mapping Extension for key/value stores'
)
