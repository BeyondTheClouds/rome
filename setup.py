from distutils.core import setup

setup(
    name='rome',
    version='0.1',
    packages=['lib', 'lib.rome', 'lib.rome.core', 'lib.rome.core.orm', 'lib.rome.core.dataformat', 'lib.rome.driver',
              'lib.rome.driver.riak', 'lib.rome.driver.redis', 'lib.rome.utils', 'lib.rome.conf', 'lib.rome.engine',
              'lib.rome.core.expression', 'lib.rome.core.rows',  'lib.rome.core.terms', 'test'],
    url='https://github.com/badock/rome',
    license='',
    author='jonathan',
    author_email='',
    description='Relational Object Mapping Extension for key/value stores'
)
