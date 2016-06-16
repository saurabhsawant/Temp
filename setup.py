__author__ = 'jmettu'

from setuptools import setup

install_requires = [
    'luigi==2.1.1',
    'pytz',
    'requests',
    'coverage',
    'python-dateutil'
    'django'
    'djangorestframework'
    'mock'
]

setup(
    name='wario',
    version='1.0',
    description='workflow for IQ',
    url='http://github.services.ooyala.net/BigData/wario',
    packages=['wario', 'wario.lib'],
    package_data={'': ['resources/*.json']},
    include_package_data=True,
    install_requires=install_requires,
    entry_points={'console_scripts': ['wario=wario.cmdline:main']}
)
