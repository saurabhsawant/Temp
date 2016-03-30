__author__ = 'jmettu'

from setuptools import setup

install_requires = [
    'luigi==2.0.1',
]


setup(
    name='wario',
    version='1.0',
    description='workflow for IQ',
    url='http://github.services.ooyala.net/BigData/wario',
    packages=['wario', 'wario.lib'],
    package_data={'': ['utils/*.json']},
    include_package_data=True,
    entry_points={'console_scripts': ['wario=wario.cmdline:main']}
)
