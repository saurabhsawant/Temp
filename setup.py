__author__ = 'jmettu'

from setuptools import setup, find_packages

setup(
    name='wario',
    version='1.0',
    description='workflow for IQ',
    url='http://github.services.ooyala.net/BigData/wario',
    packages=find_packages('wario'),
    package_dir={'': 'wario'},
    package_data={'': ['utils/*.json']},
    include_package_data=True,
    entry_points={'console_scripts': ['wario=wario.cmdline:main']}
)
