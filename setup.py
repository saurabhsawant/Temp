__author__ = 'jmettu'

import os
from setuptools import setup

setup(
    name='wario',
    version='1.0',
    description='workflow for IQ',
    url='http://github.services.ooyala.net/BigData/wario',
    packages=['wario', 'wario.lib'],
    entry_points={'console_scripts': ['wario=wario.cmdline:trigger_min15_dailyrollup']}
)




