#!/usr/bin/env bash
export LUIGI_CONFIG_PATH=$PWD/test/resources/WarioTest.cfg
coverage run -m unittest discover test -p *_test.py
coverage report --include=*wario* --omit=*test*
