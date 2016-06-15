#!/usr/bin/env bash
luigi_cfg_file="$PWD/wario/resources/luigi.cfg"
export LUIGI_CONFIG_PATH="$luigi_cfg_file"
echo -e "\n[core]\nlogging_conf_file:$PWD/wario/resources/log.cfg" >> $luigi_cfg_file

