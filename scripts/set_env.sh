#!/usr/bin/env bash
luigi_cfg_file="$PWD/wario/resources/luigi.cfg"
log_cfg_file="$PWD/wario/resources/log.cfg"
log_file="$PWD/logs/wario.log"
session_files="file://$PWD/test/resources/session_files"

export LUIGI_CONFIG_PATH="$luigi_cfg_file"

if ! grep -q "$log_cfg_file" "$luigi_cfg_file"; then
    echo -e "\n[core]\nlogging_conf_file:$log_cfg_file" >> $luigi_cfg_file
fi

if ! grep -q "$log_file" "$log_cfg_file"; then
    echo -e "\n[handler_logfile]\nargs:['$log_file', 'a']" >> $log_cfg_file
fi

if ! grep -q "$session_files" "$luigi_cfg_file"; then
    echo -e "\n[InputSessionFile]\nhdfs_sessions:$session_files" >> $luigi_cfg_file
fi

sudo cp $PWD/wario/resources/WarioCmv.cfg   /etc/luigi/WarioCmv.cfg


