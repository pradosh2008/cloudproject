#!/usr/bin/bash
. ${HOME}/lib/set_env.sh
. ${HOME}/lib/common.sh

##### Define local functions #####
#
run_job () {
    local script=$1
    local ctl=$2

    $HOME/script/${script}
    rc_check $? $script Warning
    mv $ctl ${arc_path}
    rc_check $? "Archive control file" Warning
    log_job_status $script $ctl
}
    
##### Main #####
#
pid=$$
program=`basename $0`
ctl_path=/data/ctl
arc_path=/data/arc
declare -a children

exec_log $program

#Check to see if prior run script is still up
echo "pid: $pid"
#echo "other_gscp: $other_gscp"
#ps -C $program
#other_gscp=`ps -fu $USER |egrep 'bash .*run_bq_job$' |grep -v $pid`
#echo "other_gscp: $other_gscp"
#if [ -n "$other_gscp" ]; then
if ps --no-headers -C $program |grep -v $pid; then
    echo "$program already running - shutting down..."
    exit 0
fi
i=0
ls ${ctl_path}/*.ctl 2>/dev/null |( while read ctl_file
    do
        script_name=`cat $ctl_file`
        run_job $script_name $ctl_file &
        children[${i}]=$!
        ((i++))
    done
    if [ ${#children[@]} -gt 0 ]; then
        date
        wait ${children[@]}
        load_job_status $program
        date
    fi
) 
if [ ! -s ${LOG_FILE} ]; then
    rm $LOG_FILE
fi
