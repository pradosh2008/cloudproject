rc_check () {
    local rc=$1
    local pstep=$2
    local severity=${3:-error}

    if [ $rc -ne 0 ]; then
        local error="${pstep} failed with return value: ${rc}"
        echo ${error}
        if [ -z "$ERROR_IND" ]; then
            export ERROR_IND=true
            export ERROR_MSG=${error}
        fi
        if [ ${severity} = 'error' ]; then
            exit $rc
        fi
    else
        echo "$pstep succeeded"
    fi
} #rc_check

log_job_status () {
    local bqscript=$1
    local ctl=$2

    local current_dt=`date "+%Y-%m-%d"`
    local status="Success"
    if [ -n "$ERROR_IND" ]; then
        status="Error"
    fi
    echo "{ \"runDate\": \"${current_dt}\" \
        ,\"ctlFileName\": \"${ctl}\" \
        ,\"bqScriptName\": \"${bqscript}\" \
        ,\"logFileName\": \"${LOG_FILE}\" \
        ,\"runStatus\": \"${status}\" \
        ,\"errorMsg\": \"${ERROR_MSG}\" }" >>${LOG_FILE}.status
} #log_job_status

load_job_status () {
    local program=$1

    echo "Loading ${program} status into edl_stage.bq_job_status..."
    bq load --noreplace --source_format=NEWLINE_DELIMITED_JSON \
            edl_stage.bq_job_status \
            ${LOG_FILE}.status \
            ${HOME}/schema/edl_stage/bq_job_status.json 
} #load_job_status 

exec_log () {
    local log_name=$1
    local timestamp=`date +%Y%m%d%H%M%S`

    export LOG_FILE="${HOME}/log/${log_name}_${timestamp}.log"
    echo "Redirecting output to log file: ${LOG_FILE}"
    exec >"${LOG_FILE}" 2>&1
} #exec_log 

