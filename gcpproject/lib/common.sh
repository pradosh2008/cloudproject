rc_check () {
     local rc=$1
     local pstep=$2     
	 local severity=${3:-error}
     if [ "$rc" -ne 0 ]; then
        local error="${pstep} failed with return value: ${rc}"
        echo ${error}
        if [ -z "$ERROR_CD" ]; then
            export ERROR_CD="${severity}"
            export ERROR_MSG="${error}"			
        fi		
        if [ ${severity} = 'error' ]; then
            
			exit $rc
        fi
    else
        echo "$pstep succeeded"
    fi
	#echo "rc_check ERROR_CD" $ERROR_CD
	#echo "rc_check ERROR_MSG" $ERROR_MSG
} #rc_check

log_job_status () {
    local bqscript=$1
    local ctl=$2

    local current_dt=`date "+%Y-%m-%d"`
    local runseq=`date +"%Y%m%d%H%M%S.%3N"`
    local status="Success"

	#echo $status
    #echo "errormsg - "$errormsg	
    if [ ! -z "$ERROR_CD" ]; then				
        status="$ERROR_CD"		
    fi
	#echo $status
    #echo $errormsg	
#TODO:  Add numeric runseq column to status table
#       Use date +"%Y%m%d%H%M%S.%3N" to add seq num
    echo "{ \"runDate\": \"${current_dt}\" \
        ,\"runSeq\": \"${runseq}\" \
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

run_validation () {
    local sql=$1
    local vstep=$2

    local options=" --quiet --format=csv --use_legacy_sql=false"
    local query='bq query $options $sql | awk "NR>1"'
	#echo "query" $query	
    eval result=\$\($query\)
	#echo "inside validation" $result	
    rc_check $result "$vstep" $result

} #run_validation 

archive_bucket_files () {
    local bucket=$1
    local bucket_dir="${bucket%\/*}"

    echo "Moving ${bucket} files to ${bucket_dir}..."
    gsutil -m mv "${bucket}" "${bucket_dir}/arc/"
} #archive_bucket_files 
