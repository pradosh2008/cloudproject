#!/usr/bin/bash
#set -xv
USAGE="Usage: `basename $0` [-h] [-b bucketname] [-d databasename] [-t table_name]"

#. $HOME/lib/set_env
url_opt='default'
procCntrl='BOTH'

# Parse command line options.
while getopts h:b:d:t: OPT; do
    case "$OPT" in
        h)
            echo $USAGE
            exit 0
            ;;
        b)  bucketname=$OPTARG
            ;;
        d)  databasename=$OPTARG
            ;;
        t)  table_name=$OPTARG
            ;;
        \?)
            # getopts issues an error message
            echo $USAGE >&2
            exit 1
            ;;
    esac
done

bucket_path_name="gs://p-asna-datasink-003/${bucketname}/*"

bq load --replace=true --source_format=AVRO --use_avro_logical_types=true $databasename.$table_name $bucket_path_name
