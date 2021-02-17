. ${HOME}/lib/set_env.sh
. ${HOME}/lib/common.sh


#!/usr/bin/bash
#set -xv
USAGE="Usage: `basename $0` [-h] [-f filename] [-b bucketname]"

filename=$1
bucketname=$2

data_path="/data/pre/"
bucket_path_name="gs://p-asna-datasink-003/${bucketname}/"
archive_path="gs://p-asna-datasink-003/pre/archive"

# Parse command line options.
while getopts h:f:b: OPT; do
    case "$OPT" in
        h)
            echo $USAGE
            exit 0
            ;;
        f)  filename=$OPTARG
            ;;
        b)  bucketname=$OPTARG
            ;;            
        \?)
            # getopts issues an error message
            echo $USAGE >&2
            exit 1
            ;;
    esac
done

if [ ! -z "$filename" -a "$filename" != " " ]; then	
	echo "filename "$filename
	echo "bucketname "$bucketname
	echo "data_path "$data_path
	echo "bucket_path_name "$bucket_path_name

	echo "Copying in the bucket $bucket_path_name from $data_path$filename*"

	#gsutil mv $data_path$filename* $bucket_path_name
	rc_check $? "Copying in the bucket"

else
	echo "FileName or Bucket Name missing"	
	echo $USAGE
	exit 1	
fi




