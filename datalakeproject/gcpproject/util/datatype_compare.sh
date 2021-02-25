#!/usr/bin/bash

if [ ! -d ~/datatype_comp ]
then
     mkdir ~/datatype_comp
     chmod 755 ~/datatype_comp
fi

cd ~/datatype_comp/

if [ ! -d ./prod/ ]
then
     mkdir ./prod/
     chmod 755 ./prod/
fi

if [ ! -d ./non_prod/ ]
then
     mkdir ./non_prod/
     chmod 755 ./non_prod/
fi

##
rm -f ./non_prod/*.json
rm -f ./prod/*.json

v_dataset="edl_stage"

##Fetch json Schema for Non-Prod tables
v_project_id="p-ascn-da-aadp-001"

for f_name in `bq ls --project_id=${v_project_id} --dataset_id=${v_dataset}|grep TABLE|awk '{print $1}'`
do
    bq show --schema --format=prettyjson ${v_project_id}:${v_dataset}.${f_name} >~/datatype_comp/non_prod/${f_name}.json
done


##Fetch json Schema for Prod tables
v_project_id="p-asna-analytics-002"

for f_name in `bq ls --project_id=${v_project_id} --dataset_id=${v_dataset}|grep TABLE|awk '{print $1}'`
do
    bq show --schema --format=prettyjson ${v_project_id}:${v_dataset}.${f_name} >~/datatype_comp/prod/${f_name}.json
done

##Compare datatype
rm -f ~/datatype_comp/*.json_diff

for f_schema in `ls -ltr ./prod/*.json|rev|cut -f1 -d'/'|rev`
do
    if [ -f ./non_prod/${f_schema} ]
    then
       diff ./prod/${f_schema} ./non_prod/${f_schema} > ${f_schema}_diff
    fi
done

##Remove zero byte diff files
find . -size 0 -exec rm {} \;
