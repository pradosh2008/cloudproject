#!/bin/bash
#################################################################################
#
# File Name: prm_caa_extracts.sh
#
# Description: Exports BEGIN_DT and END_DT for the AWS extracts
#            
#
# AUDIT TRAIL
# Modified By
# ================================================================
# Date         Person         Description
# -----------  -------------  ------------------------------------
# 10/30/2020   V.Kalathil		  Initial Script
# 
##################################################################################




##################################################################################
# Set up date variables
##################################################################################
   DATE=`date +"%Y-%m-%d"`
   TIME=`date +%T`


##################################################################################
#			     History   
# Set up BEGIN_DT & END_DT Manually for the specific Period to do Adhoc Pull.
# In case of last 7 or 4 days of data comment the BEGIN_DT & ENDT_DT and
# add the value in DAYS_DIFF variable.
###################################################################################

#export BEGIN_DT=2020-10-01
#export END_DT=2020-10-29


##################################################################################
#			     Incremental    
# For Incremental load just add the number of days in INCR_DAYS variable
# and comment the BEGIN_DT & END_DT Variable defined above.
# To pull the last n days of data give INCR_DAYS as n, where n ispositive integer.
##################################################################################

export DAYS_DIFF=4



##################################################################################
# Dont make any changes in this part. BIGIN_DT & ENDT_DT calcuation logic.
# 
##################################################################################


if [ "$BEGIN_DT" = "" ]  || [ "$END_DT" = "" ]
then 
  echo "Either of BEGIN_DT or END_DT is blank. In this case BEGIN_DT and END_DT will be current date."
  export END_DT=`date +%Y-%m-%d`
  export DAY_DIFF=`expr $DAYS_DIFF - 1`
  export BEGIN_DT=`date --date="${END_DT} -${DAY_DIFF} day" +%Y-%m-%d`
  echo "BEGIN_DT is $BEGIN_DT and END_DT is $END_DT. Data will be fetched between $BEGIN_DT and $END_DT."
else
  echo "BEGIN_DT is $BEGIN_DT and END_DT is $END_DT. Data will be fetched between $BEGIN_DT and $END_DT."
fi
echo "End of the script"