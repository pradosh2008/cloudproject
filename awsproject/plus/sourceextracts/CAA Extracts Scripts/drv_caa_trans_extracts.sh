#!/bin/bash
######################################################################
#
# Script Name: drv_edw_trans_caa_extract.sh
#
# Description: Exports Sales information from EDW 
#            
#
# Parameters:Starting Step
#
# AUDIT TRAIL
# Modified By
# =====================================================================
# Date         Person         Description
# -----------  -------------  ------------------------------------
# 10/30/2020   V.Kalathil     Initial Script
######################################################################
. library.sh
. prm_caa_extracts.sh

STARTSTEP=00
JOBSTEP=00



######################################################################
# Make sure a parameters were passed in.
######################################################################
paramcheck "Please specify a job step and date range start and end"

######################################################################
# VERIFY THE VALUE PASSED IS NUMERIC
######################################################################
if [[ $1 = *[[:digit:]]* ]]; then
 STARTSTEP=$1
 echo --------------------------------------------------
 echo Running $0 at step: $STARTSTEP
 echo --------------------------------------------------
else
 echo "$1 is not numeric."
 exit 1 
fi


######################################################################
# Remove  files of previous day
######################################################################

#rm /var/opt/process/extracts/caa/edw/${BRAND}*


######################################################################
# Execute Load Scripts
######################################################################
runsteps 001 $1 "exp_caa_sales_transaction_header.fxp"
runsteps 002 $1 "exp_caa_sales_transaction_detail.fxp"
runsteps 003 $1 "exp_caa_sales_merch_department.fxp"
runsteps 004 $1 "exp_caa_sales_merch_class.fxp"
runsteps 005 $1 "exp_caa_store_address.fxp"
runsteps 006 $1 "exp_caa_sales_customer.fxp"

## execute this step when needing to force complete a job. It completes the load process tables,
## so they are ready to run next time.

runsteps 999 $1 "echo COMPLETE: drv_caa_trans_extract.sh"

echo -------------------------------------------------------------
echo PROCESSING COMPLETE!
echo -------------------------------------------------------------