#!/bin/bash
######################################################################
#
# Script Name: drv_caa_crm_extracts.sh
#
# Description: Exports CRM Information from 
#            
#
# Parameters:Starting Step
#
# AUDIT TRAIL
# Modified By
# ================================================================
# Date         Person         Description
# -----------  -------------  ------------------------------------
# 10/30/2020   V.Kalathil 		  Initial Script
######################################################################
. library.sh
. prm_caa_extracts.sh

STARTSTEP=00
JOBSTEP=00



######################################################################
# Make sure a parameters were passed in.
######################################################################
#paramcheck "Please specify a job step and date range start and end"

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

#rm /var/opt/process/extracts/aws/caa/crm/${BRAND}*


######################################################################
# Execute Load Scripts
######################################################################
runsteps 001 $1 "exp_caa_vmci019_customer.fxp"
runsteps 002 $1 "exp_caa_vmci079_club_memb.fxp"
runsteps 003 $1 "exp_caa_vmci172_chain_cust_email.fxp"
runsteps 004 $1 "exp_caa_vmci180_cust_demog.fxp"
runsteps 005 $1 "exp_caa_vmci024_dir_mkt_cl.fxp"
runsteps 006 $1 "exp_caa_vmci051_clcst_gp_h.fxp"
runsteps 007 $1 "exp_caa_vmci173_full_email_address.fxp"
runsteps 008 $1 "exp_caa_vmci396_curr_opt_in_out_full.fxp"
runsteps 009 $1 "exp_caa_tmci997_esp_outbound.fxp"
runsteps 010 $1 "exp_caa_vmci015_cust_ccard.fxp"
runsteps 011 $1 "exp_caa_vmci016_ccard_type.fxp"
runsteps 012 $1 "exp_caa_vmci019_custonly.fxp"
runsteps 013 $1 "exp_caa_vmci105_chain_cust.fxp"
runsteps 014 $1 "exp_caa_vmci173_full_email_addr_status.fxp"
runsteps 015 $1 "exp_caa_vmci198_opt_in_out_type.fxp"
runsteps 016 $1 "exp_caa_vmci972_chain_cust_fullemail.fxp"
runsteps 017 $1 "exp_caa_vmci025_mailing.fxp"


## execute this step when needing to force complete a job. It completes the load process tables,
## so they are ready to run next time.

runsteps 999 $1 "echo COMPLETE: drv_caa_crm_extracts.sh"

echo -------------------------------------------------------------
echo PROCESSING COMPLETE!
echo -------------------------------------------------------------