run_bq_job -e plus_tmci997_esp_outbound.bq &
run_bq_job -e plus_vmci015_cust_ccard.bq &
run_bq_job -e plus_vmci019_customer.bq &
run_bq_job -e plus_vmci019_custonly.bq &
run_bq_job -e plus_vmci021_cust_phone.bq &
#run_bq_job -e plus_vmci067_cust_block.bq &
run_bq_job -e plus_vmci079_club_memb.bq &
run_bq_job -e plus_vmci105_chain_cust.bq &
run_bq_job -e plus_vmci172_chain_cust_email.bq &
run_bq_job -e plus_vmci173_full_email_addr_status.bq &
run_bq_job -e plus_vmci173_full_email_address.bq &
run_bq_job -e plus_vmci180_cust_demog.bq &
run_bq_job -e plus_vmci396_curr_opt_in_out_full.bq &
run_bq_job -e plus_vmci972_chain_cust_fullemail.bq &
run_bq_job -e plus_vmci995_esp_last_click_open.bq &
#added marketing tables to the flow : 07/08/2020 : karthikd
run_bq_job -e plus_vmci024_dir_mkt_cl.bq &
run_bq_job -e plus_vmci025_mailing.bq &
run_bq_job -e plus_vmci051_clcst_gp_h.bq
wait
