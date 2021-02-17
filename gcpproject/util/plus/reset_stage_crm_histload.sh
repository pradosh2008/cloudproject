#!/usr/bin/bash			
			
. ${HOME}/lib/set_env.sh			
. ${HOME}/lib/common.sh			
			
			
archive_bucket_files 'gs://p-ascena-aadp-landing-01/plus/crm/*.txt*'			
			

																																	
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci067_cust_block20200409.txt.gz	gs://p-ascena-aadp-landing-01/plus/crm/             &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci067_cust_block2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/             &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci067_cust_block2020042*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/ &             																																				
																																		
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci105_chain_cust20200409.txt.gz	gs://p-ascena-aadp-landing-01/plus/crm/             &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci105_chain_cust2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/             &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci105_chain_cust2020042*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/             &

																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci172_chain_cust_email20200409.txt.gz			gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci172_chain_cust_email2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci172_chain_cust_email2020042*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/crm/ &																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci173_full_email_addr_status20200409.txt.gz	gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci173_full_email_addr_status2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci173_full_email_addr_status2020042*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci173_full_email_address20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci173_full_email_address2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci173_full_email_address2020042*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/crm/ &																																				
				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci396_curr_opt_in_out_full20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci396_curr_opt_in_out_full2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci396_curr_opt_in_out_full2020042*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/ &
