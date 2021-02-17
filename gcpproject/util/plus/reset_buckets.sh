#!/usr/bin/bash			
			
. ${HOME}/lib/set_env.sh			
. ${HOME}/lib/common.sh			
			
archive_bucket_files 'gs://p-ascena-aadp-landing-01/plus/edw/*.txt*'			
archive_bucket_files 'gs://p-ascena-aadp-landing-01/plus/item/*.txt*'			
archive_bucket_files 'gs://p-ascena-aadp-landing-01/plus/store/*.txt*'			
archive_bucket_files 'gs://p-ascena-aadp-landing-01/plus/crm/*.txt*'			
			
			
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_detail20200409.txt.gz   	gs://p-ascena-aadp-landing-01/plus/edw/	&
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_detail2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/	&
		
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_discount20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/edw/	&
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_discount2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/edw/	&
																																				
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_fee20200409.txt.gz			gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_fee2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_header20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_header2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_notes20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_notes2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_payment20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_payment2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/edw/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_tax20200409.txt.gz			gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_tax2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_tender20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_transaction_tender2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/ &

gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_customer20200409.txt.gz   				gs://p-ascena-aadp-landing-01/plus/edw/	&
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/ca_sales_customer2020041*.txt.gz'				gs://p-ascena-aadp-landing-01/plus/edw/	&

gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_detail20200409.txt.gz   	gs://p-ascena-aadp-landing-01/plus/edw/	&
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_detail2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/	&
		
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_discount20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/edw/	&
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_discount2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/edw/	&
																																				
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_fee20200409.txt.gz			gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_fee2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_header20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_header2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_notes20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_notes2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_payment20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_payment2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/edw/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_tax20200409.txt.gz			gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_tax2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_tender20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/edw/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_transaction_tender2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/edw/ &

gsutil cp	gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_customer20200409.txt.gz   				gs://p-ascena-aadp-landing-01/plus/edw/	&
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/edw/arc/lb_sales_customer2020041*.txt.gz'				gs://p-ascena-aadp-landing-01/plus/edw/	&

wait																																				
																																				
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/item/arc/CA_PRODUCT_20200409*.txt'			gs://p-ascena-aadp-landing-01/plus/item/            &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/item/arc/CA_PRODUCT_2020041*.txt'			gs://p-ascena-aadp-landing-01/plus/item/            &
																																				
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/item/arc/LB_PRODUCT_20200409*.txt'			gs://p-ascena-aadp-landing-01/plus/item/            &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/item/arc/LB_PRODUCT_2020041*.txt'			gs://p-ascena-aadp-landing-01/plus/item/            &
																																				
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/store/arc/CA_STORE_20200409*.txt' 			gs://p-ascena-aadp-landing-01/plus/store/	        &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/store/arc/CA_STORE_2020041*.txt' 			gs://p-ascena-aadp-landing-01/plus/store/           &
																																				
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/store/arc/LB_STORE_20200409*.txt' 			gs://p-ascena-aadp-landing-01/plus/store/		    &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/store/arc/LB_STORE_2020041*.txt' 			gs://p-ascena-aadp-landing-01/plus/store/           &
													
wait
													
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci015_cust_ccard20200409.txt.gz	gs://p-ascena-aadp-landing-01/plus/crm/             &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci015_cust_ccard2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/             &
																																				
																																
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci019_customer20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/crm/             &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci019_customer2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/             &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci019_custonly20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/crm/             &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci019_custonly2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/             &
																																				
wait

gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci021_cust_phone20200409.txt.gz	gs://p-ascena-aadp-landing-01/plus/crm/             &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci021_cust_phone2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/             &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci067_cust_block20200409.txt.gz	gs://p-ascena-aadp-landing-01/plus/crm/             &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci067_cust_block2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/             &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci079_club_memb20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/crm/             &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci079_club_memb2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/             &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci105_chain_cust20200409.txt.gz	gs://p-ascena-aadp-landing-01/plus/crm/             &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci105_chain_cust2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/             &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci172_chain_cust_email20200409.txt.gz			gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci172_chain_cust_email2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/crm/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci173_full_email_addr_status20200409.txt.gz	gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci173_full_email_addr_status2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci173_full_email_address20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci173_full_email_address2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/crm/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci180_cust_demog20200409.txt.gz				gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci180_cust_demog2020041*.txt.gz'				gs://p-ascena-aadp-landing-01/plus/crm/ &
			
wait			
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci396_curr_opt_in_out_full20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci396_curr_opt_in_out_full2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci972_chain_cust_fullemail20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci972_chain_cust_fullemail2020041*.txt.gz'	gs://p-ascena-aadp-landing-01/plus/crm/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci995_esp_last_click_open20200409.txt.gz		gs://p-ascena-aadp-landing-01/plus/crm/ &
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/vmci995_esp_last_click_open2020041*.txt.gz'		gs://p-ascena-aadp-landing-01/plus/crm/ &
																																				
gsutil cp	gs://p-ascena-aadp-landing-01/plus/crm/arc/arc/VMCI016_CCARD_TYPE_20200318.txt				gs://p-ascena-aadp-landing-01/plus/crm/ &
																																				
gsutil cp	'gs://p-ascena-aadp-landing-01/plus/crm/arc/tmci997_esp_outbound2020041*.txt.gz'			gs://p-ascena-aadp-landing-01/plus/crm/ &
			
			
			
