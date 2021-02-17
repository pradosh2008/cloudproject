echo "ann_aw_transactions_header.bq" >/${env_data_root}/ctl/tran_header_${$}-`date +%m%d%H%M%S`.ctl
echo "ann_aw_transactions_line.bq" >/${env_data_root}/ctl/tran_line_${$}-`date +%m%d%H%M%S`.ctl
echo "ann_aw_transactions_merchdtl.bq" >/${env_data_root}/ctl/tran_mech_${$}-`date +%m%d%H%M%S`.ctl
echo "ann_aw_transactions_discountdtl.bq" >/${env_data_root}/ctl/tran_disc_${$}-`date +%m%d%H%M%S`.ctl
echo "ann_aw_transactions_linenotes.bq" >/${env_data_root}/ctl/tran_disc_notes_${$}-`date +%m%d%H%M%S`.ctl
echo "ann_aw_transactions_returndtl.bq" >/${env_data_root}/ctl/tran_disc_return_${$}-`date +%m%d%H%M%S`.ctl
echo "ann_aw_transactions_taxdetail.bq" >/${env_data_root}/ctl/tran_disc_tax_${$}-`date +%m%d%H%M%S`.ctl
