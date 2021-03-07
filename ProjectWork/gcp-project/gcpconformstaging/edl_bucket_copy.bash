#COMPLETED LOADS#
gscp -d ascena_analytic_mart brand_customer_location>brand_customer_location.out 2>&1
gscp -d ascena_analytic_mart transaction>transaction.out 2>&1
gscp -d ascena_analytic_mart brand_customer_transaction_xref>brand_customer_transaction_xref.out 2>&1

#CUSTOMER TABLE LOADS#
gscp -d ascena_analytic_mart brand_customer>brand_customer.out 2>&1
gscp -d ascena_analytic_mart brand_customer_email>brand_customer_email.out 2>&1
gscp -d ascena_analytic_mart brand_customer_phone>brand_customer_phone.out 2>&1
gscp -d ascena_analytic_mart brand_customer_membership>brand_customer_membership.out 2>&1
gscp -d ascena_analytic_mart brand_customer_account>brand_customer_account.out 2>&1
gscp -d ascena_analytic_mart brand_customer_addr>brand_customer_addr.out 2>&1


#MISC TABLE LOADS#
gscp -d ascena_analytic_mart store>store.out 2>&1
gscp -d ascena_analytic_mart item>item.out 2>&1


#TRANSACTION TABLE LOADS#
gscp -d ascena_analytic_mart transaction_detail>transaction_detail.out 2>&1
gscp -d ascena_analytic_mart transaction_tender>transaction_tender.out 2>&1
gscp -d ascena_analytic_mart transaction_discount>transaction_discount.out 2>&1
gscp -d ascena_analytic_mart transaction_fee>transaction_fee.out 2>&1
gscp -d ascena_analytic_mart transaction_notes>transaction_notes.out 2>&1
gscp -d ascena_analytic_mart transaction_tax>transaction_tax.out 2>&1