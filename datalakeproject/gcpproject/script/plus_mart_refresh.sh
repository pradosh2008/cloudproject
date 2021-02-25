#!/usr/bin/bash

. ${HOME}/lib/set_env.sh
. ${HOME}/lib/common.sh

bq cp --force edl_conform.brand_customer analytic_mart.dim_brand_customer

bq cp --force edl_conform.brand_customer_account analytic_mart.dim_brand_customer_account

bq cp --force edl_conform.brand_customer_addr analytic_mart.dim_brand_customer_addr

bq cp --force edl_conform.brand_customer_email analytic_mart.dim_brand_customer_email

bq cp --force edl_conform.brand_customer_location analytic_mart.dim_brand_customer_location

bq cp --force edl_conform.brand_customer_membership analytic_mart.dim_brand_customer_membership

bq cp --force edl_conform.brand_customer_phone analytic_mart.dim_brand_customer_phone

bq cp --force edl_conform.store analytic_mart.dim_store

bq cp --force edl_conform.transaction_fee analytic_mart.fact_transaction_fee

bq cp --force edl_conform.transaction_notes analytic_mart.fact_transaction_notes

bq cp --force edl_conform.transaction_tax analytic_mart.fact_transaction_tax

bq cp --force edl_conform.transaction_tender analytic_mart.fact_transaction_tender

echo "dim_item.bq" >/data/ctl/dim_item.ctl

echo "fact_transaction.bq" >/data/ctl/fact_transaction.ctl

echo "fact_transaction_detail.bq" >/data/ctl/fact_transaction_detail.ctl

echo "fact_transaction_discount.bq" >/data/ctl/fact_transaction_discount.ctl
