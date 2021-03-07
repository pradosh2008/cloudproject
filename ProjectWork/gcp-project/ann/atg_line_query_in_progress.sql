select 
          ord.orderid as transaction_num
		  ,'ATG' as transaction_source_cd
          ,pre_store.brand_cd_des as brand_cd	
          -- line_id		  
          ,ord.siteid as store_key -- needs to be hashed
		  ,'1' as register_num
          ,EXTRACT(DATE FROM (submitteddate)) as transaction_dt  -- there is no order date
		  ,EXTRACT(TIME from submitteddate) as transaction_tms
		  ---, transaction_type_cd
		  ,ci.catalogrefid as sku
		  ,it.listprice as list_price_amt
		  ,commerceitemquantity as item_sold_qty
		  ,case when saleprice=0 then listprice else saleprice end as item_sold_amt
		  --item cogs amount
		  --item gross_amt
		  ,ca.totaladjustment_ItemDiscount as item_disc_amt
		  --item_giftcard_amt
		  --item_adj_cd
		  --item_adj_amt
          ,commerceitempriceinfoamount as item_net_amt
		  ,NULL as original_order_id
		  ,NULL as item_return_cd
		  ,NULL as item_return_dt
		  ,0 as 	item_return_qty
		  ,0 as 	item_return_amt
      FROM edl_stage.pre_atg_order_header ord
      join edl_stage.pre_atg_order_commerceItem ci
        on ord.orderid = ci.orderid
      join `edl_stage.pre_atg_order_commerceitemadjustments` ca
        on ca.orderid = ord.orderid
        and ca.commerceitemid  = ci.commerceitemid   
union distinct
select  
      ret.returnid as transaction_num
	  ,'ATG' as transaction_source_cd
      ,pre_store.brand_cd_des as brand_cd
	  ,line_id
      ,ord.siteid as store_key -- needs to be hashed
	  ,'1' as register_num
      ,EXTRACT( DATE FROM ret.createddate) as transaction_dt -- as there is no order date 
	  ,EXTRACT(TIME from ret.createddate) as transaction_tms
	  ,it.catalogrefid as sku
	  ,it.listprice as list_price_amt
	  , 0 as item_sold_qty
	  , 0 as item_sold_amt
	  --item cogs amount
	  --item gross_amt
      ,ca.totaladjustment_ItemDiscount as item_disc_amt --needs to be checked
	   --item_giftcard_amt
	   --item_adj_cd
	   --item_adj_amt
	   ,ord.orderid as original_order_id
	   ,'NA' as item_return_cd
	   ,EXTRACT( DATE FROM ret.createddate) as item_return_dt
       , quantitytoreturn as item_return_qty
       , refundamount as item_return_amt
      from edl_stage.pre_atg_order_return ret
      join edl_stage.pre_atg_order_commerceItem it
        on ret.orderid = it.orderid
        and ret.commerceitemid = it.commerceitemid
      join   edl_stage.pre_atg_order_header ord
       on ret.orderid = ord.orderid
          join `edl_stage.pre_atg_order_commerceitemadjustments` ca
       on ca.orderid = ord.orderid
       and ca.commerceitemid  = it.commerceitemid