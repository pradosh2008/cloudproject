unique_keys = ['ITEM', 'LOC']
columns = ["ITEM","LOC","ITEM_PARENT","ITEM_GRANDPARENT","LOC_TYPE","UNIT_RETAIL","REGULAR_UNIT_RETAIL",
           "MULTI_UNITS","MULTI_UNIT_RETAIL","MULTI_SELLING_UOM","SELLING_UNIT_RETAIL","SELLING_UOM","PROMO_RETAIL",
           "PROMO_SELLING_RETAIL","PROMO_SELLING_UOM","CLEAR_IND","TAXABLE_IND","LOCAL_ITEM_DESC","LOCAL_SHORT_DESC",
           "TI","HI","STORE_ORD_MULT","STATUS","STATUS_UPDATE_DATE","DAILY_WASTE_PCT","MEAS_OF_EACH","MEAS_OF_PRICE",
           "UOM_OF_PRICE","PRIMARY_VARIANT","PRIMARY_COST_PACK","PRIMARY_SUPP","PRIMARY_CNTRY","RECEIVE_AS_TYPE",
           "CREATE_DATETIME","LAST_UPDATE_DATETIME","LAST_UPDATE_ID","INBOUND_HANDLING_DAYS","SOURCE_METHOD",
           "SOURCE_WH","STORE_PRICE_IND","RPM_IND","UIN_TYPE","UIN_LABEL","CAPTURE_TIME","EXT_UIN_IND"]
drop_columns = ["ITEM_PARENT","ITEM_GRANDPARENT","LOC_TYPE","UNIT_RETAIL","REGULAR_UNIT_RETAIL",
                "MULTI_UNITS","MULTI_UNIT_RETAIL","MULTI_SELLING_UOM","SELLING_UNIT_RETAIL","SELLING_UOM","PROMO_RETAIL",
                "PROMO_SELLING_RETAIL","PROMO_SELLING_UOM","CLEAR_IND","TAXABLE_IND","LOCAL_ITEM_DESC","LOCAL_SHORT_DESC",
                "TI","HI","STORE_ORD_MULT","STATUS","STATUS_UPDATE_DATE","DAILY_WASTE_PCT","MEAS_OF_EACH","MEAS_OF_PRICE",
                "UOM_OF_PRICE","PRIMARY_VARIANT","PRIMARY_COST_PACK","PRIMARY_SUPP","PRIMARY_CNTRY","RECEIVE_AS_TYPE",
                "CREATE_DATETIME","LAST_UPDATE_DATETIME","LAST_UPDATE_ID","INBOUND_HANDLING_DAYS","SOURCE_METHOD",
                "SOURCE_WH","STORE_PRICE_IND","RPM_IND","UIN_TYPE","UIN_LABEL","CAPTURE_TIME","EXT_UIN_IND"]
unique_key1 = ["ID_EMAIL", "ID_CUST"]
col1 = ["ID_CHAIN", "ID_EMAIL", "ID_CUST", "AD_EMAIL_FULL", "CD_EMAIL_SRCE_INITIAL"]
drop1 = ["ID_CHAIN", "AD_EMAIL_FULL", "CD_EMAIL_SRCE_INITIAL"]
