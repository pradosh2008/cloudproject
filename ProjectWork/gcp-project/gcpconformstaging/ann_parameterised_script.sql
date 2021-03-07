set hivevar:days=${p1};
set hivevar:dateval=${p1};

INSERT OVERWRITE TABLE ascena_analytic_mart.ann_ann_atg_transactions
SELECT orderid
, profileid
, orderstate
, createdbyorder
, originoforder
, createddate
, submitteddate
, lastmodifieddate
, completeddate
, agentid
, saleschannel
, siteid
, isassociateuser
, associatenumber
, ipaddress
, couponcode
,mask(useremailaddress) as useremailaddress
, international
, storeassoclogin
, storeassocstoreid
, storeassocreason
, itempriceoverridden
, shippriceoverridden
, partnerid
, priceinfotype
, rawsubtotal
, orderpriceinfotax
, shipping
, priceinfocurrencycode
, priceinfoamount
, priceinfodiscounted
, priceinfoamountisfinal
, priceinfofullprice
, priceinfononfullprice
, commerceitemid
, commerceitemtype
, producttype
, catalogrefid
, commerceitemquantity
, finalsale
, giftcardstatus
, message
, quantitydiscounted
, saleprice
, listprice
, onsale
, rawtotalprice
, itempriceinfoorderdiscountshare
, detaileditempriceinfoquantity
, detaileditempriceinfoorderdiscountshare
, detaileditempriceinfotax
, ordermanualadjustmentshare
, commerceitempriceinfoamount
, commerceitempriceinfodiscounted
, commerceitempriceinfoamountisfinal
, commerceitempriceinfofullprice
, commerceitempriceinfononfullprice
, adjustmentdescription
, totaladjustment
, quantityadjusted
, catalogid
, catalogkey
, productid
, commerceitemstate
, commerceitemstatedetail
, order_ref
, lastmodified
, maxsvscalls
, giftboxed
, messageto
, messagefrom
, commerceitempartnerid
, paymentgroupid
, encryptedcreditcardnumber
, mask(creditcardtype) as creditcardtype
, mask(digitalwalletid) as digitalwalletid
, mask(digitalwallettype) as digitalwallettype
, mask(creditcardbillingaddressprefix) as creditcardbillingaddressprefix
, mask(creditcardbillingaddressfirstname) as creditcardbillingaddressfirstname
, mask(creditcardbillingaddressmiddlename) as creditcardbillingaddressmiddlename
, mask(creditcardbillingaddresslastname) as creditcardbillingaddresslastname
, mask(creditcardbillingaddresssuffix) as creditcardbillingaddresssuffix
, mask(creditcardbillingaddresscompanyname) as creditcardbillingaddresscompanyname
, mask(creditcardbillingaddressjobtitle) as creditcardbillingaddressjobtitle
, mask(creditcardbillingaddresspobox) as creditcardbillingaddresspobox
, mask(creditcardbillingaddressaddress1) as creditcardbillingaddressaddress1
, mask(creditcardbillingaddressaddress2) as creditcardbillingaddressaddress2
, mask(creditcardbillingaddressaddress3) as creditcardbillingaddressaddress3
, mask(creditcardbillingaddresscity) as creditcardbillingaddresscity
, mask(creditcardbillingaddresscountry) as creditcardbillingaddresscountry
, mask(creditcardbillingaddresspostalcode) as creditcardbillingaddresspostalcode
, mask(creditcardbillingaddressemail) as creditcardbillingaddressemail
, mask(creditcardbillingaddressphonenumber) as creditcardbillingaddressphonenumber
, mask(creditcardbillingaddressstateaddress)  as creditcardbillingaddressstateaddress
, mask(creditcardbillingaddresscounty) as creditcardbillingaddresscounty
, mask(creditcardbillingaddressfaxnumber) as creditcardbillingaddressfaxnumber
, mask(creditcardnumber)
, mask(expirationmonth)
, mask(expirationdayofmonth)
, mask(expirationyear)
, amountcredited
, paymentgroupamount
, paymentmethod
, paymentgroupsubmitteddate
, paymentgroupcurrencycode
, paymentgroupstate
, paymentgroupclasstype
, amountauthorized
, amountdebited
, paymentgroupstatedetail
, paymentgroupspecialinstructions
, authorizationstatus
, debitstatus
, creditstatus
, preauthcompletecalls
, tendertype
, tendersubtype
, payerid
, paypaltransactionid
, mask(paypalbillingaddressprefix) as paypalbillingaddressprefix
, mask(paypalbillingaddressfirstname) as paypalbillingaddressfirstname
, mask(paypalbillingaddressmiddlename) as paypalbillingaddressmiddlename
, mask(paypalbillingaddresslastname) as paypalbillingaddresslastname
, mask(paypalbillingaddresssuffix) as paypalbillingaddresssuffix
, mask(paypalbillingaddresscompanyname) as paypalbillingaddresscompanyname
, mask(paypalbillingaddressjobtitle) as paypalbillingaddressjobtitle
, mask(paypalbillingaddresspobox) as paypalbillingaddresspobox
, mask(paypalbillingaddressaddress1) as paypalbillingaddressaddress1
, mask(paypalbillingaddressaddress2) as paypalbillingaddressaddress2
, mask(paypalbillingaddressaddress3) as paypalbillingaddressaddress3
, mask(paypalbillingaddresscity) as paypalbillingaddresscity
, mask(paypalbillingaddresscountry) as paypalbillingaddresscountry
, mask(paypalbillingaddresspostalcode) as paypalbillingaddresspostalcode
, mask(paypalbillingaddressemail) as paypalbillingaddressemail
, mask(paypalbillingaddressphonenumber) as paypalbillingaddressphonenumber
, mask(paypalbillingaddressstateaddress) as paypalbillingaddressstateaddress
, mask(paypalbillingaddresscounty) as paypalbillingaddresscounty
, mask(paypalbillingaddressfaxnumber) as paypalbillingaddressfaxnumber
, giftcardtransactionid
, giftcertificateprofileid
, storecreditprofileid
, storecreditnumber
, shippinggroupid
, shippingmethod
, description
, shipondate
, actualshipdate
, shippinggroupstate
, shippinggroupstatedetail
, shippinggroupsubmitteddate
, rawshipping
, shippingtax
, order_no
, shippinggroupspecialinstructions
, shippinggrouptype
, hardgoodshippinggrouptrackingnumber
, pickuptype
, storenum
, mask(shippingaddressprefix) as shippingaddressprefix
, mask(shippingaddressfirstname) as shippingaddressfirstname
, mask(shippingaddressmiddlename) as shippingaddressmiddlename
, mask(shippingaddresslastname) as shippingaddresslastname
, mask(shippingaddresssuffix) as shippingaddresssuffix
, mask(shippingaddresscompanyname) as shippingaddresscompanyname
, mask(shippingaddressjobtitle) as shippingaddressjobtitle
, mask(shippingaddresspobox) as shippingaddresspobox
, mask(shippingaddressaddress1) as shippingaddressaddress1
, mask(shippingaddressaddress2) as shippingaddressaddress2
, mask(shippingaddressaddress3) as shippingaddressaddress3
, mask(shippingaddresscity) as shippingaddresscity
, mask(shippingaddressstateaddress) as shippingaddressstateaddress
, mask(shippingaddresscountry) as shippingaddresscountry
, mask(shippingaddresspostalcode) as shippingaddresspostalcode
, mask(shippingaddressemail) as shippingaddressemail
, mask(shippingaddressphonenumber) as shippingaddressphonenumber
, mask(shippingaddresscounty) as shippingaddresscounty
, mask(shippingaddressfaxnumber) as shippingaddressfaxnumber
, electronicshippinggroupemail
, handlinginstructionstype
, giftlistid
, giftlistitemid
, handlingmethod
, handlinginstructionsshippinggroupid
, handlinginstructionscommerceitemid
, handlinginstructionsquantity
, relationshiptype
, shipitemrelshippinggroupid
, shipitemrelcommerceitemid
, shipitemrelquantity
, returnedquantity
, relationshipshipitemrelamount
, shipitemrelstate
, shipitemrelstatedetail
, lowbound
, highbound
, shortshippedquantity
, egcnumber
, shipitemrelid
, trackingdetailtrackingnumber
, shippingcarrier
, shipmentdate
, fullfilledlocation
, shippedquantity
, payorderrelpaymentgroupid
, orderref
, relationshippayorderrelamount
, commerceitemcouponscouponcode
, commerceitemcouponsdiscountamt
, loyaltyid
, mask_identifier
, batch_id
FROM ascena_staging.ann_ann_atg_transactions
WHERE to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) > date_sub(current_date,${days})
;


where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('${dateval}' as string),'yyyy-MM-dd')),'yyyy-MM-dd')
;

where to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) >= date_format(from_unixtime(unix_timestamp(cast('${dateval}' as string),'yyyy-MM-dd')),'yyyy-MM-dd')

WHERE to_date(from_unixtime(unix_timestamp(cast(batch_id as string),'yyyyMMdd'), 'yyyy-MM-dd')) > date_sub(current_date,${days})

set hivevar:days=${p1};
