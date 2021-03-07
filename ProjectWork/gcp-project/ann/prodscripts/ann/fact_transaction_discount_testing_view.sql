--HISTORICAL
--This query proves that, for all historical sale records have discount_amt as -ve. Only 5 lines have -ve discount_amt for returns
SELECT d.return_ind, count(*)
FROM `p-asna-analytics-002.analytic_mart_dev.fact_transaction_discount` dis
LEFT OUTER JOIN `p-asna-analytics-002.analytic_mart_dev.fact_transaction_detail` d ON d.transaction_key = dis.transaction_key AND d.transaction_line_num = dis.transaction_line_num
WHERE discount_amt < 0
AND dis.transaction_dt <= '2019-03-28'
GROUP BY 1

--HISTORICAL
--This query proves that, for all historical return records have discount_amt as +ve. Only 69 lines have +ve discount_amt for sales
SELECT d.return_ind, count(*)
FROM `p-asna-analytics-002.analytic_mart_dev.fact_transaction_discount` dis
LEFT OUTER JOIN `p-asna-analytics-002.analytic_mart_dev.fact_transaction_detail` d ON d.transaction_key = dis.transaction_key AND d.transaction_line_num = dis.transaction_line_num
WHERE discount_amt > 0
AND dis.transaction_dt <= '2019-03-28'
GROUP BY 1

--INCREMENTAL
--No Incremental records for SALE/RETURN have negative discount_amt
SELECT d.return_ind, count(*)
FROM `p-asna-analytics-002.analytic_mart_dev.fact_transaction_discount` dis
LEFT OUTER JOIN `p-asna-analytics-002.analytic_mart_dev.fact_transaction_detail` d ON d.transaction_key = dis.transaction_key AND d.transaction_line_num = dis.transaction_line_num
WHERE discount_amt < 0
AND dis.transaction_dt > '2019-03-28'
GROUP BY 1

