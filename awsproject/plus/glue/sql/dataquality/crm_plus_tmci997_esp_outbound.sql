select count(*) as cnt from (SELECT count(*) FROM plus_tmci997_esp_outbound GROUP BY plus_tmci997_esp_outbound_key,extract_ts HAVING count(*) >1)
select count(*) as cnt from (SELECT count(*) FROM plus_tmci997_esp_outbound GROUP BY  row_hashed_val HAVING count(*)>1)