select count(*) as cnt from (SELECT count(*) FROM plus_store GROUP BY  plus_store_key,element_at(split(source_file_nam,'_'),1),batch_id HAVING count(*) >1)
select count(*) as cnt from (SELECT count(*) FROM plus_store GROUP BY  row_hashed_val HAVING count(*)>1)