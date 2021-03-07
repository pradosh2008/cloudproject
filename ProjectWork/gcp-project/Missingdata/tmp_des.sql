--/*********************************************************************
--*
--* Script Name: tmp table creation for group class division department descriptions of items
--*
--**********************************************************************/

create table ascena_analytic_mart_dev.tmp_class_des  as
select class_des
,class_num
from(
select 
class_num,class_des
,row_number() over (partition by class_des order by last_update_tms desc ) as row_num
from ascena_analytic_mart_dev.DIM_ITEM) as A					
where row_num = 1;


create table ascena_analytic_mart_dev.tmp_DIVISION_des as
select DIVISION_DES
,DIVISION_CD
from(
select 
DIVISION_CD
,DIVISION_DES
,row_number() over (partition by DIVISION_DES order by last_update_tms desc ) as row_num
from ascena_analytic_mart_dev.DIM_ITEM) as A					
where row_num = 1
;

create table ascena_analytic_mart_dev.tmp_DEPARTMENT_DES as
select DEPARTMENT_DES
,DEPARTMENT_NUM
from(
select 
 DEPARTMENT_NUM
,DEPARTMENT_DES 
,row_number() over (partition by DEPARTMENT_DES order by last_update_tms desc ) as row_num
from ascena_analytic_mart_dev.DIM_ITEM) as A					
where row_num = 1
; 


create table ascena_analytic_mart_dev.tmp_group_des as
select GROUP_DES
,GROUP_NUM
from(
select 
GROUP_NUM
,GROUP_DES  
,row_number() over (partition by GROUP_DES order by last_update_tms desc ) as row_num
from ascena_analytic_mart_dev.DIM_ITEM) as A					
where row_num = 1
;
