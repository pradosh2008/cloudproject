CREATE TABLE  f_indiv_email_rltnshp_vw (                                               
     ccmp_brand_cd  varchar(10),                                                              
     brand_cd  varchar(100),                                                                   
     indiv_id  decimal(13,0),                                                            
     email_id  decimal(10,0),                                                            
     email_addr  varchar(100),                                                                 
     best_email_flg  varchar(100),                                                             
     best_indiv_flg  varchar(100),
	 CONSTRAINT f_indiv_email_rltnshp_pk PRIMARY KEY (ccmp_brand_cd,indiv_id,email_id);