 CREATE TABLE  f_ccmp_email_click_vw (                                               
     ccmp_msg_id  decimal(20,0),                                                      
     brand_cd  varchar(100),                                                                
     ccmp_brand_cd  varchar(100),                                                           
     email_id  decimal(10,0),                                                         
     click_id  decimal(20,0),                                                         
     click_dt  timestamp,                                                             
     click_time  decimal(6,0),                                                        
     link_id  decimal(11,0),                                                          
     link_name  varchar(100),                                                               
     ccmp_campaign_id  decimal(11,0),                                                 
     ip_address  varchar(100),                                                              
     ccmp_pk_id  decimal(20,0),
	 CONSTRAINT f_ccmp_email_click_pk PRIMARY KEY (click_id,email_id,ccmp_brand_cd)); 