CREATE TABLE  loft_f_brand_email_vw (                                               
     email_id  decimal(10,0),                                                         
     brand_cd  varchar(100),                                                                
     email_addr  varchar(100),                                                              
     first_nm  varchar(100),                                                                
     last_nm  varchar(100),                                                                 
     postal_cd  varchar(100),                                                               
     email_domain  varchar(100),                                                            
     valid_ind  decimal(1,0),                                                         
     corrected_ind  decimal(1,0),                                                     
     undeliverable_ind  decimal(1,0),                                                 
     undeliverable_dt  timestamp,                                                     
     nuclear_optout_ind  decimal(1,0),                                                
     primary_list_id  decimal(10,0),                                                  
     primary_optout_ind  decimal(1,0),                                                
     primary_sub_chg_dt  timestamp,                                                   
     primary_optout_reason_cd  varchar(100),                                                
     teacher_list_id  decimal(10,0),                                                  
     teacher_optout_ind  decimal(1,0),                                                
     teacher_sub_chg_dt  timestamp,                                                   
     teacer_optout_reason_cd  varchar(100),                                                 
     ca_list_id  decimal(10,0),                                                       
     ca_optout_ind  decimal(1,0),                                                     
     ca_sub_chg_dt  timestamp,                                                        
     ca_optout_reason_cd  varchar(100),                                                     
     livelove_list_id  decimal(10,0),                                                 
     livelove_optout_ind  decimal(1,0),                                               
     livelove_sub_chg_dt  timestamp,                                                  
     livelove_optout_reason_cd  varchar(100),                                               
     at_browse_remarket_dt  timestamp,                                                
     at_last_cat_browse  varchar(100),                                                      
     at_remarket_dt  timestamp,                                                       
     atl_freq_cap  decimal(10,0),                                                     
     ats_freq_cap  decimal(10,0),                                                     
     ats_international_ind  decimal(1,0),                                             
     birthday_3rd_party_dt  timestamp,                                                
     birthday_pref_dt  timestamp,                                                     
     can_at_dmail_dt  timestamp,                                                      
     can_loft_dmail_dt  timestamp,                                                    
     canada_at_origpref_dt  timestamp,                                                
     canada_loft_origpref_dt  timestamp,                                              
     customer_no  varchar(100),                                                             
     default_at_browse_dt  timestamp,                                                 
     default_loft_browse_dt  timestamp,                                               
     dresses_at_browse_dt  timestamp,                                                 
     dresses_loft_browse_dt  timestamp,                                               
     email_source  varchar(100),                                                            
     form_series  varchar(100),                                                             
     form_source_last_at  varchar(100),                                                     
     form_source_last_loft  varchar(100),                                                   
     form_source_orig_at  varchar(100),                                                     
     form_source_orig_loft  varchar(100),                                                   
     loft_browse_remarket_dt  timestamp,                                              
     loft_international_flag  decimal(1,0),                                           
     loft_last_cat_browse  varchar(100),                                                    
     loft_remarket_dt  timestamp,                                                     
     maternity_browse_dt  timestamp,                                                  
     maternity_dt  timestamp,                                                         
     maternity_due_dt  timestamp,                                                     
     maternity_loft_ind  decimal(1,0),                                                
     maternity_orig_pref_dt  timestamp,                                               
     maternity_pref_dt  timestamp,                                                    
     mobile_optin_ind  decimal(1,0),                                                  
     mobile_phone_nbr  varchar(100),                                                        
     newarrival_loft_browse_dt  timestamp,                                            
     newarrivals_at_browse_dt  timestamp,                                             
     pants_at_browse_dt  timestamp,                                                   
     pants_loft_browse_dt  timestamp,                                                 
     petite_at_browse_dt  timestamp,                                                  
     petite_at_ind  decimal(1,0),                                                     
     petite_at_pref_dt  timestamp,                                                    
     petite_loft_browse_dt  timestamp,                                                
     petite_loft_ind  decimal(1,0),                                                   
     petite_loft_pref_dt  timestamp,                                                  
     sale_at_browse_dt  timestamp,                                                    
     sale_loft_browse_dt  timestamp,                                                  
     shoes_at_browse_dt  timestamp,                                                   
     shoes_loft_browse_dt  timestamp,                                                 
     students_browse_dt  timestamp,                                                   
     students_grad_dt  timestamp,                                                     
     students_ind  decimal(1,0),                                                      
     students_pref_dt  timestamp,                                                     
     suits_at_browse_dt  timestamp,                                                   
     sweaters_at_browse_dt  timestamp,                                                
     sweaters_loft_browse_dt  timestamp,                                              
     swim_loft_browse_dt  timestamp,                                                  
     tall_at_browse_dt  timestamp,                                                    
     tall_at_ind  decimal(1,0),                                                       
     tall_at_pref_dt  timestamp,                                                      
     tall_loft_browse_dt  timestamp,                                                  
     tall_loft_ind  decimal(1,0),                                                     
     tall_loft_pref_dt  timestamp,                                                    
     teachers_browse_dt  timestamp,                                                   
     teachers_ind  decimal(1,0),                                                      
     teachers_level  varchar(100),                                                          
     teachers_orig_pref_dt  timestamp,                                                
     teachers_pref_dt  timestamp,                                                     
     teachers_role  varchar(100),                                                           
     tops_at_browse_dt  timestamp,                                                    
     tops_loft_browse_dt  timestamp,                                                  
     trans_t2p_at_first_dt  timestamp,                                                
     trans_t2p_at_last_dt  timestamp,                                                 
     trans_t2p_loft_first_dt  timestamp,                                              
     trans_t2p_loft_last_dt  timestamp,                                               
     unsub_optdown_at  varchar(100),                                                        
     unsub_optdown_loft  varchar(100),                                                      
     wedding_at_ind  decimal(1,0),                                                    
     wedding_browse_dt  timestamp,                                                    
     wedding_dt  timestamp,                                                           
     wedding_orig_pref_dt  timestamp,                                                 
     wedding_pref_dt  timestamp,                                                      
     wedding_role  varchar(100),                                                            
     wedding_source  varchar(100),
	 CONSTRAINT loft_f_brand_email_pk PRIMARY KEY (email_id));   