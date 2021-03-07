set hive.exec.compress.output = true;
set hive.exec.compress.intermediate=true;
SET mapred.output.compression.type=BLOCK;


CREATE TABLE IF NOT EXISTS ascena_analytic_mart.brand_customer_addr (
 brand_customer_key                  string                
 ,addr_key                            string                
 ,line_1_addr                         string                
 ,line_2_addr                         string                
 ,line_3_addr                         string                
 ,city_nam                            string                
 ,state_nam                           string                
 ,postal_cd                           string                
 ,zip4_cd                             string                
 ,country_nam                         string                
 ,crm_global_addr_id                  string                
 ,county_nam                          string                
 ,crm_line_1_addr                     string                
 ,crm_line_2_addr                     string                
 ,crm_city_nam                        string                
 ,crm_vanity_city_nam                 string                
 ,crm_state_nam                       string                
 ,crm_postal_cd                       string                
 ,crm_zip4_cd                         string                
 ,crm_addr_remainder_des              string                
 ,primary_ind                         tinyint               
 ,household_key                       string                
 ,return_mail_ind                     tinyint               
 ,addr_type_cd                        string                
 ,addr_reliability_ind                tinyint               
 ,addr_rural_type_cd                  string                
 ,block_group_id                      string                
 ,census_state_cd                     string                
 ,census_county_cd                    string                
 ,census_block_cd                     string                
 ,census_tract_cd                     string                
 ,census_place_cd                     string                
 ,addr_source_type_cd                 string                
 ,addr_source_create_tm               timestamp             
 ,crm_do_not_mail_ind                 tinyint               
 ,crm_lacs_ind                        tinyint               
 ,crm_lacs_addr_type_cd               string                
 ,crm_lacs_certified_addr_ind         tinyint               
 ,crm_ncoa_move_dt                    date                  
 ,crm_ncoa_move_type_cd               string                
 ,crm_ncoa_link_footnote_cd           string                
 ,crm_pcoa_move_dt                    date                  
 ,crm_pac_action_cd                   string                
 ,crm_pac_footnote_cd                 string                
 ,crm_addr_source_cd                  string                
 ,crm_addr_urbanization_nam           string                
 ,crm_addr_dpbc_cd                    string                
 ,crm_addr_dpbc_checkdigit_num        smallint              
 ,crm_iso_country_cd                  string                
 ,crm_carrier_route_cd                string                
 ,crm_line_of_travel_cd               string                
 ,crm_line_of_travel_order_cd         string                
 ,crm_addr_rec_type_cd                string                
 ,crm_dpv_cmra_cd                     string                
 ,crm_dpv_ftnote                      string                
 ,crm_dpv_false_positive_ind          tinyint               
 ,crm_dpv_no_stats_cd                 string                
 ,crm_dpv_status_cd                   string                
 ,crm_dpv_vacant_ind                  tinyint               
 ,crm_foreign_addr_ind                tinyint               
 ,crm_zip5_match_ind                  tinyint               
 ,crm_zip9_match_ind                  tinyint               
 ,crm_addr_unsuitable_for_mail_ind    tinyint               
 ,crm_zip_move_ind                    tinyint               
 ,crm_zip_type_cd                     string                
 ,crm_congress_district_num           int                   
 ,crm_fips_county_cd                  string                
 ,crm_fips_county_name                string                
 ,crm_facility_type_cd                string                
 ,crm_fips_cd                         string                
 ,crm_addr_error_cd                   string                
 ,crm_addr_status_cd                  string                
 ,crm_addr_geo_match_cd               string                
 ,crm_latiitude_num                   float                 
 ,crm_longitude_num                   float                 
 ,crm_fips_ageo_place_cd              string                
 ,crm_geo_block_cd                    string                
 ,crm_ageo_mcd_cd                     string                
 ,crm_cgeo_cbsa_cd                    string                
 ,crm_cgeo_msa_cd                     string                
 ,crm_ap_lacs_cd                      string                
 ,crm_dsf_business_ind                tinyint               
 ,crm_dsf_cmra_ind                    tinyint               
 ,crm_dsf_throwback_ind               tinyint               
 ,crm_dsf_seaonal_ind                 tinyint               
 ,crm_dsf_vacant_ind                  tinyint               
 ,crm_dsf_deliviery_type_cd           string                
 ,crm_dsf_curbside_delivery_ind       tinyint               
 ,crm_dsf_nbcbu_delivery_ind          tinyint               
 ,crm_dsf_central_delivery_ind        tinyint               
 ,crm_dsf_door_slot_delivery_ind      tinyint               
 ,crm_dsf_drop_count_num              int                   
 ,crm_dsf_lacs_ind                    tinyint               
 ,crm_dsf_no_stat_ind                 tinyint               
 ,crm_dsf_educational_seasonal_ind    tinyint               
 ,crm_dsf_record_type_cd              string                
 ,crm_dsf_mailability_score_cd        string                
 ,crm_prison_ind                      tinyint               
 ,crm_occupancy_score_num             smallint              
 ,crm_dwelling_type_cd                string                
 ,crm_nursing_home_ind                tinyint               
 ,edl_create_tms                      timestamp             
 ,edl_create_job_nam                  string                
 ,edl_last_update_tms                 timestamp             
 ,edl_last_update_job_nam             string                
 ,brand_cd                            string                           
)
STORED AS AVRO
TBLPROPERTIES("avro.output.codec"="snappy")
;


insert overwrite table ascena_analytic_mart.brand_customer_addr
select   brand_customer_key                
 ,addr_key                          
 ,mask(line_1_addr)  as   line_1_addr                 
 ,mask(line_2_addr)  as   line_2_addr                
 ,mask(line_3_addr)  as   line_3_addr                
 ,mask(city_nam)     as   city_nam             
 ,mask(state_nam)    as   state_nam               
 ,mask(postal_cd)    as   postal_cd                 
 ,mask(zip4_cd)      as   zip4_cd                
 ,mask(country_nam)  as   country_nam                
 ,crm_global_addr_id                
 ,mask(county_nam)   as   county_nam                    
 ,crm_line_1_addr                   
 ,crm_line_2_addr                   
 ,crm_city_nam                      
 ,crm_vanity_city_nam               
 ,crm_state_nam                     
 ,crm_postal_cd                     
 ,crm_zip4_cd                       
 ,crm_addr_remainder_des            
 ,primary_ind                       
 ,household_key                     
 ,return_mail_ind                   
 ,addr_type_cd                      
 ,addr_reliability_ind              
 ,addr_rural_type_cd                
 ,block_group_id                    
 ,census_state_cd                   
 ,census_county_cd                  
 ,census_block_cd                   
 ,census_tract_cd                   
 ,census_place_cd                   
 ,addr_source_type_cd               
 ,addr_source_create_tm             
 ,crm_do_not_mail_ind               
 ,crm_lacs_ind                      
 ,crm_lacs_addr_type_cd             
 ,crm_lacs_certified_addr_ind       
 ,crm_ncoa_move_dt                  
 ,crm_ncoa_move_type_cd             
 ,crm_ncoa_link_footnote_cd         
 ,crm_pcoa_move_dt                  
 ,crm_pac_action_cd                 
 ,crm_pac_footnote_cd               
 ,crm_addr_source_cd                
 ,crm_addr_urbanization_nam         
 ,crm_addr_dpbc_cd                  
 ,crm_addr_dpbc_checkdigit_num      
 ,crm_iso_country_cd                
 ,crm_carrier_route_cd              
 ,crm_line_of_travel_cd             
 ,crm_line_of_travel_order_cd       
 ,crm_addr_rec_type_cd              
 ,crm_dpv_cmra_cd                   
 ,crm_dpv_ftnote                    
 ,crm_dpv_false_positive_ind        
 ,crm_dpv_no_stats_cd               
 ,crm_dpv_status_cd                 
 ,crm_dpv_vacant_ind                
 ,crm_foreign_addr_ind              
 ,crm_zip5_match_ind                
 ,crm_zip9_match_ind                
 ,crm_addr_unsuitable_for_mail_ind  
 ,crm_zip_move_ind                  
 ,crm_zip_type_cd                   
 ,crm_congress_district_num         
 ,crm_fips_county_cd                
 ,crm_fips_county_name              
 ,crm_facility_type_cd              
 ,crm_fips_cd                       
 ,crm_addr_error_cd                 
 ,crm_addr_status_cd                
 ,crm_addr_geo_match_cd             
 ,crm_latiitude_num                 
 ,crm_longitude_num                 
 ,crm_fips_ageo_place_cd            
 ,crm_geo_block_cd                  
 ,crm_ageo_mcd_cd                   
 ,crm_cgeo_cbsa_cd                  
 ,crm_cgeo_msa_cd                   
 ,crm_ap_lacs_cd                    
 ,crm_dsf_business_ind              
 ,crm_dsf_cmra_ind                  
 ,crm_dsf_throwback_ind             
 ,crm_dsf_seaonal_ind               
 ,crm_dsf_vacant_ind                
 ,crm_dsf_deliviery_type_cd         
 ,crm_dsf_curbside_delivery_ind     
 ,crm_dsf_nbcbu_delivery_ind        
 ,crm_dsf_central_delivery_ind      
 ,crm_dsf_door_slot_delivery_ind    
 ,crm_dsf_drop_count_num            
 ,crm_dsf_lacs_ind                  
 ,crm_dsf_no_stat_ind               
 ,crm_dsf_educational_seasonal_ind  
 ,crm_dsf_record_type_cd            
 ,crm_dsf_mailability_score_cd      
 ,crm_prison_ind                    
 ,crm_occupancy_score_num           
 ,crm_dwelling_type_cd              
 ,crm_nursing_home_ind              
 ,edl_create_tms                    
 ,edl_create_job_nam                
 ,edl_last_update_tms               
 ,edl_last_update_job_nam           
 ,brand_cd                                           
from ascena_conform.brand_customer_addr
where edl_last_update_tms > date_add(current_date,-3)
;