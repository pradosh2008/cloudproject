| CREATE TABLE `loft_f_abacus_data_overlay1_vw`(                                              |
|   `indiv_id` decimal(13,0),                                                                 |
|   `brand_cd` string,                                                                        |
|   `customer_information` string,                                                            |
|   `sequence_number` string,                                                                 |
|   `match_flag` string,                                                                      |
|   `duplicate_indicator` string,                                                             |
|   `reserved` string,                                                                        |
|   `o1_overall_buyrid_lf` decimal(1,0),                                                      |
|   `o1_overall_prts_lf` decimal(3,0),                                                        |
|   `o1_overall_txns_lf` decimal(3,0),                                                        |
|   `o1_overall_dols_lf` decimal(6,0),                                                        |
|   `o1_overall_recency` decimal(6,0),                                                        |
|   `o1_overall_prts_0_12m` decimal(3,0),                                                     |
|   `o1_overall_txns_0_12m` decimal(3,0),                                                     |
|   `o1_overall_dols_0_12m` decimal(6,0),                                                     |
|   `o1_overall_prts_13_24m` decimal(3,0),                                                    |
|   `o1_overall_txns_13_24m` decimal(3,0),                                                    |
|   `o1_overall_dols_13_24m` decimal(6,0),                                                    |
|   `m1_lg_sz_fem_app_buyrid_lf` decimal(1,0),                                                |
|   `m1_lg_sz_fem_app_prts_lf` decimal(3,0),                                                  |
|   `m1_lg_sz_fem_app_txns_lf` decimal(3,0),                                                  |
|   `m1_lg_sz_fem_app_dols_lf` decimal(6,0),                                                  |
|   `m1_lg_sz_fem_app_recency` decimal(6,0),                                                  |
|   `m1_lg_sz_fem_app_prts_0_12m` decimal(3,0),                                               |
|   `m1_lg_sz_fem_app_txns_0_12m` decimal(3,0),                                               |
|   `m1_lg_sz_fem_app_dols_0_12m` decimal(6,0),                                               |
|   `m1_lg_sz_fem_app_prts_13_24m` decimal(3,0),                                              |
|   `m1_lg_sz_fem_app_txns_13_24m` decimal(3,0),                                              |
|   `m1_lg_sz_fem_app_dols_13_24m` decimal(6,0),                                              |
|   `m2_lo_tk_fem_app_buyrid_lf` decimal(1,0),                                                |
|   `m2_lo_tk_fem_app_prts_lf` decimal(3,0),                                                  |
|   `m2_lo_tk_fem_app_txns_lf` decimal(3,0),                                                  |
|   `m2_lo_tk_fem_app_dols_lf` decimal(6,0),                                                  |
|   `m2_lo_tk_fem_app_recency` decimal(6,0),                                                  |
|   `m2_lo_tk_fem_app_prts_0_12m` decimal(3,0),                                               |
|   `m2_lo_tk_fem_app_txns_0_12m` decimal(3,0),                                               |
|   `m2_lo_tk_fem_app_dols_0_12m` decimal(6,0),                                               |
|   `m2_lo_tk_fem_app_prts_13_24m` decimal(3,0),                                              |
|   `m2_lo_tk_fem_app_txns_13_24m` decimal(3,0),                                              |
|   `m2_lo_tk_fem_app_dols_13_24m` decimal(6,0),                                              |
|   `m3_md_tk_fem_app_buyrid_lf` decimal(1,0),                                                |
|   `m3_md_tk_fem_app_prts_lf` decimal(3,0),                                                  |
|   `m3_md_tk_fem_app_txns_lf` decimal(3,0),                                                  |
|   `m3_md_tk_fem_app_dols_lf` decimal(6,0),                                                  |
|   `m3_md_tk_fem_app_recency` decimal(6,0),                                                  |
|   `m3_md_tk_fem_app_prts_0_12m` decimal(3,0),                                               |
|   `m3_md_tk_fem_app_txns_0_12m` decimal(3,0),                                               |
|   `m3_md_tk_fem_app_dols_0_12m` decimal(6,0),                                               |
|   `m3_md_tk_fem_app_prts_13_24m` decimal(3,0),                                              |
|   `m3_md_tk_fem_app_txns_13_24m` decimal(3,0),                                              |
|   `m3_md_tk_fem_app_dols_13_24m` decimal(6,0),                                              |
|   `m4_hi_tk_fem_app_buyrid_lf` decimal(1,0),                                                |
|   `m4_hi_tk_fem_app_prts_lf` decimal(3,0),                                                  |
|   `m4_hi_tk_fem_app_txns_lf` decimal(3,0),                                                  |
|   `m4_hi_tk_fem_app_dols_lf` decimal(6,0),                                                  |
|   `m4_hi_tk_fem_app_recency` decimal(6,0),                                                  |
|   `m4_hi_tk_fem_app_prts_0_12m` decimal(3,0),                                               |
|   `m4_hi_tk_fem_app_txns_0_12m` decimal(3,0),                                               |
|   `m4_hi_tk_fem_app_dols_0_12m` decimal(6,0),                                               |
|   `m4_hi_tk_fem_app_prts_13_24m` decimal(3,0),                                              |
|   `m4_hi_tk_fem_app_txns_13_24m` decimal(3,0),                                              |
|   `m4_hi_tk_fem_app_dols_13_24m` decimal(6,0),                                              |
|   `m5_lo_tk_mal_app_buyrid_lf` decimal(1,0),                                                |
|   `m5_lo_tk_mal_app_prts_lf` decimal(3,0),                                                  |
|   `m5_lo_tk_mal_app_txns_lf` decimal(3,0),                                                  |
|   `m5_lo_tk_mal_app_dols_lf` decimal(6,0),                                                  |
|   `m5_lo_tk_mal_app_recency` decimal(6,0),                                                  |
|   `m5_lo_tk_mal_app_prts_0_12m` decimal(3,0),                                               |
|   `m5_lo_tk_mal_app_txns_0_12m` decimal(3,0),                                               |
|   `m5_lo_tk_mal_app_dols_0_12m` decimal(6,0),                                               |
|   `m5_lo_tk_mal_app_prts_13_24m` decimal(3,0),                                              |
|   `m5_lo_tk_mal_app_txns_13_24m` decimal(3,0),                                              |
|   `m5_lo_tk_mal_app_dols_13_24m` decimal(6,0),                                              |
|   `m6_md_tk_mal_app_buyrid_lf` decimal(1,0),                                                |
|   `m6_md_tk_mal_app_prts_lf` decimal(3,0),                                                  |
|   `m6_md_tk_mal_app_txns_lf` decimal(3,0),                                                  |
|   `m6_md_tk_mal_app_dols_lf` decimal(6,0),                                                  |
|   `m6_md_tk_mal_app_recency` decimal(6,0),                                                  |
|   `m6_md_tk_mal_app_prts_0_12m` decimal(3,0),                                               |
|   `m6_md_tk_mal_app_txns_0_12m` decimal(3,0),                                               |
|   `m6_md_tk_mal_app_dols_0_12m` decimal(6,0),                                               |
|   `m6_md_tk_mal_app_prts_13_24m` decimal(3,0),                                              |
|   `m6_md_tk_mal_app_txns_13_24m` decimal(3,0),                                              |
|   `m6_md_tk_mal_app_dols_13_24m` decimal(6,0),                                              |
|   `m7_hi_tk_mal_app_buyrid_lf` decimal(1,0),                                                |
|   `m7_hi_tk_mal_app_prts_lf` decimal(3,0),                                                  |
|   `m7_hi_tk_mal_app_txns_lf` decimal(3,0),                                                  |
|   `m7_hi_tk_mal_app_dols_lf` decimal(6,0),                                                  |
|   `m7_hi_tk_mal_app_recency` decimal(6,0),                                                  |
|   `m7_hi_tk_mal_app_prts_0_12m` decimal(3,0),                                               |
|   `m7_hi_tk_mal_app_txns_0_12m` decimal(3,0),                                               |
|   `m7_hi_tk_mal_app_dols_0_12m` decimal(6,0),                                               |
|   `m7_hi_tk_mal_app_prts_13_24m` decimal(3,0),                                              |
|   `m7_hi_tk_mal_app_txns_13_24m` decimal(3,0),                                              |
|   `m7_hi_tk_mal_app_dols_13_24m` decimal(6,0),                                              |
|   `m8_lm_tk_m_fem_app_buyrid_lf` decimal(1,0),                                              |
|   `m8_lm_tk_m_fem_app_prts_lf` decimal(3,0),                                                |
|   `m8_lm_tk_m_fem_app_txns_lf` decimal(3,0),                                                |
|   `m8_lm_tk_m_fem_app_dols_lf` decimal(6,0),                                                |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `m8_lm_tk_m_fem_app_recency` decimal(6,0),                                                |
|   `m8_lm_tk_m_fem_app_prts_0_12m` decimal(3,0),                                             |
|   `m8_lm_tk_m_fem_app_txns_0_12m` decimal(3,0),                                             |
|   `m8_lm_tk_m_fem_app_dols_0_12m` decimal(6,0),                                             |
|   `m8_lm_tk_m_fem_app_prts_13_24m` decimal(3,0),                                            |
|   `m8_lm_tk_m_fem_app_txns_13_24m` decimal(3,0),                                            |
|   `m8_lm_tk_m_fem_app_dols_13_24m` decimal(6,0),                                            |
|   `m9_hi_tk_m_fem_app_buyrid_lf` decimal(1,0),                                              |
|   `m9_hi_tk_m_fem_app_prts_lf` decimal(3,0),                                                |
|   `m9_hi_tk_m_fem_app_txns_lf` decimal(3,0),                                                |
|   `m9_hi_tk_m_fem_app_dols_lf` decimal(6,0),                                                |
|   `m9_hi_tk_m_fem_app_recency` decimal(6,0),                                                |
|   `m9_hi_tk_m_fem_app_prts_0_12m` decimal(3,0),                                             |
|   `m9_hi_tk_m_fem_app_txns_0_12m` decimal(3,0),                                             |
|   `m9_hi_tk_m_fem_app_dols_0_12m` decimal(6,0),                                             |
|   `m9_hi_tk_m_fem_app_prts_13_24m` decimal(3,0),                                            |
|   `m9_hi_tk_m_fem_app_txns_13_24m` decimal(3,0),                                            |
|   `m9_hi_tk_m_fem_app_dols_13_24m` decimal(6,0),                                            |
|   `m10_mal_fem_shoes_buyrid_lf` decimal(1,0),                                               |
|   `m10_mal_fem_shoes_prts_lf` decimal(3,0),                                                 |
|   `m10_mal_fem_shoes_txns_lf` decimal(3,0),                                                 |
|   `m10_mal_fem_shoes_dols_lf` decimal(6,0),                                                 |
|   `m10_mal_fem_shoes_recency` decimal(6,0),                                                 |
|   `m10_mal_fem_shoes_prts_0_12m` decimal(3,0),                                              |
|   `m10_mal_fem_shoes_txns_0_12m` decimal(3,0),                                              |
|   `m10_mal_fem_shoes_dols_0_12m` decimal(6,0),                                              |
|   `m10_mal_fem_shoes_prts_13_24m` decimal(3,0),                                             |
|   `m10_mal_fem_shoes_txns_13_24m` decimal(3,0),                                             |
|   `m10_mal_fem_shoes_dols_13_24m` decimal(6,0),                                             |
|   `m11_fem_shoes_buyrid_lf` decimal(1,0),                                                   |
|   `m11_fem_shoes_prts_lf` decimal(3,0),                                                     |
|   `m11_fem_shoes_txns_lf` decimal(3,0),                                                     |
|   `m11_fem_shoes_dols_lf` decimal(6,0),                                                     |
|   `m11_fem_shoes_recency` decimal(6,0),                                                     |
|   `m11_fem_shoes_prts_0_12m` decimal(3,0),                                                  |
|   `m11_fem_shoes_txns_0_12m` decimal(3,0),                                                  |
|   `m11_fem_shoes_dols_0_12m` decimal(6,0),                                                  |
|   `m11_fem_shoes_prts_13_24m` decimal(3,0),                                                 |
|   `m11_fem_shoes_txns_13_24m` decimal(3,0),                                                 |
|   `m11_fem_shoes_dols_13_24m` decimal(6,0),                                                 |
|   `m12_mal_shoes_buyrid_lf` decimal(1,0),                                                   |
|   `m12_mal_shoes_prts_lf` decimal(3,0),                                                     |
|   `m12_mal_shoes_txns_lf` decimal(3,0),                                                     |
|   `m12_mal_shoes_dols_lf` decimal(6,0),                                                     |
|   `m12_mal_shoes_recency` decimal(6,0),                                                     |
|   `m12_mal_shoes_prts_0_12m` decimal(3,0),                                                  |
|   `m12_mal_shoes_txns_0_12m` decimal(3,0),                                                  |
|   `m12_mal_shoes_dols_0_12m` decimal(6,0),                                                  |
|   `m12_mal_shoes_prts_13_24m` decimal(3,0),                                                 |
|   `m12_mal_shoes_txns_13_24m` decimal(3,0),                                                 |
|   `m12_mal_shoes_dols_13_24m` decimal(6,0),                                                 |
|   `m13_int_app_undrgm_buyrid_lf` decimal(1,0),                                              |
|   `m13_int_app_undrgm_prts_lf` decimal(3,0),                                                |
|   `m13_int_app_undrgm_txns_lf` decimal(3,0),                                                |
|   `m13_int_app_undrgm_dols_lf` decimal(6,0),                                                |
|   `m13_int_app_undrgm_recency` decimal(6,0),                                                |
|   `m13_int_app_undrgm_prts_0_12m` decimal(3,0),                                             |
|   `m13_int_app_undrgm_txns_0_12m` decimal(3,0),                                             |
|   `m13_int_app_undrgm_dols_0_12m` decimal(6,0),                                             |
|   `m13_int_app_undrgm_prts_13_24m` decimal(3,0),                                            |
|   `m13_int_app_undrgm_txns_13_24m` decimal(3,0),                                            |
|   `m13_int_app_undrgm_dols_13_24m` decimal(6,0),                                            |
|   `m14_hi_tk_jwly_buyrid_lf` decimal(1,0),                                                  |
|   `m14_hi_tk_jwly_prts_lf` decimal(3,0),                                                    |
|   `m14_hi_tk_jwly_txns_lf` decimal(3,0),                                                    |
|   `m14_hi_tk_jwly_dols_lf` decimal(6,0),                                                    |
|   `m14_hi_tk_jwly_recency` decimal(6,0),                                                    |
|   `m14_hi_tk_jwly_prts_0_12m` decimal(3,0),                                                 |
|   `m14_hi_tk_jwly_txns_0_12m` decimal(3,0),                                                 |
|   `m14_hi_tk_jwly_dols_0_12m` decimal(6,0),                                                 |
|   `m14_hi_tk_jwly_prts_13_24m` decimal(3,0),                                                |
|   `m14_hi_tk_jwly_txns_13_24m` decimal(3,0),                                                |
|   `m14_hi_tk_jwly_dols_13_24m` decimal(6,0),                                                |
|   `m15_lo_md_tk_jwly_buyrid_lf` decimal(1,0),                                               |
|   `m15_lo_md_tk_jwly_prts_lf` decimal(3,0),                                                 |
|   `m15_lo_md_tk_jwly_txns_lf` decimal(3,0),                                                 |
|   `m15_lo_md_tk_jwly_dols_lf` decimal(6,0),                                                 |
|   `m15_lo_md_tk_jwly_recency` decimal(6,0),                                                 |
|   `m15_lo_md_tk_jwly_prts_0_12m` decimal(3,0),                                              |
|   `m15_lo_md_tk_jwly_txns_0_12m` decimal(3,0),                                              |
|   `m15_lo_md_tk_jwly_dols_0_12m` decimal(6,0),                                              |
|   `m15_lo_md_tk_jwly_prts_13_24m` decimal(3,0),                                             |
|   `m15_lo_md_tk_jwly_txns_13_24m` decimal(3,0),                                             |
|   `m15_lo_md_tk_jwly_dols_13_24m` decimal(6,0),                                             |
|   `m16_fnd_humanitrn_buyrid_lf` decimal(1,0),                                               |
|   `m16_fnd_humanitrn_prts_lf` decimal(3,0),                                                 |
|   `m16_fnd_humanitrn_txns_lf` decimal(3,0),                                                 |
|   `m16_fnd_humanitrn_dols_lf` decimal(6,0),                                                 |
|   `m16_fnd_humanitrn_recency` decimal(6,0),                                                 |
|   `m16_fnd_humanitrn_prts_0_12m` decimal(3,0),                                              |
|   `m16_fnd_humanitrn_txns_0_12m` decimal(3,0),                                              |
|   `m16_fnd_humanitrn_dols_0_12m` decimal(6,0),                                              |
|   `m16_fnd_humanitrn_prts_13_24m` decimal(3,0),                                             |
|   `m16_fnd_humanitrn_txns_13_24m` decimal(3,0),                                             |
|   `m16_fnd_humanitrn_dols_13_24m` decimal(6,0),                                             |
|   `m17_teen_app_shoes_buyrid_lf` decimal(1,0),                                              |
|   `m17_teen_app_shoes_prts_lf` decimal(3,0),                                                |
|   `m17_teen_app_shoes_txns_lf` decimal(3,0),                                                |
|   `m17_teen_app_shoes_dols_lf` decimal(6,0),                                                |
|   `m17_teen_app_shoes_recency` decimal(6,0),                                                |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `m17_teen_app_shoes_prts_0_12m` decimal(3,0),                                             |
|   `m17_teen_app_shoes_txns_0_12m` decimal(3,0),                                             |
|   `m17_teen_app_shoes_dols_0_12m` decimal(6,0),                                             |
|   `m17_teen_app_shoes_prts_13_24m` decimal(3,0),                                            |
|   `m17_teen_app_shoes_txns_13_24m` decimal(3,0),                                            |
|   `m17_teen_app_shoes_dols_13_24m` decimal(6,0),                                            |
|   `m18_kd_app_shoes_buyrid_lf` decimal(1,0),                                                |
|   `m18_kd_app_shoes_prts_lf` decimal(3,0),                                                  |
|   `m18_kd_app_shoes_txns_lf` decimal(3,0),                                                  |
|   `m18_kd_app_shoes_dols_lf` decimal(6,0),                                                  |
|   `m18_kd_app_shoes_recency` decimal(6,0),                                                  |
|   `m18_kd_app_shoes_prts_0_12m` decimal(3,0),                                               |
|   `m18_kd_app_shoes_txns_0_12m` decimal(3,0),                                               |
|   `m18_kd_app_shoes_dols_0_12m` decimal(6,0),                                               |
|   `m18_kd_app_shoes_prts_13_24m` decimal(3,0),                                              |
|   `m18_kd_app_shoes_txns_13_24m` decimal(3,0),                                              |
|   `m18_kd_app_shoes_dols_13_24m` decimal(6,0),                                              |
|   `m19_kd_mer_allage_buyrid_lf` decimal(1,0),                                               |
|   `m19_kd_mer_allage_prts_lf` decimal(3,0),                                                 |
|   `m19_kd_mer_allage_txns_lf` decimal(3,0),                                                 |
|   `m19_kd_mer_allage_dols_lf` decimal(6,0),                                                 |
|   `m19_kd_mer_allage_recency` decimal(6,0),                                                 |
|   `m19_kd_mer_allage_prts_0_12m` decimal(3,0),                                              |
|   `m19_kd_mer_allage_txns_0_12m` decimal(3,0),                                              |
|   `m19_kd_mer_allage_dols_0_12m` decimal(6,0),                                              |
|   `m19_kd_mer_allage_prts_13_24m` decimal(3,0),                                             |
|   `m19_kd_mer_allage_txns_13_24m` decimal(3,0),                                             |
|   `m19_kd_mer_allage_dols_13_24m` decimal(6,0),                                             |
|   `m20_kd_mer_age_0_6_buyrid_lf` decimal(1,0),                                              |
|   `m20_kd_mer_age_0_6_prts_lf` decimal(3,0),                                                |
|   `m20_kd_mer_age_0_6_txns_lf` decimal(3,0),                                                |
|   `m20_kd_mer_age_0_6_dols_lf` decimal(6,0),                                                |
|   `m20_kd_mer_age_0_6_recency` decimal(6,0),                                                |
|   `m20_kd_mer_age_0_6_prts_0_12m` decimal(3,0),                                             |
|   `m20_kd_mer_age_0_6_txns_0_12m` decimal(3,0),                                             |
|   `m20_kd_mer_age_0_6_dols_0_12m` decimal(6,0),                                             |
|   `m20_kd_mer_age_0_6_prts_13_24m` decimal(3,0),                                            |
|   `m20_kd_mer_age_0_6_txns_13_24m` decimal(3,0),                                            |
|   `m20_kd_mer_age_0_6_dols_13_24m` decimal(6,0),                                            |
|   `m21_kd_mer_age_7up_buyrid_lf` decimal(1,0),                                              |
|   `m21_kd_mer_age_7up_prts_lf` decimal(3,0),                                                |
|   `m21_kd_mer_age_7up_txns_lf` decimal(3,0),                                                |
|   `m21_kd_mer_age_7up_dols_lf` decimal(6,0),                                                |
|   `m21_kd_mer_age_7up_recency` decimal(6,0),                                                |
|   `m21_kd_mer_age_7up_prts_0_12m` decimal(3,0),                                             |
|   `m21_kd_mer_age_7up_txns_0_12m` decimal(3,0),                                             |
|   `m21_kd_mer_age_7up_dols_0_12m` decimal(6,0),                                             |
|   `m21_kd_mer_age_7up_prts_13_24m` decimal(3,0),                                            |
|   `m21_kd_mer_age_7up_txns_13_24m` decimal(3,0),                                            |
|   `m21_kd_mer_age_7up_dols_13_24m` decimal(6,0),                                            |
|   `m22_lo_tk_hmdecgft_buyrid_lf` decimal(1,0),                                              |
|   `m22_lo_tk_hmdecgft_prts_lf` decimal(3,0),                                                |
|   `m22_lo_tk_hmdecgft_txns_lf` decimal(3,0),                                                |
|   `m22_lo_tk_hmdecgft_dols_lf` decimal(6,0),                                                |
|   `m22_lo_tk_hmdecgft_recency` decimal(6,0),                                                |
|   `m22_lo_tk_hmdecgft_prts_0_12m` decimal(3,0),                                             |
|   `m22_lo_tk_hmdecgft_txns_0_12m` decimal(3,0),                                             |
|   `m22_lo_tk_hmdecgft_dols_0_12m` decimal(6,0),                                             |
|   `m22_lo_tk_hmdecgft_prts_13_24m` decimal(3,0),                                            |
|   `m22_lo_tk_hmdecgft_txns_13_24m` decimal(3,0),                                            |
|   `m22_lo_tk_hmdecgft_dols_13_24m` decimal(6,0),                                            |
|   `m23_md_tk_hmdecgft_buyrid_lf` decimal(1,0),                                              |
|   `m23_md_tk_hmdecgft_prts_lf` decimal(3,0),                                                |
|   `m23_md_tk_hmdecgft_txns_lf` decimal(3,0),                                                |
|   `m23_md_tk_hmdecgft_dols_lf` decimal(6,0),                                                |
|   `m23_md_tk_hmdecgft_recency` decimal(6,0),                                                |
|   `m23_md_tk_hmdecgft_prts_0_12m` decimal(3,0),                                             |
|   `m23_md_tk_hmdecgft_txns_0_12m` decimal(3,0),                                             |
|   `m23_md_tk_hmdecgft_dols_0_12m` decimal(6,0),                                             |
|   `m23_md_tk_hmdecgft_prts_13_24m` decimal(3,0),                                            |
|   `m23_md_tk_hmdecgft_txns_13_24m` decimal(3,0),                                            |
|   `m23_md_tk_hmdecgft_dols_13_24m` decimal(6,0),                                            |
|   `m24_hi_tk_hmdecgft_buyrid_lf` decimal(1,0),                                              |
|   `m24_hi_tk_hmdecgft_prts_lf` decimal(3,0),                                                |
|   `m24_hi_tk_hmdecgft_txns_lf` decimal(3,0),                                                |
|   `m24_hi_tk_hmdecgft_dols_lf` decimal(6,0),                                                |
|   `m24_hi_tk_hmdecgft_recency` decimal(6,0),                                                |
|   `m24_hi_tk_hmdecgft_prts_0_12m` decimal(3,0),                                             |
|   `m24_hi_tk_hmdecgft_txns_0_12m` decimal(3,0),                                             |
|   `m24_hi_tk_hmdecgft_dols_0_12m` decimal(6,0),                                             |
|   `m24_hi_tk_hmdecgft_prts_13_24m` decimal(3,0),                                            |
|   `m24_hi_tk_hmdecgft_txns_13_24m` decimal(3,0),                                            |
|   `m24_hi_tk_hmdecgft_dols_13_24m` decimal(6,0),                                            |
|   `m25_mod_contdecgft_buyrid_lf` decimal(1,0),                                              |
|   `m25_mod_contdecgft_prts_lf` decimal(3,0),                                                |
|   `m25_mod_contdecgft_txns_lf` decimal(3,0),                                                |
|   `m25_mod_contdecgft_dols_lf` decimal(6,0),                                                |
|   `m25_mod_contdecgft_recency` decimal(6,0),                                                |
|   `m25_mod_contdecgft_prts_0_12m` decimal(3,0),                                             |
|   `m25_mod_contdecgft_txns_0_12m` decimal(3,0),                                             |
|   `m25_mod_contdecgft_dols_0_12m` decimal(6,0),                                             |
|   `m25_mod_contdecgft_prts_13_24m` decimal(3,0),                                            |
|   `m25_mod_contdecgft_txns_13_24m` decimal(3,0),                                            |
|   `m25_mod_contdecgft_dols_13_24m` decimal(6,0),
|   `m26_ctryhmdecgiftbuyrid_lf` decimal(1,0),                                                |
|   `m26_ctryhmdecgiftprts_lf` decimal(3,0),                                                  |
|   `m26_ctryhmdecgifttxns_lf` decimal(3,0),                                                  |
|   `m26_ctryhmdecgiftdols_lf` decimal(6,0),                                                  |
|   `m26_ctryhmdecgiftrecency` decimal(6,0),                                                  |
|   `m26_ctryhmdecgiftprts_0_12m` decimal(3,0),                                               |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `m26_ctryhmdecgifttxns_0_12m` decimal(3,0),                                               |
|   `m26_ctryhmdecgiftdols_0_12m` decimal(6,0),                                               |
|   `m26_ctryhmdecgiftprts_13_24m` decimal(3,0),                                              |
|   `m26_ctryhmdecgifttxns_13_24m` decimal(3,0),                                              |
|   `m26_ctryhmdecgiftdols_13_24m` decimal(6,0),                                              |
|   `m27_tools_agricltr_buyrid_lf` decimal(1,0),                                              |
|   `m27_tools_agricltr_prts_lf` decimal(3,0),                                                |
|   `m27_tools_agricltr_txns_lf` decimal(3,0),                                                |
|   `m27_tools_agricltr_dols_lf` decimal(6,0),                                                |
|   `m27_tools_agricltr_recency` decimal(6,0),                                                |
|   `m27_tools_agricltr_prts_0_12m` decimal(3,0),                                             |
|   `m27_tools_agricltr_txns_0_12m` decimal(3,0),                                             |
|   `m27_tools_agricltr_dols_0_12m` decimal(6,0),                                             |
|   `m27_tools_agricltr_prts_13_24m` decimal(3,0),                                            |
|   `m27_tools_agricltr_txns_13_24m` decimal(3,0),                                            |
|   `m27_tools_agricltr_dols_13_24m` decimal(6,0),                                            |
|   `m28_artgal_mus_mer_buyrid_lf` decimal(1,0),                                              |
|   `m28_artgal_mus_mer_prts_lf` decimal(3,0),                                                |
|   `m28_artgal_mus_mer_txns_lf` decimal(3,0),                                                |
|   `m28_artgal_mus_mer_dols_lf` decimal(6,0),                                                |
|   `m28_artgal_mus_mer_recency` decimal(6,0),                                                |
|   `m28_artgal_mus_mer_prts_0_12m` decimal(3,0),                                             |
|   `m28_artgal_mus_mer_txns_0_12m` decimal(3,0),                                             |
|   `m28_artgal_mus_mer_dols_0_12m` decimal(6,0),                                             |
|   `m28_artgal_mus_mer_prts_13_24m` decimal(3,0),                                            |
|   `m28_artgal_mus_mer_txns_13_24m` decimal(3,0),                                            |
|   `m28_artgal_mus_mer_dols_13_24m` decimal(6,0),                                            |
|   `m29_fine_frn_buyrid_lf` decimal(1,0),                                                    |
|   `m29_fine_frn_prts_lf` decimal(3,0),                                                      |
|   `m29_fine_frn_txns_lf` decimal(3,0),                                                      |
|   `m29_fine_frn_dols_lf` decimal(6,0),                                                      |
|   `m29_fine_frn_recency` decimal(6,0),                                                      |
|   `m29_fine_frn_prts_0_12m` decimal(3,0),                                                   |
|   `m29_fine_frn_txns_0_12m` decimal(3,0),                                                   |
|   `m29_fine_frn_dols_0_12m` decimal(6,0),                                                   |
|   `m29_fine_frn_prts_13_24m` decimal(3,0),                                                  |
|   `m29_fine_frn_txns_13_24m` decimal(3,0),                                                  |
|   `m29_fine_frn_dols_13_24m` decimal(6,0),                                                  |
|   `m30_kit_accs_dec_buyrid_lf` decimal(1,0),                                                |
|   `m30_kit_accs_dec_prts_lf` decimal(3,0),                                                  |
|   `m30_kit_accs_dec_txns_lf` decimal(3,0),                                                  |
|   `m30_kit_accs_dec_dols_lf` decimal(6,0),                                                  |
|   `m30_kit_accs_dec_recency` decimal(6,0),                                                  |
|   `m30_kit_accs_dec_prts_0_12m` decimal(3,0),                                               |
|   `m30_kit_accs_dec_txns_0_12m` decimal(3,0),                                               |
|   `m30_kit_accs_dec_dols_0_12m` decimal(6,0),                                               |
|   `m30_kit_accs_dec_prts_13_24m` decimal(3,0),                                              |
|   `m30_kit_accs_dec_txns_13_24m` decimal(3,0),                                              |
|   `m30_kit_accs_dec_dols_13_24m` decimal(6,0),                                              |
|   `m31_hi_tk_bb_lin_buyrid_lf` decimal(1,0),                                                |
|   `m31_hi_tk_bb_lin_prts_lf` decimal(3,0),                                                  |
|   `m31_hi_tk_bb_lin_txns_lf` decimal(3,0),                                                  |
|   `m31_hi_tk_bb_lin_dols_lf` decimal(6,0),                                                  |
|   `m31_hi_tk_bb_lin_recency` decimal(6,0),                                                  |
|   `m31_hi_tk_bb_lin_prts_0_12m` decimal(3,0),                                               |
|   `m31_hi_tk_bb_lin_txns_0_12m` decimal(3,0),                                               |
|   `m31_hi_tk_bb_lin_dols_0_12m` decimal(6,0),                                               |
|   `m31_hi_tk_bb_lin_prts_13_24m` decimal(3,0),                                              |
|   `m31_hi_tk_bb_lin_txns_13_24m` decimal(3,0),                                              |
|   `m31_hi_tk_bb_lin_dols_13_24m` decimal(6,0),                                              |
|   `m32_lm_tk_bb_lin_buyrid_lf` decimal(1,0),                                                |
|   `m32_lm_tk_bb_lin_prts_lf` decimal(3,0),                                                  |
|   `m32_lm_tk_bb_lin_txns_lf` decimal(3,0),                                                  |
|   `m32_lm_tk_bb_lin_dols_lf` decimal(6,0),                                                  |
|   `m32_lm_tk_bb_lin_recency` decimal(6,0),                                                  |
|   `m32_lm_tk_bb_lin_prts_0_12m` decimal(3,0),                                               |
|   `m32_lm_tk_bb_lin_txns_0_12m` decimal(3,0),                                               |
|   `m32_lm_tk_bb_lin_dols_0_12m` decimal(6,0),                                               |
|   `m32_lm_tk_bb_lin_prts_13_24m` decimal(3,0),                                              |
|   `m32_lm_tk_bb_lin_txns_13_24m` decimal(3,0),                                              |
|   `m32_lm_tk_bb_lin_dols_13_24m` decimal(6,0),                                              |
|   `m33_hi_tk_bty_spa_buyrid_lf` decimal(1,0),                                               |
|   `m33_hi_tk_bty_spa_prts_lf` decimal(3,0),                                                 |
|   `m33_hi_tk_bty_spa_txns_lf` decimal(3,0),                                                 |
|   `m33_hi_tk_bty_spa_dols_lf` decimal(6,0),                                                 |
|   `m33_hi_tk_bty_spa_recency` decimal(6,0),                                                 |
|   `m33_hi_tk_bty_spa_prts_0_12m` decimal(3,0),                                              |
|   `m33_hi_tk_bty_spa_txns_0_12m` decimal(3,0),                                              |
|   `m33_hi_tk_bty_spa_dols_0_12m` decimal(6,0),                                              |
|   `m33_hi_tk_bty_spa_prts_13_24m` decimal(3,0),                                             |
|   `m33_hi_tk_bty_spa_txns_13_24m` decimal(3,0),                                             |
|   `m33_hi_tk_bty_spa_dols_13_24m` decimal(6,0),                                             |
|   `m34_lm_tk_bty_spa_buyrid_lf` decimal(1,0),                                               |
|   `m34_lm_tk_bty_spa_prts_lf` decimal(3,0),                                                 |
|   `m34_lm_tk_bty_spa_txns_lf` decimal(3,0),                                                 |
|   `m34_lm_tk_bty_spa_dols_lf` decimal(6,0),                                                 |
|   `m34_lm_tk_bty_spa_recency` decimal(6,0),                                                 |
|   `m34_lm_tk_bty_spa_prts_0_12m` decimal(3,0),                                              |
|   `m34_lm_tk_bty_spa_txns_0_12m` decimal(3,0),                                              |
|   `m34_lm_tk_bty_spa_dols_0_12m` decimal(6,0),                                              |
|   `m34_lm_tk_bty_spa_prts_13_24m` decimal(3,0),                                             |
|   `m34_lm_tk_bty_spa_txns_13_24m` decimal(3,0),                                             |
|   `m34_lm_tk_bty_spa_dols_13_24m` decimal(6,0),                                             |
|   `m35_rec_outdoor_buyrid_lf` decimal(1,0),                                                 |
|   `m35_rec_outdoor_prts_lf` decimal(3,0),                                                   |
|   `m35_rec_outdoor_txns_lf` decimal(3,0),                                                   |
|   `m35_rec_outdoor_dols_lf` decimal(6,0),                                                   |
|   `m35_rec_outdoor_recency` decimal(6,0),                                                   |
|   `m35_rec_outdoor_prts_0_12m` decimal(3,0),                                                |
|   `m35_rec_outdoor_txns_0_12m` decimal(3,0),                                                |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `m35_rec_outdoor_dols_0_12m` decimal(6,0),                                                |
|   `m35_rec_outdoor_prts_13_24m` decimal(3,0),                                               |
|   `m35_rec_outdoor_txns_13_24m` decimal(3,0),                                               |
|   `m35_rec_outdoor_dols_13_24m` decimal(6,0),                                               |
|   `m36_hunting_fishin_buyrid_lf` decimal(1,0),                                              |
|   `m36_hunting_fishin_prts_lf` decimal(3,0),                                                |
|   `m36_hunting_fishin_txns_lf` decimal(3,0),                                                |
|   `m36_hunting_fishin_dols_lf` decimal(6,0),                                                |
|   `m36_hunting_fishin_recency` decimal(6,0),                                                |
|   `m36_hunting_fishin_prts_0_12m` decimal(3,0),                                             |
|   `m36_hunting_fishin_txns_0_12m` decimal(3,0),                                             |
|   `m36_hunting_fishin_dols_0_12m` decimal(6,0),                                             |
|   `m36_hunting_fishin_prts_13_24m` decimal(3,0),                                            |
|   `m36_hunting_fishin_txns_13_24m` decimal(3,0),                                            |
|   `m36_hunting_fishin_dols_13_24m` decimal(6,0),                                            |
|   `m37_sports_mer_app_buyrid_lf` decimal(1,0),                                              |
|   `m37_sports_mer_app_prts_lf` decimal(3,0),                                                |
|   `m37_sports_mer_app_txns_lf` decimal(3,0),                                                |
|   `m37_sports_mer_app_dols_lf` decimal(6,0),                                                |
|   `m37_sports_mer_app_recency` decimal(6,0),                                                |
|   `m37_sports_mer_app_prts_0_12m` decimal(3,0),                                             |
|   `m37_sports_mer_app_txns_0_12m` decimal(3,0),                                             |
|   `m37_sports_mer_app_dols_0_12m` decimal(6,0),                                             |
|   `m37_sports_mer_app_prts_13_24m` decimal(3,0),                                            |
|   `m37_sports_mer_app_txns_13_24m` decimal(3,0),                                            |
|   `m37_sports_mer_app_dols_13_24m` decimal(6,0),                                            |
|   `m38_home_org_gdgts_buyrid_lf` decimal(1,0),                                              |
|   `m38_home_org_gdgts_prts_lf` decimal(3,0),                                                |
|   `m38_home_org_gdgts_txns_lf` decimal(3,0),                                                |
|   `m38_home_org_gdgts_dols_lf` decimal(6,0),                                                |
|   `m38_home_org_gdgts_recency` decimal(6,0),                                                |
|   `m38_home_org_gdgts_prts_0_12m` decimal(3,0),                                             |
|   `m38_home_org_gdgts_txns_0_12m` decimal(3,0),                                             |
|   `m38_home_org_gdgts_dols_0_12m` decimal(6,0),                                             |
|   `m38_home_org_gdgts_prts_13_24m` decimal(3,0),                                            |
|   `m38_home_org_gdgts_txns_13_24m` decimal(3,0),                                            |
|   `m38_home_org_gdgts_dols_13_24m` decimal(6,0),                                            |
|   `m39_elc_gfts_gdgts_buyrid_lf` decimal(1,0),                                              |
|   `m39_elc_gfts_gdgts_prts_lf` decimal(3,0),                                                |
|   `m39_elc_gfts_gdgts_txns_lf` decimal(3,0),                                                |
|   `m39_elc_gfts_gdgts_dols_lf` decimal(6,0),                                                |
|   `m39_elc_gfts_gdgts_recency` decimal(6,0),                                                |
|   `m39_elc_gfts_gdgts_prts_0_12m` decimal(3,0),                                             |
|   `m39_elc_gfts_gdgts_txns_0_12m` decimal(3,0),                                             |
|   `m39_elc_gfts_gdgts_dols_0_12m` decimal(6,0),                                             |
|   `m39_elc_gfts_gdgts_prts_13_24m` decimal(3,0),                                            |
|   `m39_elc_gfts_gdgts_txns_13_24m` decimal(3,0),                                            |
|   `m39_elc_gfts_gdgts_dols_13_24m` decimal(6,0),                                            |
|   `m40_fnd_anim_welfr_buyrid_lf` decimal(1,0),                                              |
|   `m40_fnd_anim_welfr_prts_lf` decimal(3,0),                                                |
|   `m40_fnd_anim_welfr_txns_lf` decimal(3,0),                                                |
|   `m40_fnd_anim_welfr_dols_lf` decimal(6,0),                                                |
|   `m40_fnd_anim_welfr_recency` decimal(6,0),                                                |
|   `m40_fnd_anim_welfr_prts_0_12m` decimal(3,0),                                             |
|   `m40_fnd_anim_welfr_txns_0_12m` decimal(3,0),                                             |
|   `m40_fnd_anim_welfr_dols_0_12m` decimal(6,0),                                             |
|   `m40_fnd_anim_welfr_prts_13_24m` decimal(3,0),                                            |
|   `m40_fnd_anim_welfr_txns_13_24m` decimal(3,0),                                            |
|   `m40_fnd_anim_welfr_dols_13_24m` decimal(6,0),                                            |
|   `m41_hi_tk_gfts_mer_buyrid_lf` decimal(1,0),                                              |
|   `m41_hi_tk_gfts_mer_prts_lf` decimal(3,0),                                                |
|   `m41_hi_tk_gfts_mer_txns_lf` decimal(3,0),                                                |
|   `m41_hi_tk_gfts_mer_dols_lf` decimal(6,0),                                                |
|   `m41_hi_tk_gfts_mer_recency` decimal(6,0),                                                |
|   `m41_hi_tk_gfts_mer_prts_0_12m` decimal(3,0),                                             |
|   `m41_hi_tk_gfts_mer_txns_0_12m` decimal(3,0),                                             |
|   `m41_hi_tk_gfts_mer_dols_0_12m` decimal(6,0),                                             |
|   `m41_hi_tk_gfts_mer_prts_13_24m` decimal(3,0),                                            |
|   `m41_hi_tk_gfts_mer_txns_13_24m` decimal(3,0),                                            |
|   `m41_hi_tk_gfts_mer_dols_13_24m` decimal(6,0),                                            |
|   `m42_md_tk_gfts_mer_buyrid_lf` decimal(1,0),                                              |
|   `m42_md_tk_gfts_mer_prts_lf` decimal(3,0),                                                |
|   `m42_md_tk_gfts_mer_txns_lf` decimal(3,0),                                                |
|   `m42_md_tk_gfts_mer_dols_lf` decimal(6,0),                                                |
|   `m42_md_tk_gfts_mer_recency` decimal(6,0),                                                |
|   `m42_md_tk_gfts_mer_prts_0_12m` decimal(3,0),                                             |
|   `m42_md_tk_gfts_mer_txns_0_12m` decimal(3,0),                                             |
|   `m42_md_tk_gfts_mer_dols_0_12m` decimal(6,0),                                             |
|   `m42_md_tk_gfts_mer_prts_13_24m` decimal(3,0),                                            |
|   `m42_md_tk_gfts_mer_txns_13_24m` decimal(3,0),                                            |
|   `m42_md_tk_gfts_mer_dols_13_24m` decimal(6,0),                                            |
|   `m43_lo_tk_gfts_mer_buyrid_lf` decimal(1,0),                                              |
|   `m43_lo_tk_gfts_mer_prts_lf` decimal(3,0),                                                |
|   `m43_lo_tk_gfts_mer_txns_lf` decimal(3,0),                                                |
|   `m43_lo_tk_gfts_mer_dols_lf` decimal(6,0),                                                |
|   `m43_lo_tk_gfts_mer_recency` decimal(6,0),                                                |
|   `m43_lo_tk_gfts_mer_prts_0_12m` decimal(3,0),                                             |
|   `m43_lo_tk_gfts_mer_txns_0_12m` decimal(3,0),                                             |
|   `m43_lo_tk_gfts_mer_dols_0_12m` decimal(6,0),                                             |
|   `m43_lo_tk_gfts_mer_prts_13_24m` decimal(3,0),                                            |
|   `m43_lo_tk_gfts_mer_txns_13_24m` decimal(3,0),                                            |
|   `m43_lo_tk_gfts_mer_dols_13_24m` decimal(6,0),                                            |
|   `m44_theme_gfts_mer_buyrid_lf` decimal(1,0),                                              |
|   `m44_theme_gfts_mer_prts_lf` decimal(3,0),                                                |
|   `m44_theme_gfts_mer_txns_lf` decimal(3,0),                                                |
|   `m44_theme_gfts_mer_dols_lf` decimal(6,0),                                                |
|   `m44_theme_gfts_mer_recency` decimal(6,0),                                                |
|   `m44_theme_gfts_mer_prts_0_12m` decimal(3,0),                                             |
|   `m44_theme_gfts_mer_txns_0_12m` decimal(3,0),                                             |
|   `m44_theme_gfts_mer_dols_0_12m` decimal(6,0),                                             |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `m44_theme_gfts_mer_prts_13_24m` decimal(3,0),                                            |
|   `m44_theme_gfts_mer_txns_13_24m` decimal(3,0),                                            |
|   `m44_theme_gfts_mer_dols_13_24m` decimal(6,0),                                            |
|   `m45_pat_yrd_decfrn_buyrid_lf` decimal(1,0),                                              |
|   `m45_pat_yrd_decfrn_prts_lf` decimal(3,0),                                                |
|   `m45_pat_yrd_decfrn_txns_lf` decimal(3,0),                                                |
|   `m45_pat_yrd_decfrn_dols_lf` decimal(6,0),                                                |
|   `m45_pat_yrd_decfrn_recency` decimal(6,0),                                                |
|   `m45_pat_yrd_decfrn_prts_0_12m` decimal(3,0),                                             |
|   `m45_pat_yrd_decfrn_txns_0_12m` decimal(3,0),                                             |
|   `m45_pat_yrd_decfrn_dols_0_12m` decimal(6,0),                                             |
|   `m45_pat_yrd_decfrn_prts_13_24m` decimal(3,0),                                            |
|   `m45_pat_yrd_decfrn_txns_13_24m` decimal(3,0),                                            |
|   `m45_pat_yrd_decfrn_dols_13_24m` decimal(6,0),                                            |
|   `m46_fem_activewear_buyrid_lf` decimal(1,0),                                              |
|   `m46_fem_activewear_prts_lf` decimal(3,0),                                                |
|   `m46_fem_activewear_txns_lf` decimal(3,0),                                                |
|   `m46_fem_activewear_dols_lf` decimal(6,0),                                                |
|   `m46_fem_activewear_recency` decimal(6,0),                                                |
|   `m46_fem_activewear_prts_0_12m` decimal(3,0),                                             |
|   `m46_fem_activewear_txns_0_12m` decimal(3,0),                                             |
|   `m46_fem_activewear_dols_0_12m` decimal(6,0),                                             |
|   `m46_fem_activewear_prts_13_24m` decimal(3,0),                                            |
|   `m46_fem_activewear_txns_13_24m` decimal(3,0),                                            |
|   `m46_fem_activewear_dols_13_24m` decimal(6,0),                                            |
|   `m47_gard_tools_sup_buyrid_lf` decimal(1,0),                                              |
|   `m47_gard_tools_sup_prts_lf` decimal(3,0),                                                |
|   `m47_gard_tools_sup_txns_lf` decimal(3,0),                                                |
|   `m47_gard_tools_sup_dols_lf` decimal(6,0),                                                |
|   `m47_gard_tools_sup_recency` decimal(6,0),                                                |
|   `m47_gard_tools_sup_prts_0_12m` decimal(3,0),                                             |
|   `m47_gard_tools_sup_txns_0_12m` decimal(3,0),                                             |
|   `m47_gard_tools_sup_dols_0_12m` decimal(6,0),                                             |
|   `m47_gard_tools_sup_prts_13_24m` decimal(3,0),                                            |
|   `m47_gard_tools_sup_txns_13_24m` decimal(3,0),                                            |
|   `m47_gard_tools_sup_dols_13_24m` decimal(6,0),                                            |
|   `m48_seed_blblivpnt_buyrid_lf` decimal(1,0),                                              |
|   `m48_seed_blblivpnt_prts_lf` decimal(3,0),                                                |
|   `m48_seed_blblivpnt_txns_lf` decimal(3,0),                                                |
|   `m48_seed_blblivpnt_dols_lf` decimal(6,0),                                                |
|   `m48_seed_blblivpnt_recency` decimal(6,0),                                                |
|   `m48_seed_blblivpnt_prts_0_12m` decimal(3,0),                                             |
|   `m48_seed_blblivpnt_txns_0_12m` decimal(3,0),                                             |
|   `m48_seed_blblivpnt_dols_0_12m` decimal(6,0),                                             |
|   `m48_seed_blblivpnt_prts_13_24m` decimal(3,0),                                            |
|   `m48_seed_blblivpnt_txns_13_24m` decimal(3,0),                                            |
|   `m48_seed_blblivpnt_dols_13_24m` decimal(6,0),                                            |
|   `m49_woodwrk_sup_buyrid_lf` decimal(1,0),                                                 |
|   `m49_woodwrk_sup_prts_lf` decimal(3,0),                                                   |
|   `m49_woodwrk_sup_txns_lf` decimal(3,0),                                                   |
|   `m49_woodwrk_sup_dols_lf` decimal(6,0),                                                   |
|   `m49_woodwrk_sup_recency` decimal(6,0),                                                   |
|   `m49_woodwrk_sup_prts_0_12m` decimal(3,0),                                                |
|   `m49_woodwrk_sup_txns_0_12m` decimal(3,0),                                                |
|   `m49_woodwrk_sup_dols_0_12m` decimal(6,0),                                                |
|   `m49_woodwrk_sup_prts_13_24m` decimal(3,0),                                               |
|   `m49_woodwrk_sup_txns_13_24m` decimal(3,0),                                               |
|   `m49_woodwrk_sup_dols_13_24m` decimal(6,0),                                               |
|   `m50_arts_crafts_buyrid_lf` decimal(1,0),                                                 |
|   `m50_arts_crafts_prts_lf` decimal(3,0),                                                   |
|   `m50_arts_crafts_txns_lf` decimal(3,0),                                                   |
|   `m50_arts_crafts_dols_lf` decimal(6,0),                                                   |
|   `m50_arts_crafts_recency` decimal(6,0),                                                   |
|   `m50_arts_crafts_prts_0_12m` decimal(3,0),                                                |
|   `m50_arts_crafts_txns_0_12m` decimal(3,0),                                                |
|   `m50_arts_crafts_dols_0_12m` decimal(6,0),                                                |
|   `m50_arts_crafts_prts_13_24m` decimal(3,0),                                               |
|   `m50_arts_crafts_txns_13_24m` decimal(3,0),                                               |
|   `m50_arts_crafts_dols_13_24m` decimal(6,0),                                               |
|   `m51_stnry_buyrid_lf` decimal(1,0),                                                       |
|   `m51_stnry_prts_lf` decimal(3,0),                                                         |
|   `m51_stnry_txns_lf` decimal(3,0),                                                         |
|   `m51_stnry_dols_lf` decimal(6,0),                                                         |
|   `m51_stnry_recency` decimal(6,0),                                                         |
|   `m51_stnry_prts_0_12m` decimal(3,0),                                                      |
|   `m51_stnry_txns_0_12m` decimal(3,0),                                                      |
|   `m51_stnry_dols_0_12m` decimal(6,0),                                                      |
|   `m51_stnry_prts_13_24m` decimal(3,0),                                                     |
|   `m51_stnry_txns_13_24m` decimal(3,0),                                                     |
|   `m51_stnry_dols_13_24m` decimal(6,0),                                                     |
|   `m52_pub_misc_buyrid_lf` decimal(1,0),                                                    |
|   `m52_pub_misc_prts_lf` decimal(3,0),                                                      |
|   `m52_pub_misc_txns_lf` decimal(3,0),                                                      |
|   `m52_pub_misc_dols_lf` decimal(6,0),                                                      |
|   `m52_pub_misc_recency` decimal(6,0),                                                      |
|   `m52_pub_misc_prts_0_12m` decimal(3,0),                                                   |
|   `m52_pub_misc_txns_0_12m` decimal(3,0),                                                   |
|   `m52_pub_misc_dols_0_12m` decimal(6,0),                                                   |
|   `m52_pub_misc_prts_13_24m` decimal(3,0),                                                  |
|   `m52_pub_misc_txns_13_24m` decimal(3,0),                                                  |
|   `m52_pub_misc_dols_13_24m` decimal(6,0),                                                  |
|   `m53_music_buyrid_lf` decimal(1,0),                                                       |
|   `m53_music_prts_lf` decimal(3,0),                                                         |
|   `m53_music_txns_lf` decimal(3,0),                                                         |
|   `m53_music_dols_lf` decimal(6,0),                                                         |
|   `m53_music_recency` decimal(6,0),                                                         |
|   `m53_music_prts_0_12m` decimal(3,0),                                                      |
|   `m53_music_txns_0_12m` decimal(3,0),                                                      |
|   `m53_music_dols_0_12m` decimal(6,0),                                                      |
|   `m53_music_prts_13_24m` decimal(3,0),                                                     |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `m53_music_txns_13_24m` decimal(3,0),                                                     |
|   `m53_music_dols_13_24m` decimal(6,0),                                                     |
|   `m54_video_buyrid_lf` decimal(1,0),                                                       |
|   `m54_video_prts_lf` decimal(3,0),                                                         |
|   `m54_video_txns_lf` decimal(3,0),                                                         |
|   `m54_video_dols_lf` decimal(6,0),                                                         |
|   `m54_video_recency` decimal(6,0),                                                         |
|   `m54_video_prts_0_12m` decimal(3,0),                                                      |
|   `m54_video_txns_0_12m` decimal(3,0),                                                      |
|   `m54_video_dols_0_12m` decimal(6,0),                                                      |
|   `m54_video_prts_13_24m` decimal(3,0),                                                     |
|   `m54_video_txns_13_24m` decimal(3,0),                                                     |
|   `m54_video_dols_13_24m` decimal(6,0),                                                     |
|   `m55_wine_accsies_buyrid_lf` decimal(1,0),                                                |
|   `m55_wine_accsies_prts_lf` decimal(3,0),                                                  |
|   `m55_wine_accsies_txns_lf` decimal(3,0),                                                  |
|   `m55_wine_accsies_dols_lf` decimal(6,0),                                                  |
|   `m55_wine_accsies_recency` decimal(6,0),                                                  |
|   `m55_wine_accsies_prts_0_12m` decimal(3,0),                                               |
|   `m55_wine_accsies_txns_0_12m` decimal(3,0),                                               |
|   `m55_wine_accsies_dols_0_12m` decimal(6,0),                                               |
|   `m55_wine_accsies_prts_13_24m` decimal(3,0),                                              |
|   `m55_wine_accsies_txns_13_24m` decimal(3,0),                                              |
|   `m55_wine_accsies_dols_13_24m` decimal(6,0),                                              |
|   `m56_pub_chldrns_buyrid_lf` decimal(1,0),                                                 |
|   `m56_pub_chldrns_prts_lf` decimal(3,0),                                                   |
|   `m56_pub_chldrns_txns_lf` decimal(3,0),                                                   |
|   `m56_pub_chldrns_dols_lf` decimal(6,0),                                                   |
|   `m56_pub_chldrns_recency` decimal(6,0),                                                   |
|   `m56_pub_chldrns_prts_0_12m` decimal(3,0),                                                |
|   `m56_pub_chldrns_txns_0_12m` decimal(3,0),                                                |
|   `m56_pub_chldrns_dols_0_12m` decimal(6,0),                                                |
|   `m56_pub_chldrns_prts_13_24m` decimal(3,0),                                               |
|   `m56_pub_chldrns_txns_13_24m` decimal(3,0),                                               |
|   `m56_pub_chldrns_dols_13_24m` decimal(6,0),                                               |
|   `m57_meats_seafd_buyrid_lf` decimal(1,0),                                                 |
|   `m57_meats_seafd_prts_lf` decimal(3,0),                                                   |
|   `m57_meats_seafd_txns_lf` decimal(3,0),                                                   |
|   `m57_meats_seafd_dols_lf` decimal(6,0),                                                   |
|   `m57_meats_seafd_recency` decimal(6,0),                                                   |
|   `m57_meats_seafd_prts_0_12m` decimal(3,0),                                                |
|   `m57_meats_seafd_txns_0_12m` decimal(3,0),                                                |
|   `m57_meats_seafd_dols_0_12m` decimal(6,0),                                                |
|   `m57_meats_seafd_prts_13_24m` decimal(3,0),                                               |
|   `m57_meats_seafd_txns_13_24m` decimal(3,0),                                               |
|   `m57_meats_seafd_dols_13_24m` decimal(6,0),                                               |
|   `m58_cakes_breads_buyrid_lf` decimal(1,0),                                                |
|   `m58_cakes_breads_prts_lf` decimal(3,0),                                                  |
|   `m58_cakes_breads_txns_lf` decimal(3,0),                                                  |
|   `m58_cakes_breads_dols_lf` decimal(6,0),                                                  |
|   `m58_cakes_breads_recency` decimal(6,0),                                                  |
|   `m58_cakes_breads_prts_0_12m` decimal(3,0),                                               |
|   `m58_cakes_breads_txns_0_12m` decimal(3,0),                                               |
|   `m58_cakes_breads_dols_0_12m` decimal(6,0),                                               |
|   `m58_cakes_breads_prts_13_24m` decimal(3,0),                                              |
|   `m58_cakes_breads_txns_13_24m` decimal(3,0),                                              |
|   `m58_cakes_breads_dols_13_24m` decimal(6,0),                                              |
|   `m59_choc_confect_buyrid_lf` decimal(1,0),                                                |
|   `m59_choc_confect_prts_lf` decimal(3,0),                                                  |
|   `m59_choc_confect_txns_lf` decimal(3,0),                                                  |
|   `m59_choc_confect_dols_lf` decimal(6,0),                                                  |
|   `m59_choc_confect_recency` decimal(6,0),                                                  |
|   `m59_choc_confect_prts_0_12m` decimal(3,0),                                               |
|   `m59_choc_confect_txns_0_12m` decimal(3,0),                                               |
|   `m59_choc_confect_dols_0_12m` decimal(6,0),                                               |
|   `m59_choc_confect_prts_13_24m` decimal(3,0),                                              |
|   `m59_choc_confect_txns_13_24m` decimal(3,0),                                              |
|   `m59_choc_confect_dols_13_24m` decimal(6,0),                                              |
|   `m60_fruits_nuts_buyrid_lf` decimal(1,0),                                                 |
|   `m60_fruits_nuts_prts_lf` decimal(3,0),                                                   |
|   `m60_fruits_nuts_txns_lf` decimal(3,0),                                                   |
|   `m60_fruits_nuts_dols_lf` decimal(6,0),                                                   |
|   `m60_fruits_nuts_recency` decimal(6,0),                                                   |
|   `m60_fruits_nuts_prts_0_12m` decimal(3,0),                                                |
|   `m60_fruits_nuts_txns_0_12m` decimal(3,0),                                                |
|   `m60_fruits_nuts_dols_0_12m` decimal(6,0),                                                |
|   `m60_fruits_nuts_prts_13_24m` decimal(3,0),                                               |
|   `m60_fruits_nuts_txns_13_24m` decimal(3,0),                                               |
|   `m60_fruits_nuts_dols_13_24m` decimal(6,0),                                               |
|   `m61_spec_fd_spice_buyrid_lf` decimal(1,0),                                               |
|   `m61_spec_fd_spice_prts_lf` decimal(3,0),                                                 |
|   `m61_spec_fd_spice_txns_lf` decimal(3,0),                                                 |
|   `m61_spec_fd_spice_dols_lf` decimal(6,0),                                                 |
|   `m61_spec_fd_spice_recency` decimal(6,0),                                                 |
|   `m61_spec_fd_spice_prts_0_12m` decimal(3,0),                                              |
|   `m61_spec_fd_spice_txns_0_12m` decimal(3,0),                                              |
|   `m61_spec_fd_spice_dols_0_12m` decimal(6,0),                                              |
|   `m61_spec_fd_spice_prts_13_24m` decimal(3,0),                                             |
|   `m61_spec_fd_spice_txns_13_24m` decimal(3,0),                                             |
|   `m61_spec_fd_spice_dols_13_24m` decimal(6,0),                                             |
|   `m62_pro_bus_mer_buyrid_lf` decimal(1,0),                                                 |
|   `m62_pro_bus_mer_prts_lf` decimal(3,0),                                                   |
|   `m62_pro_bus_mer_txns_lf` decimal(3,0),                                                   |
|   `m62_pro_bus_mer_dols_lf` decimal(6,0),                                                   |
|   `m62_pro_bus_mer_recency` decimal(6,0),                                                   |
|   `m62_pro_bus_mer_prts_0_12m` decimal(3,0),                                                |
|   `m62_pro_bus_mer_txns_0_12m` decimal(3,0),                                                |
|   `m62_pro_bus_mer_dols_0_12m` decimal(6,0),                                                |
|   `m62_pro_bus_mer_prts_13_24m` decimal(3,0),                                               |
|   `m62_pro_bus_mer_txns_13_24m` decimal(3,0),                                               |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `m62_pro_bus_mer_dols_13_24m` decimal(6,0),                                               |
|   `m63_trvlpk_appaccs_buyrid_lf` decimal(1,0),                                              |
|   `m63_trvlpk_appaccs_prts_lf` decimal(3,0),                                                |
|   `m63_trvlpk_appaccs_txns_lf` decimal(3,0),                                                |
|   `m63_trvlpk_appaccs_dols_lf` decimal(6,0),                                                |
|   `m63_trvlpk_appaccs_recency` decimal(6,0),                                                |
|   `m63_trvlpk_appaccs_prts_0_12m` decimal(3,0),                                             |
|   `m63_trvlpk_appaccs_txns_0_12m` decimal(3,0),                                             |
|   `m63_trvlpk_appaccs_dols_0_12m` decimal(6,0),                                             |
|   `m63_trvlpk_appaccs_prts_13_24m` decimal(3,0),                                            |
|   `m63_trvlpk_appaccs_txns_13_24m` decimal(3,0),                                            |
|   `m63_trvlpk_appaccs_dols_13_24m` decimal(6,0),                                            |
|   `m64_hh_pet_suply_buyrid_lf` decimal(1,0),                                                |
|   `m64_hh_pet_suply_prts_lf` decimal(3,0),                                                  |
|   `m64_hh_pet_suply_txns_lf` decimal(3,0),                                                  |
|   `m64_hh_pet_suply_dols_lf` decimal(6,0),                                                  |
|   `m64_hh_pet_suply_recency` decimal(6,0),                                                  |
|   `m64_hh_pet_suply_prts_0_12m` decimal(3,0),                                               |
|   `m64_hh_pet_suply_txns_0_12m` decimal(3,0),                                               |
|   `m64_hh_pet_suply_dols_0_12m` decimal(6,0),                                               |
|   `m64_hh_pet_suply_prts_13_24m` decimal(3,0),                                              |
|   `m64_hh_pet_suply_txns_13_24m` decimal(3,0),                                              |
|   `m64_hh_pet_suply_dols_13_24m` decimal(6,0),                                              |
|   `m65_horse_wst_wear_buyrid_lf` decimal(1,0),                                              |
|   `m65_horse_wst_wear_prts_lf` decimal(3,0),                                                |
|   `m65_horse_wst_wear_txns_lf` decimal(3,0),                                                |
|   `m65_horse_wst_wear_dols_lf` decimal(6,0),                                                |
|   `m65_horse_wst_wear_recency` decimal(6,0),                                                |
|   `m65_horse_wst_wear_prts_0_12m` decimal(3,0),                                             |
|   `m65_horse_wst_wear_txns_0_12m` decimal(3,0),                                             |
|   `m65_horse_wst_wear_dols_0_12m` decimal(6,0),                                             |
|   `m65_horse_wst_wear_prts_13_24m` decimal(3,0),                                            |
|   `m65_horse_wst_wear_txns_13_24m` decimal(3,0),                                            |
|   `m65_horse_wst_wear_dols_13_24m` decimal(6,0),                                            |
|   `m66_env_new_age_buyrid_lf` decimal(1,0),                                                 |
|   `m66_env_new_age_prts_lf` decimal(3,0),                                                   |
|   `m66_env_new_age_txns_lf` decimal(3,0),                                                   |
|   `m66_env_new_age_dols_lf` decimal(6,0),                                                   |
|   `m66_env_new_age_recency` decimal(6,0),                                                   |
|   `m66_env_new_age_prts_0_12m` decimal(3,0),                                                |
|   `m66_env_new_age_txns_0_12m` decimal(3,0),                                                |
|   `m66_env_new_age_dols_0_12m` decimal(6,0),                                                |
|   `m66_env_new_age_prts_13_24m` decimal(3,0),                                               |
|   `m66_env_new_age_txns_13_24m` decimal(3,0),                                               |
|   `m66_env_new_age_dols_13_24m` decimal(6,0),                                               |
|   `m67_collects_buyrid_lf` decimal(1,0),                                                    |
|   `m67_collects_prts_lf` decimal(3,0),                                                      |
|   `m67_collects_txns_lf` decimal(3,0),                                                      |
|   `m67_collects_dols_lf` decimal(6,0),                                                      |
|   `m67_collects_recency` decimal(6,0),                                                      |
|   `m67_collects_prts_0_12m` decimal(3,0),                                                   |
|   `m67_collects_txns_0_12m` decimal(3,0),                                                   |
|   `m67_collects_dols_0_12m` decimal(6,0),                                                   |
|   `m67_collects_prts_13_24m` decimal(3,0),                                                  |
|   `m67_collects_txns_13_24m` decimal(3,0),                                                  |
|   `m67_collects_dols_13_24m` decimal(6,0),                                                  |
|   `m68_disc_dom_mer_buyrid_lf` decimal(1,0),                                                |
|   `m68_disc_dom_mer_prts_lf` decimal(3,0),                                                  |
|   `m68_disc_dom_mer_txns_lf` decimal(3,0),                                                  |
|   `m68_disc_dom_mer_dols_lf` decimal(6,0),                                                  |
|   `m68_disc_dom_mer_recency` decimal(6,0),                                                  |
|   `m68_disc_dom_mer_prts_0_12m` decimal(3,0),                                               |
|   `m68_disc_dom_mer_txns_0_12m` decimal(3,0),                                               |
|   `m68_disc_dom_mer_dols_0_12m` decimal(6,0),                                               |
|   `m68_disc_dom_mer_prts_13_24m` decimal(3,0),                                              |
|   `m68_disc_dom_mer_txns_13_24m` decimal(3,0),                                              |
|   `m68_disc_dom_mer_dols_13_24m` decimal(6,0),                                              |
|   `m69_sen_hlth_hrdgd_buyrid_lf` decimal(1,0),                                              |
|   `m69_sen_hlth_hrdgd_prts_lf` decimal(3,0),                                                |
|   `m69_sen_hlth_hrdgd_txns_lf` decimal(3,0),                                                |
|   `m69_sen_hlth_hrdgd_dols_lf` decimal(6,0),                                                |
|   `m69_sen_hlth_hrdgd_recency` decimal(6,0),                                                |
|   `m69_sen_hlth_hrdgd_prts_0_12m` decimal(3,0),                                             |
|   `m69_sen_hlth_hrdgd_txns_0_12m` decimal(3,0),                                             |
|   `m69_sen_hlth_hrdgd_dols_0_12m` decimal(6,0),                                             |
|   `m69_sen_hlth_hrdgd_prts_13_24m` decimal(3,0),                                            |
|   `m69_sen_hlth_hrdgd_txns_13_24m` decimal(3,0),                                            |
|   `m69_sen_hlth_hrdgd_dols_13_24m` decimal(6,0),                                            |
|   `m70_vits_suplmnts_buyrid_lf` decimal(1,0),                                               |
|   `m70_vits_suplmnts_prts_lf` decimal(3,0),                                                 |
|   `m70_vits_suplmnts_txns_lf` decimal(3,0),                                                 |
|   `m70_vits_suplmnts_dols_lf` decimal(6,0),                                                 |
|   `m70_vits_suplmnts_recency` decimal(6,0),                                                 |
|   `m70_vits_suplmnts_prts_0_12m` decimal(3,0),                                              |
|   `m70_vits_suplmnts_txns_0_12m` decimal(3,0),                                              |
|   `m70_vits_suplmnts_dols_0_12m` decimal(6,0),                                              |
|   `m70_vits_suplmnts_prts_13_24m` decimal(3,0),                                             |
|   `m70_vits_suplmnts_txns_13_24m` decimal(3,0),                                             |
|   `m70_vits_suplmnts_dols_13_24m` decimal(6,0),                                             |
|   `m71_hlth_wellness_buyrid_lf` decimal(1,0),                                               |
|   `m71_hlth_wellness_prts_lf` decimal(3,0),                                                 |
|   `m71_hlth_wellness_txns_lf` decimal(3,0),                                                 |
|   `m71_hlth_wellness_dols_lf` decimal(6,0),                                                 |
|   `m71_hlth_wellness_recency` decimal(6,0),                                                 |
|   `m71_hlth_wellness_prts_0_12m` decimal(3,0),                                              |
|   `m71_hlth_wellness_txns_0_12m` decimal(3,0),                                              |
|   `m71_hlth_wellness_dols_0_12m` decimal(6,0),                                              |
|   `m71_hlth_wellness_prts_13_24m` decimal(3,0),                                             |
|   `m71_hlth_wellness_txns_13_24m` decimal(3,0),                                             |
|   `m71_hlth_wellness_dols_13_24m` decimal(6,0),                                             |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `m72_noprof_fndrais_buyrid_lf` decimal(1,0),                                              |
|   `m72_noprof_fndrais_prts_lf` decimal(3,0),                                                |
|   `m72_noprof_fndrais_txns_lf` decimal(3,0),                                                |
|   `m72_noprof_fndrais_dols_lf` decimal(6,0),                                                |
|   `m72_noprof_fndrais_recency` decimal(6,0),                                                |
|   `m72_noprof_fndrais_prts_0_12m` decimal(3,0),                                             |
|   `m72_noprof_fndrais_txns_0_12m` decimal(3,0),                                             |
|   `m72_noprof_fndrais_dols_0_12m` decimal(6,0),                                             |
|   `m72_noprof_fndrais_prts_13_24m` decimal(3,0),                                            |
|   `m72_noprof_fndrais_txns_13_24m` decimal(3,0),                                            |
|   `m72_noprof_fndrais_dols_13_24m` decimal(6,0),                                            |
|   `m73_miscellaneous_buyrid_lf` decimal(1,0),                                               |
|   `m73_miscellaneous_prts_lf` decimal(3,0),                                                 |
|   `m73_miscellaneous_txns_lf` decimal(3,0),                                                 |
|   `m73_miscellaneous_dols_lf` decimal(6,0),                                                 |
|   `m73_miscellaneous_recency` decimal(6,0),                                                 |
|   `m73_miscellaneous_prts_0_12m` decimal(3,0),                                              |
|   `m73_miscellaneous_txns_0_12m` decimal(3,0),                                              |
|   `m73_miscellaneous_dols_0_12m` decimal(6,0),                                              |
|   `m73_miscellaneous_prts_13_24m` decimal(3,0),                                             |
|   `m73_miscellaneous_txns_13_24m` decimal(3,0),                                             |
|   `m73_miscellaneous_dols_13_24m` decimal(6,0),                                             |
|   `m74_collects_metal_buyrid_lf` decimal(1,0),                                              |
|   `m74_collects_metal_prts_lf` decimal(3,0),                                                |
|   `m74_collects_metal_txns_lf` decimal(3,0),                                                |
|   `m74_collects_metal_dols_lf` decimal(6,0),                                                |
|   `m74_collects_metal_recency` decimal(6,0),                                                |
|   `m74_collects_metal_prts_0_12m` decimal(3,0),                                             |
|   `m74_collects_metal_txns_0_12m` decimal(3,0),                                             |
|   `m74_collects_metal_dols_0_12m` decimal(6,0),                                             |
|   `m74_collects_metal_prts_13_24m` decimal(3,0),                                            |
|   `m74_collects_metal_txns_13_24m` decimal(3,0),                                            |
|   `m74_collects_metal_dols_13_24m` decimal(6,0),                                            |
|   `m75_auto_prts_accs_buyrid_lf` decimal(1,0),                                              |
|   `m75_auto_prts_accs_prts_lf` decimal(3,0),                                                |
|   `m75_auto_prts_accs_txns_lf` decimal(3,0),                                                |
|   `m75_auto_prts_accs_dols_lf` decimal(6,0),                                                |
|   `m75_auto_prts_accs_recency` decimal(6,0),                                                |
|   `m75_auto_prts_accs_prts_0_12m` decimal(3,0),                                             |
|   `m75_auto_prts_accs_txns_0_12m` decimal(3,0),                                             |
|   `m75_auto_prts_accs_dols_0_12m` decimal(6,0),                                             |
|   `m75_auto_prts_accs_prts_13_24m` decimal(3,0),                                            |
|   `m75_auto_prts_accs_txns_13_24m` decimal(3,0),                                            |
|   `m75_auto_prts_accs_dols_13_24m` decimal(6,0),                                            |
|   `m76_mens_suplmnts_buyrid_lf` decimal(1,0),                                               |
|   `m76_mens_suplmnts_prts_lf` decimal(3,0),                                                 |
|   `m76_mens_suplmnts_txns_lf` decimal(3,0),                                                 |
|   `m76_mens_suplmnts_dols_lf` decimal(6,0),                                                 |
|   `m76_mens_suplmnts_recency` decimal(6,0),                                                 |
|   `m76_mens_suplmnts_prts_0_12m` decimal(3,0),                                              |
|   `m76_mens_suplmnts_txns_0_12m` decimal(3,0),                                              |
|   `m76_mens_suplmnts_dols_0_12m` decimal(6,0),                                              |
|   `m76_mens_suplmnts_prts_13_24m` decimal(3,0),                                             |
|   `m76_mens_suplmnts_txns_13_24m` decimal(3,0),                                             |
|   `m76_mens_suplmnts_dols_13_24m` decimal(6,0),                                             |
|   `m77_fnd_world_relf_buyrid_lf` decimal(1,0),                                              |
|   `m77_fnd_world_relf_prts_lf` decimal(3,0),                                                |
|   `m77_fnd_world_relf_txns_lf` decimal(3,0),                                                |
|   `m77_fnd_world_relf_dols_lf` decimal(6,0),                                                |
|   `m77_fnd_world_relf_recency` decimal(6,0),                                                |
|   `m77_fnd_world_relf_prts_0_12m` decimal(3,0),                                             |
|   `m77_fnd_world_relf_txns_0_12m` decimal(3,0),                                             |
|   `m77_fnd_world_relf_dols_0_12m` decimal(6,0),                                             |
|   `m77_fnd_world_relf_prts_13_24m` decimal(3,0),                                            |
|   `m77_fnd_world_relf_txns_13_24m` decimal(3,0),                                            |
|   `m77_fnd_world_relf_dols_13_24m` decimal(6,0),                                            |
|   `m78_newletters_buyrid_lf` decimal(1,0),                                                  |
|   `m78_newletters_prts_lf` decimal(3,0),                                                    |
|   `m78_newletters_txns_lf` decimal(3,0),                                                    |
|   `m78_newletters_dols_lf` decimal(6,0),                                                    |
|   `m78_newletters_recency` decimal(6,0),                                                    |
|   `m78_newletters_prts_0_12m` decimal(3,0),                                                 |
|   `m78_newletters_txns_0_12m` decimal(3,0),                                                 |
|   `m78_newletters_dols_0_12m` decimal(6,0),                                                 |
|   `m78_newletters_prts_13_24m` decimal(3,0),                                                |
|   `m78_newletters_txns_13_24m` decimal(3,0),                                                |
|   `m78_newletters_dols_13_24m` decimal(6,0),                                                |
|   `m79_fnd_med_resrch_buyrid_lf` decimal(1,0),                                              |
|   `m79_fnd_med_resrch_prts_lf` decimal(3,0),                                                |
|   `m79_fnd_med_resrch_txns_lf` decimal(3,0),                                                |
|   `m79_fnd_med_resrch_dols_lf` decimal(6,0),                                                |
|   `m79_fnd_med_resrch_recency` decimal(6,0),                                                |
|   `m79_fnd_med_resrch_prts_0_12m` decimal(3,0),                                             |
|   `m79_fnd_med_resrch_txns_0_12m` decimal(3,0),                                             |
|   `m79_fnd_med_resrch_dols_0_12m` decimal(6,0),                                             |
|   `m79_fnd_med_resrch_prts_13_24m` decimal(3,0),                                            |
|   `m79_fnd_med_resrch_txns_13_24m` decimal(3,0),                                            |
|   `m79_fnd_med_resrch_dols_13_24m` decimal(6,0),                                            |
|   `m80_fnd_envrntl_buyrid_lf` decimal(1,0),                                                 |
|   `m80_fnd_envrntl_prts_lf` decimal(3,0),                                                   |
|   `m80_fnd_envrntl_txns_lf` decimal(3,0),                                                   |
|   `m80_fnd_envrntl_dols_lf` decimal(6,0),                                                   |
|   `m80_fnd_envrntl_recency` decimal(6,0),                                                   |
|   `m80_fnd_envrntl_prts_0_12m` decimal(3,0),                                                |
|   `m80_fnd_envrntl_txns_0_12m` decimal(3,0),                                                |
|   `m80_fnd_envrntl_dols_0_12m` decimal(6,0),                                                |
|   `m80_fnd_envrntl_prts_13_24m` decimal(3,0),                                               |
|   `m80_fnd_envrntl_txns_13_24m` decimal(3,0),                                               |
|   `m80_fnd_envrntl_dols_13_24m` decimal(6,0))                                               |
| ROW FORMAT DELIMITED                                                                        |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   FIELDS TERMINATED BY '|'                                                                  |
| STORED AS INPUTFORMAT                                                                       |
|   'org.apache.hadoop.mapred.TextInputFormat'                                                |
| OUTPUTFORMAT                                                                                |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                              |
| LOCATION                                                                                    |
|   'hdfs://ascenaprod/apps/hive/warehouse/ascena_staging.db/loft_f_abacus_data_overlay1_vw'  |
| TBLPROPERTIES (                                                                             |
|   'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}',                                     |
|   'numFiles'='0',                                                                           |
|   'numRows'='0',                                                                            |
|   'rawDataSize'='0',                                                                        |
|   'serialization.null.format'='',                                                           |
|   'skip.header.line.count'='1',                                                             |
|   'totalSize'='0',                                                                          |
|   'transient_lastDdlTime'='1564751246')                                                     |
+---------------------------------------------------------------------------------------------+--+
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
| CREATE TABLE `loft_f_abacus_data_overlay2_vw`(                                              |
|   `indiv_id` decimal(13,0),                                                                 |
|   `brand_cd` string,                                                                        |
|   `customer_information` string,                                                            |
|   `sequence_number` string,                                                                 |
|   `match_flag` string,                                                                      |
|   `duplicate_indicator` string,                                                             |
|   `reserved` string,                                                                        |
|   `m81_b2b_mag_new_bk_buyrid_lf` decimal(1,0),                                              |
|   `m81_b2b_mag_new_bk_prts_lf` decimal(3,0),                                                |
|   `m81_b2b_mag_new_bk_txns_lf` decimal(3,0),                                                |
|   `m81_b2b_mag_new_bk_dols_lf` decimal(6,0),                                                |
|   `m81_b2b_mag_new_bk_recency` decimal(6,0),                                                |
|   `m81_b2b_mag_new_bk_prts_0_12m` decimal(3,0),                                             |
|   `m81_b2b_mag_new_bk_txns_0_12m` decimal(3,0),                                             |
|   `m81_b2b_mag_new_bk_dols_0_12m` decimal(6,0),                                             |
|   `m81_b2b_mag_new_bk_prts_13_24m` decimal(3,0),                                            |
|   `m81_b2b_mag_new_bk_txns_13_24m` decimal(3,0),                                            |
|   `m81_b2b_mag_new_bk_dols_13_24m` decimal(6,0),                                            |
|   `m82_pub_hobbies_buyrid_lf` decimal(1,0),                                                 |
|   `m82_pub_hobbies_prts_lf` decimal(3,0),                                                   |
|   `m82_pub_hobbies_txns_lf` decimal(3,0),                                                   |
|   `m82_pub_hobbies_dols_lf` decimal(6,0),                                                   |
|   `m82_pub_hobbies_recency` decimal(6,0),                                                   |
|   `m82_pub_hobbies_prts_0_12m` decimal(3,0),                                                |
|   `m82_pub_hobbies_txns_0_12m` decimal(3,0),                                                |
|   `m82_pub_hobbies_dols_0_12m` decimal(6,0),                                                |
|   `m82_pub_hobbies_prts_13_24m` decimal(3,0),                                               |
|   `m82_pub_hobbies_txns_13_24m` decimal(3,0),                                               |
|   `m82_pub_hobbies_dols_13_24m` decimal(6,0),                                               |
|   `m83_b2b_hi_tech_buyrid_lf` decimal(1,0),                                                 |
|   `m83_b2b_hi_tech_prts_lf` decimal(3,0),                                                   |
|   `m83_b2b_hi_tech_txns_lf` decimal(3,0),                                                   |
|   `m83_b2b_hi_tech_dols_lf` decimal(6,0),                                                   |
|   `m83_b2b_hi_tech_recency` decimal(6,0),                                                   |
|   `m83_b2b_hi_tech_prts_0_12m` decimal(3,0),                                                |
|   `m83_b2b_hi_tech_txns_0_12m` decimal(3,0),                                                |
|   `m83_b2b_hi_tech_dols_0_12m` decimal(6,0),                                                |
|   `m83_b2b_hi_tech_prts_13_24m` decimal(3,0),                                               |
|   `m83_b2b_hi_tech_txns_13_24m` decimal(3,0),                                               |
|   `m83_b2b_hi_tech_dols_13_24m` decimal(6,0),                                               |
|   `m84_b2b_comind_sft_buyrid_lf` decimal(1,0),                                              |
|   `m84_b2b_comind_sft_prts_lf` decimal(3,0),                                                |
|   `m84_b2b_comind_sft_txns_lf` decimal(3,0),                                                |
|   `m84_b2b_comind_sft_dols_lf` decimal(6,0),                                                |
|   `m84_b2b_comind_sft_recency` decimal(6,0),                                                |
|   `m84_b2b_comind_sft_prts_0_12m` decimal(3,0),                                             |
|   `m84_b2b_comind_sft_txns_0_12m` decimal(3,0),                                             |
|   `m84_b2b_comind_sft_dols_0_12m` decimal(6,0),                                             |
|   `m84_b2b_comind_sft_prts_13_24m` decimal(3,0),                                            |
|   `m84_b2b_comind_sft_txns_13_24m` decimal(3,0),                                            |
|   `m84_b2b_comind_sft_dols_13_24m` decimal(6,0),                                            |
|   `m85_b2b_off_sup_eq_buyrid_lf` decimal(1,0),                                              |
|   `m85_b2b_off_sup_eq_prts_lf` decimal(3,0),                                                |
|   `m85_b2b_off_sup_eq_txns_lf` decimal(3,0),                                                |
|   `m85_b2b_off_sup_eq_dols_lf` decimal(6,0),                                                |
|   `m85_b2b_off_sup_eq_recency` decimal(6,0),                                                |
|   `m85_b2b_off_sup_eq_prts_0_12m` decimal(3,0),                                             |
|   `m85_b2b_off_sup_eq_txns_0_12m` decimal(3,0),                                             |
|   `m85_b2b_off_sup_eq_dols_0_12m` decimal(6,0),                                             |
|   `m85_b2b_off_sup_eq_prts_13_24m` decimal(3,0),                                            |
|   `m85_b2b_off_sup_eq_txns_13_24m` decimal(3,0),                                            |
|   `m85_b2b_off_sup_eq_dols_13_24m` decimal(6,0),                                            |
|   `m86_b2b_seminars_buyrid_lf` decimal(1,0),                                                |
|   `m86_b2b_seminars_prts_lf` decimal(3,0),                                                  |
|   `m86_b2b_seminars_txns_lf` decimal(3,0),                                                  |
|   `m86_b2b_seminars_dols_lf` decimal(6,0),                                                  |
|   `m86_b2b_seminars_recency` decimal(6,0),                                                  |
|   `m86_b2b_seminars_prts_0_12m` decimal(3,0),                                               |
|   `m86_b2b_seminars_txns_0_12m` decimal(3,0),                                               |
|   `m86_b2b_seminars_dols_0_12m` decimal(6,0),                                               |
|   `m86_b2b_seminars_prts_13_24m` decimal(3,0),                                              |
|   `m86_b2b_seminars_txns_13_24m` decimal(3,0),                                              |
|   `m86_b2b_seminars_dols_13_24m` decimal(6,0),                                              |
|   `m87_b2b_gfts_fd_buyrid_lf` decimal(1,0),                                                 |
|   `m87_b2b_gfts_fd_prts_lf` decimal(3,0),                                                   |
|   `m87_b2b_gfts_fd_txns_lf` decimal(3,0),                                                   |
|   `m87_b2b_gfts_fd_dols_lf` decimal(6,0),                                                   |
|   `m87_b2b_gfts_fd_recency` decimal(6,0),                                                   |
|   `m87_b2b_gfts_fd_prts_0_12m` decimal(3,0),                                                |
|   `m87_b2b_gfts_fd_txns_0_12m` decimal(3,0),                                                |
|   `m87_b2b_gfts_fd_dols_0_12m` decimal(6,0),                                                |
|   `m87_b2b_gfts_fd_prts_13_24m` decimal(3,0),                                               |
|   `m87_b2b_gfts_fd_txns_13_24m` decimal(3,0),                                               |
|   `m87_b2b_gfts_fd_dols_13_24m` decimal(6,0),                                               |
|   `m88_b2b_card_stnry_buyrid_lf` decimal(1,0),                                              |
|   `m88_b2b_card_stnry_prts_lf` decimal(3,0),                                                |
|   `m88_b2b_card_stnry_txns_lf` decimal(3,0),                                                |
|   `m88_b2b_card_stnry_dols_lf` decimal(6,0),                                                |
|   `m88_b2b_card_stnry_recency` decimal(6,0),                                                |
|   `m88_b2b_card_stnry_prts_0_12m` decimal(3,0),                                             |
|   `m88_b2b_card_stnry_txns_0_12m` decimal(3,0),                                             |
|   `m88_b2b_card_stnry_dols_0_12m` decimal(6,0),                                             |
|   `m88_b2b_card_stnry_prts_13_24m` decimal(3,0),                                            |
|   `m88_b2b_card_stnry_txns_13_24m` decimal(3,0),                                            |
|   `m88_b2b_card_stnry_dols_13_24m` decimal(6,0),                                            |
|   `m89_b2b_ad_spec_buyrid_lf` decimal(1,0),                                                 |
|   `m89_b2b_ad_spec_prts_lf` decimal(3,0),                                                   |
|   `m89_b2b_ad_spec_txns_lf` decimal(3,0),                                                   |
|   `m89_b2b_ad_spec_dols_lf` decimal(6,0),                                                   |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `m89_b2b_ad_spec_recency` decimal(6,0),                                                   |
|   `m89_b2b_ad_spec_prts_0_12m` decimal(3,0),                                                |
|   `m89_b2b_ad_spec_txns_0_12m` decimal(3,0),                                                |
|   `m89_b2b_ad_spec_dols_0_12m` decimal(6,0),                                                |
|   `m89_b2b_ad_spec_prts_13_24m` decimal(3,0),                                               |
|   `m89_b2b_ad_spec_txns_13_24m` decimal(3,0),                                               |
|   `m89_b2b_ad_spec_dols_13_24m` decimal(6,0),                                               |
|   `m90_b2b_uniforms_buyrid_lf` decimal(1,0),                                                |
|   `m90_b2b_uniforms_prts_lf` decimal(3,0),                                                  |
|   `m90_b2b_uniforms_txns_lf` decimal(3,0),                                                  |
|   `m90_b2b_uniforms_dols_lf` decimal(6,0),                                                  |
|   `m90_b2b_uniforms_recency` decimal(6,0),                                                  |
|   `m90_b2b_uniforms_prts_0_12m` decimal(3,0),                                               |
|   `m90_b2b_uniforms_txns_0_12m` decimal(3,0),                                               |
|   `m90_b2b_uniforms_dols_0_12m` decimal(6,0),                                               |
|   `m90_b2b_uniforms_prts_13_24m` decimal(3,0),                                              |
|   `m90_b2b_uniforms_txns_13_24m` decimal(3,0),                                              |
|   `m90_b2b_uniforms_dols_13_24m` decimal(6,0),                                              |
|   `m91_b2b_frniture_buyrid_lf` decimal(1,0),                                                |
|   `m91_b2b_frniture_prts_lf` decimal(3,0),                                                  |
|   `m91_b2b_frniture_txns_lf` decimal(3,0),                                                  |
|   `m91_b2b_frniture_dols_lf` decimal(6,0),                                                  |
|   `m91_b2b_frniture_recency` decimal(6,0),                                                  |
|   `m91_b2b_frniture_prts_0_12m` decimal(3,0),                                               |
|   `m91_b2b_frniture_txns_0_12m` decimal(3,0),                                               |
|   `m91_b2b_frniture_dols_0_12m` decimal(6,0),                                               |
|   `m91_b2b_frniture_prts_13_24m` decimal(3,0),                                              |
|   `m91_b2b_frniture_txns_13_24m` decimal(3,0),                                              |
|   `m91_b2b_frniture_dols_13_24m` decimal(6,0),                                              |
|   `m92_pub_hlth_buyrid_lf` decimal(1,0),                                                    |
|   `m92_pub_hlth_prts_lf` decimal(3,0),                                                      |
|   `m92_pub_hlth_txns_lf` decimal(3,0),                                                      |
|   `m92_pub_hlth_dols_lf` decimal(6,0),                                                      |
|   `m92_pub_hlth_recency` decimal(6,0),                                                      |
|   `m92_pub_hlth_prts_0_12m` decimal(3,0),                                                   |
|   `m92_pub_hlth_txns_0_12m` decimal(3,0),                                                   |
|   `m92_pub_hlth_dols_0_12m` decimal(6,0),                                                   |
|   `m92_pub_hlth_prts_13_24m` decimal(3,0),                                                  |
|   `m92_pub_hlth_txns_13_24m` decimal(3,0),                                                  |
|   `m92_pub_hlth_dols_13_24m` decimal(6,0),                                                  |
|   `m93_b2b_gft_givawy_buyrid_lf` decimal(1,0),                                              |
|   `m93_b2b_gft_givawy_prts_lf` decimal(3,0),                                                |
|   `m93_b2b_gft_givawy_txns_lf` decimal(3,0),                                                |
|   `m93_b2b_gft_givawy_dols_lf` decimal(6,0),                                                |
|   `m93_b2b_gft_givawy_recency` decimal(6,0),                                                |
|   `m93_b2b_gft_givawy_prts_0_12m` decimal(3,0),                                             |
|   `m93_b2b_gft_givawy_txns_0_12m` decimal(3,0),                                             |
|   `m93_b2b_gft_givawy_dols_0_12m` decimal(6,0),                                             |
|   `m93_b2b_gft_givawy_prts_13_24m` decimal(3,0),                                            |
|   `m93_b2b_gft_givawy_txns_13_24m` decimal(3,0),                                            |
|   `m93_b2b_gft_givawy_dols_13_24m` decimal(6,0),                                            |
|   `m94_b2b_hr_train_buyrid_lf` decimal(1,0),                                                |
|   `m94_b2b_hr_train_prts_lf` decimal(3,0),                                                  |
|   `m94_b2b_hr_train_txns_lf` decimal(3,0),                                                  |
|   `m94_b2b_hr_train_dols_lf` decimal(6,0),                                                  |
|   `m94_b2b_hr_train_recency` decimal(6,0),                                                  |
|   `m94_b2b_hr_train_prts_0_12m` decimal(3,0),                                               |
|   `m94_b2b_hr_train_txns_0_12m` decimal(3,0),                                               |
|   `m94_b2b_hr_train_dols_0_12m` decimal(6,0),                                               |
|   `m94_b2b_hr_train_prts_13_24m` decimal(3,0),                                              |
|   `m94_b2b_hr_train_txns_13_24m` decimal(3,0),                                              |
|   `m94_b2b_hr_train_dols_13_24m` decimal(6,0),                                              |
|   `m95_pub_finance_buyrid_lf` decimal(1,0),                                                 |
|   `m95_pub_finance_prts_lf` decimal(3,0),                                                   |
|   `m95_pub_finance_txns_lf` decimal(3,0),                                                   |
|   `m95_pub_finance_dols_lf` decimal(6,0),                                                   |
|   `m95_pub_finance_recency` decimal(6,0),                                                   |
|   `m95_pub_finance_prts_0_12m` decimal(3,0),                                                |
|   `m95_pub_finance_txns_0_12m` decimal(3,0),                                                |
|   `m95_pub_finance_dols_0_12m` decimal(6,0),                                                |
|   `m95_pub_finance_prts_13_24m` decimal(3,0),                                               |
|   `m95_pub_finance_txns_13_24m` decimal(3,0),                                               |
|   `m95_pub_finance_dols_13_24m` decimal(6,0),                                               |
|   `m96_pub_womens_buyrid_lf` decimal(1,0),                                                  |
|   `m96_pub_womens_prts_lf` decimal(3,0),                                                    |
|   `m96_pub_womens_txns_lf` decimal(3,0),                                                    |
|   `m96_pub_womens_dols_lf` decimal(6,0),                                                    |
|   `m96_pub_womens_recency` decimal(6,0),                                                    |
|   `m96_pub_womens_prts_0_12m` decimal(3,0),                                                 |
|   `m96_pub_womens_txns_0_12m` decimal(3,0),                                                 |
|   `m96_pub_womens_dols_0_12m` decimal(6,0),                                                 |
|   `m96_pub_womens_prts_13_24m` decimal(3,0),                                                |
|   `m96_pub_womens_txns_13_24m` decimal(3,0),                                                |
|   `m96_pub_womens_dols_13_24m` decimal(6,0),                                                |
|   `m97_pub_mens_buyrid_lf` decimal(1,0),                                                    |
|   `m97_pub_mens_prts_lf` decimal(3,0),                                                      |
|   `m97_pub_mens_txns_lf` decimal(3,0),                                                      |
|   `m97_pub_mens_dols_lf` decimal(6,0),                                                      |
|   `m97_pub_mens_recency` decimal(6,0),                                                      |
|   `m97_pub_mens_prts_0_12m` decimal(3,0),                                                   |
|   `m97_pub_mens_txns_0_12m` decimal(3,0),                                                   |
|   `m97_pub_mens_dols_0_12m` decimal(6,0),                                                   |
|   `m97_pub_mens_prts_13_24m` decimal(3,0),                                                  |
|   `m97_pub_mens_txns_13_24m` decimal(3,0),                                                  |
|   `m97_pub_mens_dols_13_24m` decimal(6,0),                                                  |
|   `m98_pub_ktchn_hm_buyrid_lf` decimal(1,0),                                                |
|   `m98_pub_ktchn_hm_prts_lf` decimal(3,0),                                                  |
|   `m98_pub_ktchn_hm_txns_lf` decimal(3,0),                                                  |
|   `m98_pub_ktchn_hm_dols_lf` decimal(6,0),                                                  |
|   `m98_pub_ktchn_hm_recency` decimal(6,0),                                                  |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `m98_pub_ktchn_hm_prts_0_12m` decimal(3,0),                                               |
|   `m98_pub_ktchn_hm_txns_0_12m` decimal(3,0),                                               |
|   `m98_pub_ktchn_hm_dols_0_12m` decimal(6,0),                                               |
|   `m98_pub_ktchn_hm_prts_13_24m` decimal(3,0),                                              |
|   `m98_pub_ktchn_hm_txns_13_24m` decimal(3,0),                                              |
|   `m98_pub_ktchn_hm_dols_13_24m` decimal(6,0),                                              |
|   `h1_direct_buyrid_lf` decimal(1,0),                                                       |
|   `h1_direct_prts_lf` decimal(3,0),                                                         |
|   `h1_direct_txns_lf` decimal(3,0),                                                         |
|   `h1_direct_dols_lf` decimal(6,0),                                                         |
|   `h1_direct_recency` decimal(6,0),                                                         |
|   `h1_direct_prts_0_12m` decimal(3,0),                                                      |
|   `h1_direct_txns_0_12m` decimal(3,0),                                                      |
|   `h1_direct_dols_0_12m` decimal(6,0),                                                      |
|   `h1_direct_prts_13_24m` decimal(3,0),                                                     |
|   `h1_direct_txns_13_24m` decimal(3,0),                                                     |
|   `h1_direct_dols_13_24m` decimal(6,0),                                                     |
|   `h2_phone_mail_buyrid_lf` decimal(1,0),                                                   |
|   `h2_phone_mail_prts_lf` decimal(3,0),                                                     |
|   `h2_phone_mail_txns_lf` decimal(3,0),                                                     |
|   `h2_phone_mail_dols_lf` decimal(6,0),                                                     |
|   `h2_phone_mail_recency` decimal(6,0),                                                     |
|   `h2_phone_mail_prts_0_12m` decimal(3,0),                                                  |
|   `h2_phone_mail_txns_0_12m` decimal(3,0),                                                  |
|   `h2_phone_mail_dols_0_12m` decimal(6,0),                                                  |
|   `h2_phone_mail_prts_13_24m` decimal(3,0),                                                 |
|   `h2_phone_mail_txns_13_24m` decimal(3,0),                                                 |
|   `h2_phone_mail_dols_13_24m` decimal(6,0),                                                 |
|   `h3_retail_buyrid_lf` decimal(1,0),                                                       |
|   `h3_retail_prts_lf` decimal(3,0),                                                         |
|   `h3_retail_txns_lf` decimal(3,0),                                                         |
|   `h3_retail_dols_lf` decimal(6,0),                                                         |
|   `h3_retail_recency` decimal(6,0),                                                         |
|   `h3_retail_prts_0_12m` decimal(3,0),                                                      |
|   `h3_retail_txns_0_12m` decimal(3,0),                                                      |
|   `h3_retail_dols_0_12m` decimal(6,0),                                                      |
|   `h3_retail_prts_13_24m` decimal(3,0),                                                     |
|   `h3_retail_txns_13_24m` decimal(3,0),                                                     |
|   `h3_retail_dols_13_24m` decimal(6,0),                                                     |
|   `h4_web_buyrid_lf` decimal(1,0),                                                          |
|   `h4_web_prts_lf` decimal(3,0),                                                            |
|   `h4_web_txns_lf` decimal(3,0),                                                            |
|   `h4_web_dols_lf` decimal(6,0),                                                            |
|   `h4_web_recency` decimal(6,0),                                                            |
|   `h4_web_prts_0_12m` decimal(3,0),                                                         |
|   `h4_web_txns_0_12m` decimal(3,0),                                                         |
|   `h4_web_dols_0_12m` decimal(6,0),                                                         |
|   `h4_web_prts_13_24m` decimal(3,0),                                                        |
|   `h4_web_txns_13_24m` decimal(3,0),                                                        |
|   `h4_web_dols_13_24m` decimal(6,0),                                                        |
|   `r1_fam_fashioners_buyrid_lf` decimal(1,0),                                               |
|   `r1_fam_fashioners_prts_lf` decimal(3,0),                                                 |
|   `r1_fam_fashioners_txns_lf` decimal(3,0),                                                 |
|   `r1_fam_fashioners_dols_lf` decimal(6,0),                                                 |
|   `r1_fam_fashioners_recency` decimal(6,0),                                                 |
|   `r1_fam_fashioners_prts_0_12m` decimal(3,0),                                              |
|   `r1_fam_fashioners_txns_0_12m` decimal(3,0),                                              |
|   `r1_fam_fashioners_dols_0_12m` decimal(6,0),                                              |
|   `r1_fam_fashioners_prts_13_24m` decimal(3,0),                                             |
|   `r1_fam_fashioners_txns_13_24m` decimal(3,0),                                             |
|   `r1_fam_fashioners_dols_13_24m` decimal(6,0),                                             |
|   `r2_gmas_trinks_cat_buyrid_lf` decimal(1,0),                                              |
|   `r2_gmas_trinks_cat_prts_lf` decimal(3,0),                                                |
|   `r2_gmas_trinks_cat_txns_lf` decimal(3,0),                                                |
|   `r2_gmas_trinks_cat_dols_lf` decimal(6,0),                                                |
|   `r2_gmas_trinks_cat_recency` decimal(6,0),                                                |
|   `r2_gmas_trinks_cat_prts_0_12m` decimal(3,0),                                             |
|   `r2_gmas_trinks_cat_txns_0_12m` decimal(3,0),                                             |
|   `r2_gmas_trinks_cat_dols_0_12m` decimal(6,0),                                             |
|   `r2_gmas_trinks_cat_prts_13_24m` decimal(3,0),                                            |
|   `r2_gmas_trinks_cat_txns_13_24m` decimal(3,0),                                            |
|   `r2_gmas_trinks_cat_dols_13_24m` decimal(6,0),                                            |
|   `r3_metal_detectors_buyrid_lf` decimal(1,0),                                              |
|   `r3_metal_detectors_prts_lf` decimal(3,0),                                                |
|   `r3_metal_detectors_txns_lf` decimal(3,0),                                                |
|   `r3_metal_detectors_dols_lf` decimal(6,0),                                                |
|   `r3_metal_detectors_recency` decimal(6,0),                                                |
|   `r3_metal_detectors_prts_0_12m` decimal(3,0),                                             |
|   `r3_metal_detectors_txns_0_12m` decimal(3,0),                                             |
|   `r3_metal_detectors_dols_0_12m` decimal(6,0),                                             |
|   `r3_metal_detectors_prts_13_24m` decimal(3,0),                                            |
|   `r3_metal_detectors_txns_13_24m` decimal(3,0),                                            |
|   `r3_metal_detectors_dols_13_24m` decimal(6,0),                                            |
|   `r4_fountn_of_youth_buyrid_lf` decimal(1,0),                                              |
|   `r4_fountn_of_youth_prts_lf` decimal(3,0),                                                |
|   `r4_fountn_of_youth_txns_lf` decimal(3,0),                                                |
|   `r4_fountn_of_youth_dols_lf` decimal(6,0),                                                |
|   `r4_fountn_of_youth_recency` decimal(6,0),                                                |
|   `r4_fountn_of_youth_prts_0_12m` decimal(3,0),                                             |
|   `r4_fountn_of_youth_txns_0_12m` decimal(3,0),                                             |
|   `r4_fountn_of_youth_dols_0_12m` decimal(6,0),                                             |
|   `r4_fountn_of_youth_prts_13_24m` decimal(3,0),                                            |
|   `r4_fountn_of_youth_txns_13_24m` decimal(3,0),                                            |
|   `r4_fountn_of_youth_dols_13_24m` decimal(6,0),                                            |
|   `r5_lucys_laptop_buyrid_lf` decimal(1,0),                                                 |
|   `r5_lucys_laptop_prts_lf` decimal(3,0),                                                   |
|   `r5_lucys_laptop_txns_lf` decimal(3,0),                                                   |
|   `r5_lucys_laptop_dols_lf` decimal(6,0),                                                   |
|   `r5_lucys_laptop_recency` decimal(6,0),                                                   |
|   `r5_lucys_laptop_prts_0_12m` decimal(3,0),                                                |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `r5_lucys_laptop_txns_0_12m` decimal(3,0),                                                |
|   `r5_lucys_laptop_dols_0_12m` decimal(6,0),                                                |
|   `r5_lucys_laptop_prts_13_24m` decimal(3,0),                                               |
|   `r5_lucys_laptop_txns_13_24m` decimal(3,0),                                               |
|   `r5_lucys_laptop_dols_13_24m` decimal(6,0),                                               |
|   `r6_zensational_onl_buyrid_lf` decimal(1,0),                                              |
|   `r6_zensational_onl_prts_lf` decimal(3,0),                                                |
|   `r6_zensational_onl_txns_lf` decimal(3,0),                                                |
|   `r6_zensational_onl_dols_lf` decimal(6,0),                                                |
|   `r6_zensational_onl_recency` decimal(6,0),                                                |
|   `r6_zensational_onl_prts_0_12m` decimal(3,0),                                             |
|   `r6_zensational_onl_txns_0_12m` decimal(3,0),                                             |
|   `r6_zensational_onl_dols_0_12m` decimal(6,0),                                             |
|   `r6_zensational_onl_prts_13_24m` decimal(3,0),                                            |
|   `r6_zensational_onl_txns_13_24m` decimal(3,0),                                            |
|   `r6_zensational_onl_dols_13_24m` decimal(6,0),                                            |
|   `r7_kd_first_buyrid_lf` decimal(1,0),                                                     |
|   `r7_kd_first_prts_lf` decimal(3,0),                                                       |
|   `r7_kd_first_txns_lf` decimal(3,0),                                                       |
|   `r7_kd_first_dols_lf` decimal(6,0),                                                       |
|   `r7_kd_first_recency` decimal(6,0),                                                       |
|   `r7_kd_first_prts_0_12m` decimal(3,0),                                                    |
|   `r7_kd_first_txns_0_12m` decimal(3,0),                                                    |
|   `r7_kd_first_dols_0_12m` decimal(6,0),                                                    |
|   `r7_kd_first_prts_13_24m` decimal(3,0),                                                   |
|   `r7_kd_first_txns_13_24m` decimal(3,0),                                                   |
|   `r7_kd_first_dols_13_24m` decimal(6,0),                                                   |
|   `r8_zensational_cat_buyrid_lf` decimal(1,0),                                              |
|   `r8_zensational_cat_prts_lf` decimal(3,0),                                                |
|   `r8_zensational_cat_txns_lf` decimal(3,0),                                                |
|   `r8_zensational_cat_dols_lf` decimal(6,0),                                                |
|   `r8_zensational_cat_recency` decimal(6,0),                                                |
|   `r8_zensational_cat_prts_0_12m` decimal(3,0),                                             |
|   `r8_zensational_cat_txns_0_12m` decimal(3,0),                                             |
|   `r8_zensational_cat_dols_0_12m` decimal(6,0),                                             |
|   `r8_zensational_cat_prts_13_24m` decimal(3,0),                                            |
|   `r8_zensational_cat_txns_13_24m` decimal(3,0),                                            |
|   `r8_zensational_cat_dols_13_24m` decimal(6,0),                                            |
|   `r9_claras_comf_cat_buyrid_lf` decimal(1,0),                                              |
|   `r9_claras_comf_cat_prts_lf` decimal(3,0),                                                |
|   `r9_claras_comf_cat_txns_lf` decimal(3,0),                                                |
|   `r9_claras_comf_cat_dols_lf` decimal(6,0),                                                |
|   `r9_claras_comf_cat_recency` decimal(6,0),                                                |
|   `r9_claras_comf_cat_prts_0_12m` decimal(3,0),                                             |
|   `r9_claras_comf_cat_txns_0_12m` decimal(3,0),                                             |
|   `r9_claras_comf_cat_dols_0_12m` decimal(6,0),                                             |
|   `r9_claras_comf_cat_prts_13_24m` decimal(3,0),                                            |
|   `r9_claras_comf_cat_txns_13_24m` decimal(3,0),                                            |
|   `r9_claras_comf_cat_dols_13_24m` decimal(6,0),                                            |
|   `r10_save_the_world_buyrid_lf` decimal(1,0),                                              |
|   `r10_save_the_world_prts_lf` decimal(3,0),                                                |
|   `r10_save_the_world_txns_lf` decimal(3,0),                                                |
|   `r10_save_the_world_dols_lf` decimal(6,0),                                                |
|   `r10_save_the_world_recency` decimal(6,0),                                                |
|   `r10_save_the_world_prts_0_12m` decimal(3,0),                                             |
|   `r10_save_the_world_txns_0_12m` decimal(3,0),                                             |
|   `r10_save_the_world_dols_0_12m` decimal(6,0),                                             |
|   `r10_save_the_world_prts_13_24m` decimal(3,0),                                            |
|   `r10_save_the_world_txns_13_24m` decimal(3,0),                                            |
|   `r10_save_the_world_dols_13_24m` decimal(6,0),                                            |
|   `r11_hlth_wlth_rpt_buyrid_lf` decimal(1,0),                                               |
|   `r11_hlth_wlth_rpt_prts_lf` decimal(3,0),                                                 |
|   `r11_hlth_wlth_rpt_txns_lf` decimal(3,0),                                                 |
|   `r11_hlth_wlth_rpt_dols_lf` decimal(6,0),                                                 |
|   `r11_hlth_wlth_rpt_recency` decimal(6,0),                                                 |
|   `r11_hlth_wlth_rpt_prts_0_12m` decimal(3,0),                                              |
|   `r11_hlth_wlth_rpt_txns_0_12m` decimal(3,0),                                              |
|   `r11_hlth_wlth_rpt_dols_0_12m` decimal(6,0),                                              |
|   `r11_hlth_wlth_rpt_prts_13_24m` decimal(3,0),                                             |
|   `r11_hlth_wlth_rpt_txns_13_24m` decimal(3,0),                                             |
|   `r11_hlth_wlth_rpt_dols_13_24m` decimal(6,0),                                             |
|   `r12_senior_comfs_buyrid_lf` decimal(1,0),                                                |
|   `r12_senior_comfs_prts_lf` decimal(3,0),                                                  |
|   `r12_senior_comfs_txns_lf` decimal(3,0),                                                  |
|   `r12_senior_comfs_dols_lf` decimal(6,0),                                                  |
|   `r12_senior_comfs_recency` decimal(6,0),                                                  |
|   `r12_senior_comfs_prts_0_12m` decimal(3,0),                                               |
|   `r12_senior_comfs_txns_0_12m` decimal(3,0),                                               |
|   `r12_senior_comfs_dols_0_12m` decimal(6,0),                                               |
|   `r12_senior_comfs_prts_13_24m` decimal(3,0),                                              |
|   `r12_senior_comfs_txns_13_24m` decimal(3,0),                                              |
|   `r12_senior_comfs_dols_13_24m` decimal(6,0),                                              |
|   `r13_regal_rug_rats_buyrid_lf` decimal(1,0),                                              |
|   `r13_regal_rug_rats_prts_lf` decimal(3,0),                                                |
|   `r13_regal_rug_rats_txns_lf` decimal(3,0),                                                |
|   `r13_regal_rug_rats_dols_lf` decimal(6,0),                                                |
|   `r13_regal_rug_rats_recency` decimal(6,0),                                                |
|   `r13_regal_rug_rats_prts_0_12m` decimal(3,0),                                             |
|   `r13_regal_rug_rats_txns_0_12m` decimal(3,0),                                             |
|   `r13_regal_rug_rats_dols_0_12m` decimal(6,0),                                             |
|   `r13_regal_rug_rats_prts_13_24m` decimal(3,0),                                            |
|   `r13_regal_rug_rats_txns_13_24m` decimal(3,0),                                            |
|   `r13_regal_rug_rats_dols_13_24m` decimal(6,0),                                            |
|   `r14_world_guards_buyrid_lf` decimal(1,0),                                                |
|   `r14_world_guards_prts_lf` decimal(3,0),                                                  |
|   `r14_world_guards_txns_lf` decimal(3,0),                                                  |
|   `r14_world_guards_dols_lf` decimal(6,0),                                                  |
|   `r14_world_guards_recency` decimal(6,0),                                                  |
|   `r14_world_guards_prts_0_12m` decimal(3,0),                                               |
|   `r14_world_guards_txns_0_12m` decimal(3,0),                                               |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `r14_world_guards_dols_0_12m` decimal(6,0),                                               |
|   `r14_world_guards_prts_13_24m` decimal(3,0),                                              |
|   `r14_world_guards_txns_13_24m` decimal(3,0),                                              |
|   `r14_world_guards_dols_13_24m` decimal(6,0),                                              |
|   `r15_putonmytab_cat_buyrid_lf` decimal(1,0),                                              |
|   `r15_putonmytab_cat_prts_lf` decimal(3,0),                                                |
|   `r15_putonmytab_cat_txns_lf` decimal(3,0),                                                |
|   `r15_putonmytab_cat_dols_lf` decimal(6,0),                                                |
|   `r15_putonmytab_cat_recency` decimal(6,0),                                                |
|   `r15_putonmytab_cat_prts_0_12m` decimal(3,0),                                             |
|   `r15_putonmytab_cat_txns_0_12m` decimal(3,0),                                             |
|   `r15_putonmytab_cat_dols_0_12m` decimal(6,0),                                             |
|   `r15_putonmytab_cat_prts_13_24m` decimal(3,0),                                            |
|   `r15_putonmytab_cat_txns_13_24m` decimal(3,0),                                            |
|   `r15_putonmytab_cat_dols_13_24m` decimal(6,0),                                            |
|   `r16_the_huntsman_buyrid_lf` decimal(1,0),                                                |
|   `r16_the_huntsman_prts_lf` decimal(3,0),                                                  |
|   `r16_the_huntsman_txns_lf` decimal(3,0),                                                  |
|   `r16_the_huntsman_dols_lf` decimal(6,0),                                                  |
|   `r16_the_huntsman_recency` decimal(6,0),                                                  |
|   `r16_the_huntsman_prts_0_12m` decimal(3,0),                                               |
|   `r16_the_huntsman_txns_0_12m` decimal(3,0),                                               |
|   `r16_the_huntsman_dols_0_12m` decimal(6,0),                                               |
|   `r16_the_huntsman_prts_13_24m` decimal(3,0),                                              |
|   `r16_the_huntsman_txns_13_24m` decimal(3,0),                                              |
|   `r16_the_huntsman_dols_13_24m` decimal(6,0),                                              |
|   `r17_just_because_buyrid_lf` decimal(1,0),                                                |
|   `r17_just_because_prts_lf` decimal(3,0),                                                  |
|   `r17_just_because_txns_lf` decimal(3,0),                                                  |
|   `r17_just_because_dols_lf` decimal(6,0),                                                  |
|   `r17_just_because_recency` decimal(6,0),                                                  |
|   `r17_just_because_prts_0_12m` decimal(3,0),                                               |
|   `r17_just_because_txns_0_12m` decimal(3,0),                                               |
|   `r17_just_because_dols_0_12m` decimal(6,0),                                               |
|   `r17_just_because_prts_13_24m` decimal(3,0),                                              |
|   `r17_just_because_txns_13_24m` decimal(3,0),                                              |
|   `r17_just_because_dols_13_24m` decimal(6,0),                                              |
|   `r18_recipe_box_buyrid_lf` decimal(1,0),                                                  |
|   `r18_recipe_box_prts_lf` decimal(3,0),                                                    |
|   `r18_recipe_box_txns_lf` decimal(3,0),                                                    |
|   `r18_recipe_box_dols_lf` decimal(6,0),                                                    |
|   `r18_recipe_box_recency` decimal(6,0),                                                    |
|   `r18_recipe_box_prts_0_12m` decimal(3,0),                                                 |
|   `r18_recipe_box_txns_0_12m` decimal(3,0),                                                 |
|   `r18_recipe_box_dols_0_12m` decimal(6,0),                                                 |
|   `r18_recipe_box_prts_13_24m` decimal(3,0),                                                |
|   `r18_recipe_box_txns_13_24m` decimal(3,0),                                                |
|   `r18_recipe_box_dols_13_24m` decimal(6,0),                                                |
|   `r19_hr_glass_fash_buyrid_lf` decimal(1,0),                                               |
|   `r19_hr_glass_fash_prts_lf` decimal(3,0),                                                 |
|   `r19_hr_glass_fash_txns_lf` decimal(3,0),                                                 |
|   `r19_hr_glass_fash_dols_lf` decimal(6,0),                                                 |
|   `r19_hr_glass_fash_recency` decimal(6,0),                                                 |
|   `r19_hr_glass_fash_prts_0_12m` decimal(3,0),                                              |
|   `r19_hr_glass_fash_txns_0_12m` decimal(3,0),                                              |
|   `r19_hr_glass_fash_dols_0_12m` decimal(6,0),                                              |
|   `r19_hr_glass_fash_prts_13_24m` decimal(3,0),                                             |
|   `r19_hr_glass_fash_txns_13_24m` decimal(3,0),                                             |
|   `r19_hr_glass_fash_dols_13_24m` decimal(6,0),                                             |
|   `r20_read_hlthofit_buyrid_lf` decimal(1,0),                                               |
|   `r20_read_hlthofit_prts_lf` decimal(3,0),                                                 |
|   `r20_read_hlthofit_txns_lf` decimal(3,0),                                                 |
|   `r20_read_hlthofit_dols_lf` decimal(6,0),                                                 |
|   `r20_read_hlthofit_recency` decimal(6,0),                                                 |
|   `r20_read_hlthofit_prts_0_12m` decimal(3,0),                                              |
|   `r20_read_hlthofit_txns_0_12m` decimal(3,0),                                              |
|   `r20_read_hlthofit_dols_0_12m` decimal(6,0),                                              |
|   `r20_read_hlthofit_prts_13_24m` decimal(3,0),                                             |
|   `r20_read_hlthofit_txns_13_24m` decimal(3,0),                                             |
|   `r20_read_hlthofit_dols_13_24m` decimal(6,0),                                             |
|   `r21_grandpas_den_buyrid_lf` decimal(1,0),                                                |
|   `r21_grandpas_den_prts_lf` decimal(3,0),                                                  |
|   `r21_grandpas_den_txns_lf` decimal(3,0),                                                  |
|   `r21_grandpas_den_dols_lf` decimal(6,0),                                                  |
|   `r21_grandpas_den_recency` decimal(6,0),                                                  |
|   `r21_grandpas_den_prts_0_12m` decimal(3,0),                                               |
|   `r21_grandpas_den_txns_0_12m` decimal(3,0),                                               |
|   `r21_grandpas_den_dols_0_12m` decimal(6,0),                                               |
|   `r21_grandpas_den_prts_13_24m` decimal(3,0),                                              |
|   `r21_grandpas_den_txns_13_24m` decimal(3,0),                                              |
|   `r21_grandpas_den_dols_13_24m` decimal(6,0),                                              |
|   `r22_reel_it_in_onl_buyrid_lf` decimal(1,0),                                              |
|   `r22_reel_it_in_onl_prts_lf` decimal(3,0),                                                |
|   `r22_reel_it_in_onl_txns_lf` decimal(3,0),                                                |
|   `r22_reel_it_in_onl_dols_lf` decimal(6,0),                                                |
|   `r22_reel_it_in_onl_recency` decimal(6,0),                                                |
|   `r22_reel_it_in_onl_prts_0_12m` decimal(3,0),                                             |
|   `r22_reel_it_in_onl_txns_0_12m` decimal(3,0),                                             |
|   `r22_reel_it_in_onl_dols_0_12m` decimal(6,0),                                             |
|   `r22_reel_it_in_onl_prts_13_24m` decimal(3,0),                                            |
|   `r22_reel_it_in_onl_txns_13_24m` decimal(3,0),                                            |
|   `r22_reel_it_in_onl_dols_13_24m` decimal(6,0),                                            |
|   `r23_uplifting_buyrid_lf` decimal(1,0),                                                   |
|   `r23_uplifting_prts_lf` decimal(3,0),                                                     |
|   `r23_uplifting_txns_lf` decimal(3,0),                                                     |
|   `r23_uplifting_dols_lf` decimal(6,0),                                                     |
|   `r23_uplifting_recency` decimal(6,0),                                                     |
|   `r23_uplifting_prts_0_12m` decimal(3,0),                                                  |
|   `r23_uplifting_txns_0_12m` decimal(3,0),                                                  |
|   `r23_uplifting_dols_0_12m` decimal(6,0),                                                  |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `r23_uplifting_prts_13_24m` decimal(3,0),                                                 |
|   `r23_uplifting_txns_13_24m` decimal(3,0),                                                 |
|   `r23_uplifting_dols_13_24m` decimal(6,0),                                                 |
|   `r24_putonmytab_onl_buyrid_lf` decimal(1,0),                                              |
|   `r24_putonmytab_onl_prts_lf` decimal(3,0),                                                |
|   `r24_putonmytab_onl_txns_lf` decimal(3,0),                                                |
|   `r24_putonmytab_onl_dols_lf` decimal(6,0),                                                |
|   `r24_putonmytab_onl_recency` decimal(6,0),                                                |
|   `r24_putonmytab_onl_prts_0_12m` decimal(3,0),                                             |
|   `r24_putonmytab_onl_txns_0_12m` decimal(3,0),                                             |
|   `r24_putonmytab_onl_dols_0_12m` decimal(6,0),                                             |
|   `r24_putonmytab_onl_prts_13_24m` decimal(3,0),                                            |
|   `r24_putonmytab_onl_txns_13_24m` decimal(3,0),                                            |
|   `r24_putonmytab_onl_dols_13_24m` decimal(6,0),                                            |
|   `r25_grow_goodness_buyrid_lf` decimal(1,0),                                               |
|   `r25_grow_goodness_prts_lf` decimal(3,0),                                                 |
|   `r25_grow_goodness_txns_lf` decimal(3,0),                                                 |
|   `r25_grow_goodness_dols_lf` decimal(6,0),                                                 |
|   `r25_grow_goodness_recency` decimal(6,0),                                                 |
|   `r25_grow_goodness_prts_0_12m` decimal(3,0),                                              |
|   `r25_grow_goodness_txns_0_12m` decimal(3,0),                                              |
|   `r25_grow_goodness_dols_0_12m` decimal(6,0),                                              |
|   `r25_grow_goodness_prts_13_24m` decimal(3,0),                                             |
|   `r25_grow_goodness_txns_13_24m` decimal(3,0),                                             |
|   `r25_grow_goodness_dols_13_24m` decimal(6,0),                                             |
|   `r26_hi_end_for_him_buyrid_lf` decimal(1,0),                                              |
|   `r26_hi_end_for_him_prts_lf` decimal(3,0),                                                |
|   `r26_hi_end_for_him_txns_lf` decimal(3,0),                                                |
|   `r26_hi_end_for_him_dols_lf` decimal(6,0),                                                |
|   `r26_hi_end_for_him_recency` decimal(6,0),                                                |
|   `r26_hi_end_for_him_prts_0_12m` decimal(3,0),                                             |
|   `r26_hi_end_for_him_txns_0_12m` decimal(3,0),                                             |
|   `r26_hi_end_for_him_dols_0_12m` decimal(6,0),                                             |
|   `r26_hi_end_for_him_prts_13_24m` decimal(3,0),                                            |
|   `r26_hi_end_for_him_txns_13_24m` decimal(3,0),                                            |
|   `r26_hi_end_for_him_dols_13_24m` decimal(6,0),                                            |
|   `r27_fashion_fix_buyrid_lf` decimal(1,0),                                                 |
|   `r27_fashion_fix_prts_lf` decimal(3,0),                                                   |
|   `r27_fashion_fix_txns_lf` decimal(3,0),                                                   |
|   `r27_fashion_fix_dols_lf` decimal(6,0),                                                   |
|   `r27_fashion_fix_recency` decimal(6,0),                                                   |
|   `r27_fashion_fix_prts_0_12m` decimal(3,0),                                                |
|   `r27_fashion_fix_txns_0_12m` decimal(3,0),                                                |
|   `r27_fashion_fix_dols_0_12m` decimal(6,0),                                                |
|   `r27_fashion_fix_prts_13_24m` decimal(3,0),                                               |
|   `r27_fashion_fix_txns_13_24m` decimal(3,0),                                               |
|   `r27_fashion_fix_dols_13_24m` decimal(6,0),                                               |
|   `r28_hit_the_trail_buyrid_lf` decimal(1,0),                                               |
|   `r28_hit_the_trail_prts_lf` decimal(3,0),                                                 |
|   `r28_hit_the_trail_txns_lf` decimal(3,0),                                                 |
|   `r28_hit_the_trail_dols_lf` decimal(6,0),                                                 |
|   `r28_hit_the_trail_recency` decimal(6,0),                                                 |
|   `r28_hit_the_trail_prts_0_12m` decimal(3,0),                                              |
|   `r28_hit_the_trail_txns_0_12m` decimal(3,0),                                              |
|   `r28_hit_the_trail_dols_0_12m` decimal(6,0),                                              |
|   `r28_hit_the_trail_prts_13_24m` decimal(3,0),                                             |
|   `r28_hit_the_trail_txns_13_24m` decimal(3,0),                                             |
|   `r28_hit_the_trail_dols_13_24m` decimal(6,0),                                             |
|   `r29_hi_end_nesters_buyrid_lf` decimal(1,0),                                              |
|   `r29_hi_end_nesters_prts_lf` decimal(3,0),                                                |
|   `r29_hi_end_nesters_txns_lf` decimal(3,0),                                                |
|   `r29_hi_end_nesters_dols_lf` decimal(6,0),                                                |
|   `r29_hi_end_nesters_recency` decimal(6,0),                                                |
|   `r29_hi_end_nesters_prts_0_12m` decimal(3,0),                                             |
|   `r29_hi_end_nesters_txns_0_12m` decimal(3,0),                                             |
|   `r29_hi_end_nesters_dols_0_12m` decimal(6,0),                                             |
|   `r29_hi_end_nesters_prts_13_24m` decimal(3,0),                                            |
|   `r29_hi_end_nesters_txns_13_24m` decimal(3,0),                                            |
|   `r29_hi_end_nesters_dols_13_24m` decimal(6,0),                                            |
|   `r30_manly_mag_buyrid_lf` decimal(1,0),                                                   |
|   `r30_manly_mag_prts_lf` decimal(3,0),                                                     |
|   `r30_manly_mag_txns_lf` decimal(3,0),                                                     |
|   `r30_manly_mag_dols_lf` decimal(6,0),                                                     |
|   `r30_manly_mag_recency` decimal(6,0),                                                     |
|   `r30_manly_mag_prts_0_12m` decimal(3,0),                                                  |
|   `r30_manly_mag_txns_0_12m` decimal(3,0),                                                  |
|   `r30_manly_mag_dols_0_12m` decimal(6,0),                                                  |
|   `r30_manly_mag_prts_13_24m` decimal(3,0),                                                 |
|   `r30_manly_mag_txns_13_24m` decimal(3,0),                                                 |
|   `r30_manly_mag_dols_13_24m` decimal(6,0),                                                 |
|   `r31_mighty_me_buyrid_lf` decimal(1,0),                                                   |
|   `r31_mighty_me_prts_lf` decimal(3,0),                                                     |
|   `r31_mighty_me_txns_lf` decimal(3,0),                                                     |
|   `r31_mighty_me_dols_lf` decimal(6,0),                                                     |
|   `r31_mighty_me_recency` decimal(6,0),                                                     |
|   `r31_mighty_me_prts_0_12m` decimal(3,0),                                                  |
|   `r31_mighty_me_txns_0_12m` decimal(3,0),                                                  |
|   `r31_mighty_me_dols_0_12m` decimal(6,0),                                                  |
|   `r31_mighty_me_prts_13_24m` decimal(3,0),                                                 |
|   `r31_mighty_me_txns_13_24m` decimal(3,0),                                                 |
|   `r31_mighty_me_dols_13_24m` decimal(6,0),                                                 |
|   `r32_young_at_heart_buyrid_lf` decimal(1,0),                                              |
|   `r32_young_at_heart_prts_lf` decimal(3,0),                                                |
|   `r32_young_at_heart_txns_lf` decimal(3,0),                                                |
|   `r32_young_at_heart_dols_lf` decimal(6,0),                                                |
|   `r32_young_at_heart_recency` decimal(6,0),                                                |
|   `r32_young_at_heart_prts_0_12m` decimal(3,0),                                             |
|   `r32_young_at_heart_txns_0_12m` decimal(3,0),                                             |
|   `r32_young_at_heart_dols_0_12m` decimal(6,0),                                             |
|   `r32_young_at_heart_prts_13_24m` decimal(3,0),                                            |
+---------------------------------------------------------------------------------------------+--+
|                                       createtab_stmt                                        |
+---------------------------------------------------------------------------------------------+--+
|   `r32_young_at_heart_txns_13_24m` decimal(3,0),                                            |
|   `r32_young_at_heart_dols_13_24m` decimal(6,0),                                            |
|   `r33_the_library_buyrid_lf` decimal(1,0),                                                 |
|   `r33_the_library_prts_lf` decimal(3,0),                                                   |
|   `r33_the_library_txns_lf` decimal(3,0),                                                   |
|   `r33_the_library_dols_lf` decimal(6,0),                                                   |
|   `r33_the_library_recency` decimal(6,0),                                                   |
|   `r33_the_library_prts_0_12m` decimal(3,0),                                                |
|   `r33_the_library_txns_0_12m` decimal(3,0),                                                |
|   `r33_the_library_dols_0_12m` decimal(6,0),                                                |
|   `r33_the_library_prts_13_24m` decimal(3,0),                                               |
|   `r33_the_library_txns_13_24m` decimal(3,0),                                               |
|   `r33_the_library_dols_13_24m` decimal(6,0),                                               |
|   `r34_farmstead_buyrid_lf` decimal(1,0),                                                   |
|   `r34_farmstead_prts_lf` decimal(3,0),                                                     |
|   `r34_farmstead_txns_lf` decimal(3,0),                                                     |
|   `r34_farmstead_dols_lf` decimal(6,0),                                                     |
|   `r34_farmstead_recency` decimal(6,0),                                                     |
|   `r34_farmstead_prts_0_12m` decimal(3,0),                                                  |
|   `r34_farmstead_txns_0_12m` decimal(3,0),                                                  |
|   `r34_farmstead_dols_0_12m` decimal(6,0),                                                  |
|   `r34_farmstead_prts_13_24m` decimal(3,0),                                                 |
|   `r34_farmstead_txns_13_24m` decimal(3,0),                                                 |
|   `r34_farmstead_dols_13_24m` decimal(6,0),                                                 |
|   `r35_cottage_crafts_buyrid_lf` decimal(1,0),                                              |
|   `r35_cottage_crafts_prts_lf` decimal(3,0),                                                |
|   `r35_cottage_crafts_txns_lf` decimal(3,0),                                                |
|   `r35_cottage_crafts_dols_lf` decimal(6,0),                                                |
|   `r35_cottage_crafts_recency` decimal(6,0),                                                |
|   `r35_cottage_crafts_prts_0_12m` decimal(3,0),                                             |
|   `r35_cottage_crafts_txns_0_12m` decimal(3,0),                                             |
|   `r35_cottage_crafts_dols_0_12m` decimal(6,0),                                             |
|   `r35_cottage_crafts_prts_13_24m` decimal(3,0),                                            |
|   `r35_cottage_crafts_txns_13_24m` decimal(3,0),                                            |
|   `r35_cottage_crafts_dols_13_24m` decimal(6,0),                                            |
|   `c1_at_buyrid_lf` decimal(1,0),                                                           |
|   `c1_at_prts_lf` decimal(3,0),                                                             |
|   `c1_at_txns_lf` decimal(3,0),                                                             |
|   `c1_at_dols_lf` decimal(6,0),                                                             |
|   `c1_at_recency` decimal(6,0),                                                             |
|   `c1_at_prts_0_12m` decimal(3,0),                                                          |
|   `c1_at_txns_0_12m` decimal(3,0),                                                          |
|   `c1_at_dols_0_12m` decimal(6,0),                                                          |
|   `c1_at_prts_13_24m` decimal(3,0),                                                         |
|   `c1_at_txns_13_24m` decimal(3,0),                                                         |
|   `c1_at_dols_13_24m` decimal(6,0),                                                         |
|   `c2_loft_buyrid_lf` decimal(1,0),                                                         |
|   `c2_loft_prts_lf` decimal(3,0),                                                           |
|   `c2_loft_txns_lf` decimal(3,0),                                                           |
|   `c2_loft_dols_lf` decimal(6,0),                                                           |
|   `c2_loft_recency` decimal(6,0),                                                           |
|   `c2_loft_prts_0_12m` decimal(3,0),                                                        |
|   `c2_loft_txns_0_12m` decimal(3,0),                                                        |
|   `c2_loft_dols_0_12m` decimal(6,0),                                                        |
|   `c2_loft_prts_13_24m` decimal(3,0),                                                       |
|   `c2_loft_txns_13_24m` decimal(3,0),                                                       |
|   `c2_loft_dols_13_24m` decimal(6,0),                                                       |
|   `c3_at_comp_cat_buyrid_lf` decimal(1,0),                                                  |
|   `c3_at_comp_cat_prts_lf` decimal(3,0),                                                    |
|   `c3_at_comp_cat_txns_lf` decimal(3,0),                                                    |
|   `c3_at_comp_cat_dols_lf` decimal(6,0),                                                    |
|   `c3_at_comp_cat_recency` decimal(6,0),                                                    |
|   `c3_at_comp_cat_prts_0_12m` decimal(3,0),                                                 |
|   `c3_at_comp_cat_txns_0_12m` decimal(3,0),                                                 |
|   `c3_at_comp_cat_dols_0_12m` decimal(6,0),                                                 |
|   `c3_at_comp_cat_prts_13_24m` decimal(3,0),                                                |
|   `c3_at_comp_cat_txns_13_24m` decimal(3,0),                                                |
|   `c3_at_comp_cat_dols_13_24m` decimal(6,0),                                                |
|   `c4_loft_comp_cat_buyrid_lf` decimal(1,0),                                                |
|   `c4_loft_comp_cat_prts_lf` decimal(3,0),                                                  |
|   `c4_loft_comp_cat_txns_lf` decimal(3,0),                                                  |
|   `c4_loft_comp_cat_dols_lf` decimal(6,0),                                                  |
|   `c4_loft_comp_cat_recency` decimal(6,0),                                                  |
|   `c4_loft_comp_cat_prts_0_12m` decimal(3,0),                                               |
|   `c4_loft_comp_cat_txns_0_12m` decimal(3,0),                                               |
|   `c4_loft_comp_cat_dols_0_12m` decimal(6,0),                                               |
|   `c4_loft_comp_cat_prts_13_24m` decimal(3,0),                                              |
|   `c4_loft_comp_cat_txns_13_24m` decimal(3,0),                                              |
|   `c4_loft_comp_cat_dols_13_24m` decimal(6,0))                                              |
| ROW FORMAT DELIMITED                                                                        |
|   FIELDS TERMINATED BY '|'                                                                  |
| STORED AS INPUTFORMAT                                                                       |
|   'org.apache.hadoop.mapred.TextInputFormat'                                                |
| OUTPUTFORMAT                                                                                |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                              |
| LOCATION                                                                                    |
|   'hdfs://ascenaprod/apps/hive/warehouse/ascena_staging.db/loft_f_abacus_data_overlay2_vw'  |
| TBLPROPERTIES (                                                                             |
|   'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}',                                     |
|   'numFiles'='0',                                                                           |
|   'numRows'='0',                                                                            |
|   'rawDataSize'='0',                                                                        |
|   'serialization.null.format'='',                                                           |
|   'skip.header.line.count'='1',                                                             |
|   'totalSize'='0',                                                                          |
|   'transient_lastDdlTime'='1564751046')                                                     |
+---------------------------------------------------------------------------------------------+--+
+------------------------------------------------------------------------------------+--+
|                                   createtab_stmt                                   |
+------------------------------------------------------------------------------------+--+
| CREATE TABLE `loft_f_brand_email_vw`(                                              |
|   `email_id` decimal(10,0),                                                        |
|   `brand_cd` string,                                                               |
|   `email_addr` string,                                                             |
|   `first_nm` string,                                                               |
|   `last_nm` string,                                                                |
|   `postal_cd` string,                                                              |
|   `email_domain` string,                                                           |
|   `valid_ind` decimal(1,0),                                                        |
|   `corrected_ind` decimal(1,0),                                                    |
|   `undeliverable_ind` decimal(1,0),                                                |
|   `undeliverable_dt` timestamp,                                                    |
|   `nuclear_optout_ind` decimal(1,0),                                               |
|   `primary_list_id` decimal(10,0),                                                 |
|   `primary_optout_ind` decimal(1,0),                                               |
|   `primary_sub_chg_dt` timestamp,                                                  |
|   `primary_optout_reason_cd` string,                                               |
|   `teacher_list_id` decimal(10,0),                                                 |
|   `teacher_optout_ind` decimal(1,0),                                               |
|   `teacher_sub_chg_dt` timestamp,                                                  |
|   `teacer_optout_reason_cd` string,                                                |
|   `ca_list_id` decimal(10,0),                                                      |
|   `ca_optout_ind` decimal(1,0),                                                    |
|   `ca_sub_chg_dt` timestamp,                                                       |
|   `ca_optout_reason_cd` string,                                                    |
|   `livelove_list_id` decimal(10,0),                                                |
|   `livelove_optout_ind` decimal(1,0),                                              |
|   `livelove_sub_chg_dt` timestamp,                                                 |
|   `livelove_optout_reason_cd` string,                                              |
|   `at_browse_remarket_dt` timestamp,                                               |
|   `at_last_cat_browse` string,                                                     |
|   `at_remarket_dt` timestamp,                                                      |
|   `atl_freq_cap` decimal(10,0),                                                    |
|   `ats_freq_cap` decimal(10,0),                                                    |
|   `ats_international_ind` decimal(1,0),                                            |
|   `birthday_3rd_party_dt` timestamp,                                               |
|   `birthday_pref_dt` timestamp,                                                    |
|   `can_at_dmail_dt` timestamp,                                                     |
|   `can_loft_dmail_dt` timestamp,                                                   |
|   `canada_at_origpref_dt` timestamp,                                               |
|   `canada_loft_origpref_dt` timestamp,                                             |
|   `customer_no` string,                                                            |
|   `default_at_browse_dt` timestamp,                                                |
|   `default_loft_browse_dt` timestamp,                                              |
|   `dresses_at_browse_dt` timestamp,                                                |
|   `dresses_loft_browse_dt` timestamp,                                              |
|   `email_source` string,                                                           |
|   `form_series` string,                                                            |
|   `form_source_last_at` string,                                                    |
|   `form_source_last_loft` string,                                                  |
|   `form_source_orig_at` string,                                                    |
|   `form_source_orig_loft` string,                                                  |
|   `loft_browse_remarket_dt` timestamp,                                             |
|   `loft_international_flag` decimal(1,0),                                          |
|   `loft_last_cat_browse` string,                                                   |
|   `loft_remarket_dt` timestamp,                                                    |
|   `maternity_browse_dt` timestamp,                                                 |
|   `maternity_dt` timestamp,                                                        |
|   `maternity_due_dt` timestamp,                                                    |
|   `maternity_loft_ind` decimal(1,0),                                               |
|   `maternity_orig_pref_dt` timestamp,                                              |
|   `maternity_pref_dt` timestamp,                                                   |
|   `mobile_optin_ind` decimal(1,0),                                                 |
|   `mobile_phone_nbr` string,                                                       |
|   `newarrival_loft_browse_dt` timestamp,                                           |
|   `newarrivals_at_browse_dt` timestamp,                                            |
|   `pants_at_browse_dt` timestamp,                                                  |
|   `pants_loft_browse_dt` timestamp,                                                |
|   `petite_at_browse_dt` timestamp,                                                 |
|   `petite_at_ind` decimal(1,0),                                                    |
|   `petite_at_pref_dt` timestamp,                                                   |
|   `petite_loft_browse_dt` timestamp,                                               |
|   `petite_loft_ind` decimal(1,0),                                                  |
|   `petite_loft_pref_dt` timestamp,                                                 |
|   `sale_at_browse_dt` timestamp,                                                   |
|   `sale_loft_browse_dt` timestamp,                                                 |
|   `shoes_at_browse_dt` timestamp,                                                  |
|   `shoes_loft_browse_dt` timestamp,                                                |
|   `students_browse_dt` timestamp,                                                  |
|   `students_grad_dt` timestamp,                                                    |
|   `students_ind` decimal(1,0),                                                     |
|   `students_pref_dt` timestamp,                                                    |
|   `suits_at_browse_dt` timestamp,                                                  |
|   `sweaters_at_browse_dt` timestamp,                                               |
|   `sweaters_loft_browse_dt` timestamp,                                             |
|   `swim_loft_browse_dt` timestamp,                                                 |
|   `tall_at_browse_dt` timestamp,                                                   |
|   `tall_at_ind` decimal(1,0),                                                      |
|   `tall_at_pref_dt` timestamp,                                                     |
|   `tall_loft_browse_dt` timestamp,                                                 |
|   `tall_loft_ind` decimal(1,0),                                                    |
|   `tall_loft_pref_dt` timestamp,                                                   |
|   `teachers_browse_dt` timestamp,                                                  |
|   `teachers_ind` decimal(1,0),                                                     |
|   `teachers_level` string,                                                         |
|   `teachers_orig_pref_dt` timestamp,                                               |
|   `teachers_pref_dt` timestamp,                                                    |
|   `teachers_role` string,                                                          |
|   `tops_at_browse_dt` timestamp,                                                   |
|   `tops_loft_browse_dt` timestamp,                                                 |
+------------------------------------------------------------------------------------+--+
|                                   createtab_stmt                                   |
+------------------------------------------------------------------------------------+--+
|   `trans_t2p_at_first_dt` timestamp,                                               |
|   `trans_t2p_at_last_dt` timestamp,                                                |
|   `trans_t2p_loft_first_dt` timestamp,                                             |
|   `trans_t2p_loft_last_dt` timestamp,                                              |
|   `unsub_optdown_at` string,                                                       |
|   `unsub_optdown_loft` string,                                                     |
|   `wedding_at_ind` decimal(1,0),                                                   |
|   `wedding_browse_dt` timestamp,                                                   |
|   `wedding_dt` timestamp,                                                          |
|   `wedding_orig_pref_dt` timestamp,                                                |
|   `wedding_pref_dt` timestamp,                                                     |
|   `wedding_role` string,                                                           |
|   `wedding_source` string)                                                         |
| ROW FORMAT DELIMITED                                                               |
|   FIELDS TERMINATED BY '\t'                                                        |
| STORED AS INPUTFORMAT                                                              |
|   'org.apache.hadoop.mapred.TextInputFormat'                                       |
| OUTPUTFORMAT                                                                       |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                     |
| LOCATION                                                                           |
|   'hdfs://ascenaprod/apps/hive/warehouse/ascena_staging.db/loft_f_brand_email_vw'  |
| TBLPROPERTIES (                                                                    |
|   'numFiles'='2',                                                                  |
|   'numRows'='0',                                                                   |
|   'rawDataSize'='0',                                                               |
|   'serialization.null.format'='',                                                  |
|   'skip.header.line.count'='1',                                                    |
|   'totalSize'='10262310071',                                                       |
|   'transient_lastDdlTime'='1562146427')                                            |
+------------------------------------------------------------------------------------+--+
+--------------------------------------------------------------------------------------+--+
|                                    createtab_stmt                                    |
+--------------------------------------------------------------------------------------+--+
| CREATE TABLE `loft_lu_mw_pref_hist_vw`(                                              |
|   `acct_nbr` string,                                                                 |
|   `acct_source_cd` string,                                                           |
|   `brand_cd` string,                                                                 |
|   `indiv_id` decimal(13,0),                                                          |
|   `hh_id` decimal(13,0),                                                             |
|   `addr_id` string,                                                                  |
|   `pref_lga` decimal(1,0),                                                           |
|   `pref_lgan` decimal(1,0),                                                          |
|   `pref_lgc` decimal(1,0),                                                           |
|   `pref_lgch` decimal(1,0),                                                          |
|   `pref_lgd` decimal(1,0),                                                           |
|   `pref_lgde` decimal(1,0),                                                          |
|   `pref_lgec` decimal(1,0),                                                          |
|   `pref_lgf` decimal(1,0),                                                           |
|   `pref_lgh` decimal(1,0),                                                           |
|   `pref_lgn` decimal(1,0),                                                           |
|   `pref_lgnc` decimal(1,0),                                                          |
|   `pref_lgp` decimal(1,0),                                                           |
|   `pref_lgpa` decimal(1,0),                                                          |
|   `pref_lgr` decimal(1,0),                                                           |
|   `pref_lgs` decimal(1,0),                                                           |
|   `pref_lgsc` decimal(1,0),                                                          |
|   `pref_lgse` decimal(1,0),                                                          |
|   `pref_lgsf` decimal(1,0),                                                          |
|   `pref_lgt` decimal(1,0),                                                           |
|   `pref_teac_admin` decimal(1,0),                                                    |
|   `pref_teac_couns` decimal(1,0),                                                    |
|   `pref_teac_teacher` decimal(1,0),                                                  |
|   `pref_teac_unkno` decimal(1,0),                                                    |
|   `pref_carq` decimal(1,0))                                                          |
| ROW FORMAT DELIMITED                                                                 |
|   FIELDS TERMINATED BY '|'                                                           |
| STORED AS INPUTFORMAT                                                                |
|   'org.apache.hadoop.mapred.TextInputFormat'                                         |
| OUTPUTFORMAT                                                                         |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                       |
| LOCATION                                                                             |
|   'hdfs://ascenaprod/apps/hive/warehouse/ascena_staging.db/loft_lu_mw_pref_hist_vw'  |
| TBLPROPERTIES (                                                                      |
|   'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}',                              |
|   'numFiles'='0',                                                                    |
|   'numRows'='0',                                                                     |
|   'rawDataSize'='0',                                                                 |
|   'serialization.null.format'='',                                                    |
|   'skip.header.line.count'='1',                                                      |
|   'totalSize'='0',                                                                   |
|   'transient_lastDdlTime'='1564738974')                                              |
+--------------------------------------------------------------------------------------+--+
+----------------------------------------------------------------------------------+--+
|                                  createtab_stmt                                  |
+----------------------------------------------------------------------------------+--+
| CREATE TABLE `loft_sum_address_vw`(                                              |
|   `addr_id` string,                                                              |
|   `brand_cd` string)                                                             |
| ROW FORMAT DELIMITED                                                             |
|   FIELDS TERMINATED BY '|'                                                       |
| STORED AS INPUTFORMAT                                                            |
|   'org.apache.hadoop.mapred.TextInputFormat'                                     |
| OUTPUTFORMAT                                                                     |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                   |
| LOCATION                                                                         |
|   'hdfs://ascenaprod/apps/hive/warehouse/ascena_staging.db/loft_sum_address_vw'  |
| TBLPROPERTIES (                                                                  |
|   'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}',                          |
|   'numFiles'='0',                                                                |
|   'numRows'='0',                                                                 |
|   'rawDataSize'='0',                                                             |
|   'serialization.null.format'='',                                                |
|   'skip.header.line.count'='1',                                                  |
|   'totalSize'='0',                                                               |
|   'transient_lastDdlTime'='1564737707')                                          |
+----------------------------------------------------------------------------------+--+
+------------------------------------------------------------------------------------+--+
|                                   createtab_stmt                                   |
+------------------------------------------------------------------------------------+--+
| CREATE TABLE `loft_sum_household_vw`(                                              |
|   `hh_id` decimal(13,0),                                                           |
|   `brand_cd` string,                                                               |
|   `hoh_postal_contact_id` decimal(10,0),                                           |
|   `hoh_acct_source_cd` string,                                                     |
|   `hoh_acct_nbr` string,                                                           |
|   `hoh_indiv_id` decimal(13,0),                                                    |
|   `addr_id` string,                                                                |
|   `hoh_barcode_hash` string,                                                       |
|   `prefix` string,                                                                 |
|   `first_nm` string,                                                               |
|   `middle_nm` string,                                                              |
|   `last_nm` string,                                                                |
|   `mat_sfx` string,                                                                |
|   `prof_sfx` string,                                                               |
|   `firm_nm` string,                                                                |
|   `std_gender_cd` string,                                                          |
|   `birth_dt` timestamp,                                                            |
|   `addr_line_1` string,                                                            |
|   `addr_line_2` string,                                                            |
|   `addr_line_3` string,                                                            |
|   `addr_line_4` string,                                                            |
|   `city` string,                                                                   |
|   `state` string,                                                                  |
|   `country` string,                                                                |
|   `postal_cd` string,                                                              |
|   `zip4` string,                                                                   |
|   `carrier_route_cd` string,                                                       |
|   `delivery_point` string,                                                         |
|   `delivery_point_chk` string,                                                     |
|   `lot_nbr` string,                                                                |
|   `lot_seq` string,                                                                |
|   `vulgar_ind` decimal(1,0),                                                       |
|   `business_ind` decimal(1,0),                                                     |
|   `foreign_ind` decimal(1,0),                                                      |
|   `mail_score` string,                                                             |
|   `cass_deliverable_ind` decimal(1,0),                                             |
|   `prison_ind` decimal(1,0),                                                       |
|   `pander_ind` decimal(1,0),                                                       |
|   `deceased_ind` decimal(1,0),                                                     |
|   `apo_fpo_ind` decimal(1,0),                                                      |
|   `us_possession_ind` decimal(1,0),                                                |
|   `ace_dpv_status` string,                                                         |
|   `ace_dpv_footnote` string,                                                       |
|   `ace_foreign_cd` string,                                                         |
|   `ace_latitude` string,                                                           |
|   `ace_longitude` string,                                                          |
|   `dsf2_vacant_ind` decimal(1,0),                                                  |
|   `dsf2_season_ind` decimal(1,0),                                                  |
|   `dsf2_res_bus_cd` string,                                                        |
|   `dsf2_delivery_type_cd` string,                                                  |
|   `dsf2_del_pt_drop_ind` decimal(1,0),                                             |
|   `agility_dt` timestamp,                                                          |
|   `coa_status_cd` string,                                                          |
|   `coa_applied_dt` timestamp,                                                      |
|   `coa_move_dt` timestamp,                                                         |
|   `ncoa_move_type` string,                                                         |
|   `fips_state` string,                                                             |
|   `fips_county` string,                                                            |
|   `census_tract` string,                                                           |
|   `census_blockgroup` string,                                                      |
|   `geocode` string,                                                                |
|   `occupancy_score` decimal(1,0),                                                  |
|   `dnf_ind` decimal(1,0),                                                          |
|   `nursing_home_ind` decimal(1,0),                                                 |
|   `can_lang_pref` string,                                                          |
|   `usps_geo_id` decimal(10,0),                                                     |
|   `hoh_email_id` decimal(10,0),                                                    |
|   `hoh_email_addr` string,                                                         |
|   `orig_acct_source_cd` string,                                                    |
|   `orig_acct_source_dt` timestamp,                                                 |
|   `home_phone_nbr` string,                                                         |
|   `cell_phone_nbr` string,                                                         |
|   `work_phone_nbr` string,                                                         |
|   `active_associate_ind` string,                                                   |
|   `last_txn_dt` timestamp,                                                         |
|   `first_txn_dt` timestamp,                                                        |
|   `all_12m_freq` decimal(10,0),                                                    |
|   `all_12m_mont` decimal(15,2),                                                    |
|   `all_12m_disc_amt` decimal(15,2),                                                |
|   `all_13_24m_freq` decimal(10,0),                                                 |
|   `all_13_24m_mont` decimal(15,2),                                                 |
|   `all_13_24m_disc_amt` decimal(15,2),                                             |
|   `all_lt_rec` decimal(5,0),                                                       |
|   `all_lt_freq` decimal(10,0),                                                     |
|   `all_lt_mont` decimal(15,2),                                                     |
|   `all_lt_disc_amt` decimal(15,2),                                                 |
|   `ret_last_txn_dt` timestamp,                                                     |
|   `ret_first_txn_dt` timestamp,                                                    |
|   `ret_12m_freq` decimal(10,0),                                                    |
|   `ret_12m_mont` decimal(15,2),                                                    |
|   `ret_12m_disc_amt` decimal(15,2),                                                |
|   `ret_13_24m_freq` decimal(10,0),                                                 |
|   `ret_13_24m_mont` decimal(15,2),                                                 |
|   `ret_13_24m_disc_amt` decimal(15,2),                                             |
|   `ret_lt_rec` decimal(5,0),                                                       |
|   `ret_lt_freq` decimal(10,0),                                                     |
|   `ret_lt_mont` decimal(15,2),                                                     |
|   `ret_lt_disc_amt` decimal(15,2),                                                 |
|   `web_last_txn_dt` timestamp,                                                     |
+------------------------------------------------------------------------------------+--+
|                                   createtab_stmt                                   |
+------------------------------------------------------------------------------------+--+
|   `web_first_txn_dt` timestamp,                                                    |
|   `web_12m_freq` decimal(10,0),                                                    |
|   `web_12m_mont` decimal(15,2),                                                    |
|   `web_12m_disc_amt` decimal(15,2),                                                |
|   `web_13_24m_freq` decimal(10,0),                                                 |
|   `web_13_24m_mont` decimal(15,2),                                                 |
|   `web_13_24m_disc_amt` decimal(15,2),                                             |
|   `web_lt_rec` decimal(5,0),                                                       |
|   `web_lt_freq` decimal(10,0),                                                     |
|   `web_lt_mont` decimal(15,2),                                                     |
|   `web_lt_disc_amt` decimal(15,2),                                                 |
|   `preferred_channel_cd` string,                                                   |
|   `mail_optout_ind` decimal(1,0),                                                  |
|   `email_optout_ind` decimal(1,0),                                                 |
|   `sms_optout_ind` decimal(1,0),                                                   |
|   `phone_optout_ind` decimal(1,0),                                                 |
|   `rent_optout_ind` decimal(1,0),                                                  |
|   `ca_priv_ind` decimal(1,0),                                                      |
|   `promo_12m_dm` decimal(10,0),                                                    |
|   `promo_12m_em` decimal(10,0),                                                    |
|   `marketable_cd` string,                                                          |
|   `rfm_12m_decile` decimal(2,0),                                                   |
|   `rfm_13_24m_decile` decimal(2,0),                                                |
|   `rfm_12m_decile_score` decimal(14,7),                                            |
|   `rfm_13_24m_decile_score` decimal(14,7),                                         |
|   `at_plcc_cust_ind` decimal(1,0),                                                 |
|   `at_plcc_open_dt` timestamp,                                                     |
|   `loft_plcc_cust_ind` decimal(1,0),                                               |
|   `loft_plcc_open_dt` timestamp,                                                   |
|   `at_cbcc_cust_ind` decimal(1,0),                                                 |
|   `at_cbcc_open_dt` timestamp,                                                     |
|   `loft_cbcc_cust_ind` decimal(1,0),                                               |
|   `loft_cbcc_open_dt` timestamp,                                                   |
|   `client_lifestyle_status` string,                                                |
|   `orig_pur_brand_cd` string,                                                      |
|   `last_pur_brand_cd` string,                                                      |
|   `at_pur_ind` decimal(1,0),                                                       |
|   `loft_pur_ind` decimal(1,0),                                                     |
|   `atf_pur_ind` decimal(1,0),                                                      |
|   `los_pur_ind` decimal(1,0),                                                      |
|   `last_pur_store_nbr` decimal(10,0),                                              |
|   `orig_pur_store_nbr` decimal(10,0),                                              |
|   `loyalty_store_nbr` decimal(10,0),                                               |
|   `closest_store_nbr` decimal(10,0),                                               |
|   `last_pur_store_dist` decimal(10,2),                                             |
|   `orig_pur_store_dist` decimal(10,2),                                             |
|   `loyalty_store_dist` decimal(10,2),                                              |
|   `closest_store_dist` decimal(10,2))                                              |
| ROW FORMAT DELIMITED                                                               |
|   FIELDS TERMINATED BY '|'                                                         |
| STORED AS INPUTFORMAT                                                              |
|   'org.apache.hadoop.mapred.TextInputFormat'                                       |
| OUTPUTFORMAT                                                                       |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                     |
| LOCATION                                                                           |
|   'hdfs://ascenaprod/apps/hive/warehouse/ascena_staging.db/loft_sum_household_vw'  |
| TBLPROPERTIES (                                                                    |
|   'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}',                            |
|   'numFiles'='0',                                                                  |
|   'numRows'='0',                                                                   |
|   'rawDataSize'='0',                                                               |
|   'serialization.null.format'='',                                                  |
|   'skip.header.line.count'='1',                                                    |
|   'totalSize'='0',                                                                 |
|   'transient_lastDdlTime'='1564741397')                                            |
+------------------------------------------------------------------------------------+--+
+-------------------------------------------------------------------------------------+--+
|                                   createtab_stmt                                    |
+-------------------------------------------------------------------------------------+--+
| CREATE TABLE `loft_sum_individual_vw`(                                              |
|   `indiv_id` decimal(13,0),                                                         |
|   `brand_cd` string,                                                                |
|   `postal_contact_id` decimal(10,0),                                                |
|   `acct_source_cd` string,                                                          |
|   `acct_nbr` string,                                                                |
|   `hh_id` decimal(13,0),                                                            |
|   `addr_id` string,                                                                 |
|   `barcode_hash` string,                                                            |
|   `prefix` string,                                                                  |
|   `first_nm` string,                                                                |
|   `middle_nm` string,                                                               |
|   `last_nm` string,                                                                 |
|   `mat_sfx` string,                                                                 |
|   `prof_sfx` string,                                                                |
|   `firm_nm` string,                                                                 |
|   `std_gender_cd` string,                                                           |
|   `birth_dt` timestamp,                                                             |
|   `addr_line_1` string,                                                             |
|   `addr_line_2` string,                                                             |
|   `addr_line_3` string,                                                             |
|   `addr_line_4` string,                                                             |
|   `city` string,                                                                    |
|   `state` string,                                                                   |
|   `country` string,                                                                 |
|   `postal_cd` string,                                                               |
|   `zip4` string,                                                                    |
|   `carrier_route_cd` string,                                                        |
|   `delivery_point` string,                                                          |
|   `delivery_point_chk` string,                                                      |
|   `lot_nbr` string,                                                                 |
|   `lot_seq` string,                                                                 |
|   `vulgar_ind` decimal(1,0),                                                        |
|   `business_ind` decimal(1,0),                                                      |
|   `foreign_ind` decimal(1,0),                                                       |
|   `mail_score` string,                                                              |
|   `cass_deliverable_ind` decimal(1,0),                                              |
|   `prison_ind` decimal(1,0),                                                        |
|   `pander_ind` decimal(1,0),                                                        |
|   `deceased_ind` decimal(1,0),                                                      |
|   `apo_fpo_ind` decimal(1,0),                                                       |
|   `us_possession_ind` decimal(1,0),                                                 |
|   `ace_dpv_status` string,                                                          |
|   `ace_dpv_footnote` string,                                                        |
|   `ace_foreign_cd` string,                                                          |
|   `ace_latitude` string,                                                            |
|   `ace_longitude` string,                                                           |
|   `dsf2_vacant_ind` decimal(1,0),                                                   |
|   `dsf2_season_ind` decimal(1,0),                                                   |
|   `dsf2_res_bus_cd` string,                                                         |
|   `dsf2_delivery_type_cd` string,                                                   |
|   `dsf2_del_pt_drop_ind` decimal(1,0),                                              |
|   `agility_dt` timestamp,                                                           |
|   `coa_status_cd` string,                                                           |
|   `coa_applied_dt` timestamp,                                                       |
|   `coa_move_dt` timestamp,                                                          |
|   `ncoa_move_type` string,                                                          |
|   `fips_state` string,                                                              |
|   `fips_county` string,                                                             |
|   `census_tract` string,                                                            |
|   `census_blockgroup` string,                                                       |
|   `geocode` string,                                                                 |
|   `occupancy_score` decimal(1,0),                                                   |
|   `dnf_ind` decimal(1,0),                                                           |
|   `nursing_home_ind` decimal(1,0),                                                  |
|   `can_lang_pref` string,                                                           |
|   `usps_geo_id` decimal(10,0),                                                      |
|   `email_id` decimal(10,0),                                                         |
|   `email_addr` string,                                                              |
|   `orig_acct_source_cd` string,                                                     |
|   `orig_acct_source_dt` timestamp,                                                  |
|   `home_phone_nbr` string,                                                          |
|   `cell_phone_nbr` string,                                                          |
|   `work_phone_nbr` string,                                                          |
|   `active_associate_ind` string,                                                    |
|   `last_txn_dt` timestamp,                                                          |
|   `first_txn_dt` timestamp,                                                         |
|   `all_12m_freq` decimal(10,0),                                                     |
|   `all_12m_mont` decimal(15,2),                                                     |
|   `all_12m_disc_amt` decimal(15,2),                                                 |
|   `all_13_24m_freq` decimal(10,0),                                                  |
|   `all_13_24m_mont` decimal(15,2),                                                  |
|   `all_13_24m_disc_amt` decimal(15,2),                                              |
|   `all_lt_rec` decimal(5,0),                                                        |
|   `all_lt_freq` decimal(10,0),                                                      |
|   `all_lt_mont` decimal(15,2),                                                      |
|   `all_lt_disc_amt` decimal(15,2),                                                  |
|   `ret_last_txn_dt` timestamp,                                                      |
|   `ret_first_txn_dt` timestamp,                                                     |
|   `ret_12m_freq` decimal(10,0),                                                     |
|   `ret_12m_mont` decimal(15,2),                                                     |
|   `ret_12m_disc_amt` decimal(15,2),                                                 |
|   `ret_13_24m_freq` decimal(10,0),                                                  |
|   `ret_13_24m_mont` decimal(15,2),                                                  |
|   `ret_13_24m_disc_amt` decimal(15,2),                                              |
|   `ret_lt_rec` decimal(5,0),                                                        |
|   `ret_lt_freq` decimal(10,0),                                                      |
|   `ret_lt_mont` decimal(15,2),                                                      |
|   `ret_lt_disc_amt` decimal(15,2),                                                  |
|   `web_last_txn_dt` timestamp,                                                      |
+-------------------------------------------------------------------------------------+--+
|                                   createtab_stmt                                    |
+-------------------------------------------------------------------------------------+--+
|   `web_first_txn_dt` timestamp,                                                     |
|   `web_12m_freq` decimal(10,0),                                                     |
|   `web_12m_mont` decimal(15,2),                                                     |
|   `web_12m_disc_amt` decimal(15,2),                                                 |
|   `web_13_24m_freq` decimal(10,0),                                                  |
|   `web_13_24m_mont` decimal(15,2),                                                  |
|   `web_13_24m_disc_amt` decimal(15,2),                                              |
|   `web_lt_rec` decimal(5,0),                                                        |
|   `web_lt_freq` decimal(10,0),                                                      |
|   `web_lt_mont` decimal(15,2),                                                      |
|   `web_lt_disc_amt` decimal(15,2),                                                  |
|   `preferred_channel_cd` string,                                                    |
|   `mail_optout_ind` decimal(1,0),                                                   |
|   `email_optout_ind` decimal(1,0),                                                  |
|   `sms_optout_ind` decimal(1,0),                                                    |
|   `phone_optout_ind` decimal(1,0),                                                  |
|   `rent_optout_ind` decimal(1,0),                                                   |
|   `ca_priv_ind` decimal(1,0),                                                       |
|   `promo_12m_dm` decimal(10,0),                                                     |
|   `promo_12m_em` decimal(10,0),                                                     |
|   `marketable_cd` string,                                                           |
|   `rfm_12m_decile` decimal(2,0),                                                    |
|   `rfm_13_24m_decile` decimal(2,0),                                                 |
|   `rfm_12m_decile_score` decimal(14,7),                                             |
|   `rfm_13_24m_decile_score` decimal(14,7),                                          |
|   `at_plcc_cust_ind` decimal(1,0),                                                  |
|   `at_plcc_open_dt` timestamp,                                                      |
|   `loft_plcc_cust_ind` decimal(1,0),                                                |
|   `loft_plcc_open_dt` timestamp,                                                    |
|   `at_cbcc_cust_ind` decimal(1,0),                                                  |
|   `at_cbcc_open_dt` timestamp,                                                      |
|   `loft_cbcc_cust_ind` decimal(1,0),                                                |
|   `loft_cbcc_open_dt` timestamp,                                                    |
|   `client_lifestyle_status` string,                                                 |
|   `orig_pur_brand_cd` string,                                                       |
|   `last_pur_brand_cd` string,                                                       |
|   `at_pur_ind` decimal(1,0),                                                        |
|   `loft_pur_ind` decimal(1,0),                                                      |
|   `atf_pur_ind` decimal(1,0),                                                       |
|   `los_pur_ind` decimal(1,0),                                                       |
|   `last_pur_store_nbr` decimal(10,0),                                               |
|   `orig_pur_store_nbr` decimal(10,0),                                               |
|   `loyalty_store_nbr` decimal(10,0),                                                |
|   `closest_store_nbr` decimal(10,0),                                                |
|   `last_pur_store_dist` decimal(10,0),                                              |
|   `orig_pur_store_dist` decimal(10,0),                                              |
|   `loyalty_store_dist` decimal(10,0),                                               |
|   `closest_store_dist` decimal(10,0))                                               |
| ROW FORMAT DELIMITED                                                                |
|   FIELDS TERMINATED BY '|'                                                          |
| STORED AS INPUTFORMAT                                                               |
|   'org.apache.hadoop.mapred.TextInputFormat'                                        |
| OUTPUTFORMAT                                                                        |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                      |
| LOCATION                                                                            |
|   'hdfs://ascenaprod/apps/hive/warehouse/ascena_staging.db/loft_sum_individual_vw'  |
| TBLPROPERTIES (                                                                     |
|   'numFiles'='4',                                                                   |
|   'numRows'='0',                                                                    |
|   'rawDataSize'='0',                                                                |
|   'serialization.null.format'='',                                                   |
|   'skip.header.line.count'='1',                                                     |
|   'totalSize'='19469110604',                                                        |
|   'transient_lastDdlTime'='1561403278')                                             |
+-------------------------------------------------------------------------------------+--+
+--------------------------------------------------------------------------------------+--+
|                                    createtab_stmt                                    |
+--------------------------------------------------------------------------------------+--+
| CREATE TABLE `loft_xref_acct_indiv_vw`(                                              |
|   `indiv_id` decimal(13,0),                                                          |
|   `brand_cd` string,                                                                 |
|   `hh_id` decimal(13,0),                                                             |
|   `addr_id` string,                                                                  |
|   `barcode_hash` string,                                                             |
|   `acct_source_cd` string,                                                           |
|   `acct_nbr` string,                                                                 |
|   `orig_activity_dt` timestamp,                                                      |
|   `activity_dt` timestamp,                                                           |
|   `customer_no` string,                                                              |
|   `mail_request_src_cd` string,                                                      |
|   `mail_request_dt` timestamp)                                                       |
| ROW FORMAT DELIMITED                                                                 |
|   FIELDS TERMINATED BY '|'                                                           |
| STORED AS INPUTFORMAT                                                                |
|   'org.apache.hadoop.mapred.TextInputFormat'                                         |
| OUTPUTFORMAT                                                                         |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                       |
| LOCATION                                                                             |
|   'hdfs://ascenaprod/apps/hive/warehouse/ascena_staging.db/loft_xref_acct_indiv_vw'  |
| TBLPROPERTIES (                                                                      |
|   'numFiles'='25',                                                                   |
|   'numRows'='0',                                                                     |
|   'rawDataSize'='0',                                                                 |
|   'serialization.null.format'='',                                                    |
|   'skip.header.line.count'='1',                                                      |
|   'totalSize'='24786558080',                                                         |
|   'transient_lastDdlTime'='1563862253')                                              |
+--------------------------------------------------------------------------------------+--+
+---------------------------------------------------------------------------------------+--+
|                                    createtab_stmt                                     |
+---------------------------------------------------------------------------------------+--+
| CREATE TABLE `loft_xref_indiv_email_vw`(                                              |
|   `indiv_id` decimal(13,0),                                                           |
|   `brand_cd` string,                                                                  |
|   `hh_id` decimal(13,0),                                                              |
|   `addr_id` string,                                                                   |
|   `email_id` decimal(10,0),                                                           |
|   `cur_indiv_ind` decimal(1,0),                                                       |
|   `cur_email_ind` decimal(1,0))                                                       |
| ROW FORMAT DELIMITED                                                                  |
|   FIELDS TERMINATED BY '|'                                                            |
| STORED AS INPUTFORMAT                                                                 |
|   'org.apache.hadoop.mapred.TextInputFormat'                                          |
| OUTPUTFORMAT                                                                          |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                        |
| LOCATION                                                                              |
|   'hdfs://ascenaprod/apps/hive/warehouse/ascena_staging.db/loft_xref_indiv_email_vw'  |
| TBLPROPERTIES (                                                                       |
|   'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}',                               |
|   'numFiles'='0',                                                                     |
|   'numRows'='0',                                                                      |
|   'rawDataSize'='0',                                                                  |
|   'serialization.null.format'='',                                                     |
|   'skip.header.line.count'='1',                                                       |
|   'totalSize'='0',                                                                    |
|   'transient_lastDdlTime'='1564740458')                                               |
+---------------------------------------------------------------------------------------+--+
+-----------------------------------------------------------------------------------------+--+
|                                     createtab_stmt                                      |
+-----------------------------------------------------------------------------------------+--+
| CREATE TABLE `loft_xref_indiv_email_vw_p`(                                              |
|   `indiv_id` decimal(13,0),                                                             |
|   `brand_cd` string,                                                                    |
|   `hh_id` decimal(13,0),                                                                |
|   `addr_id` string,                                                                     |
|   `email_id` decimal(10,0),                                                             |
|   `cur_indiv_ind` decimal(1,0),                                                         |
|   `cur_email_ind` decimal(1,0))                                                         |
| ROW FORMAT DELIMITED                                                                    |
|   FIELDS TERMINATED BY '|'                                                              |
| STORED AS INPUTFORMAT                                                                   |
|   'org.apache.hadoop.mapred.TextInputFormat'                                            |
| OUTPUTFORMAT                                                                            |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                          |
| LOCATION                                                                                |
|   'hdfs://ascenaprod/apps/hive/warehouse/ascena_staging.db/loft_xref_indiv_email_vw_p'  |
| TBLPROPERTIES (                                                                         |
|   'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}',                                 |
|   'numFiles'='0',                                                                       |
|   'numRows'='0',                                                                        |
|   'rawDataSize'='0',                                                                    |
|   'serialization.null.format'='',                                                       |
|   'skip.header.line.count'='1',                                                         |
|   'totalSize'='0',                                                                      |
|   'transient_lastDdlTime'='1564738057')                                                 |
+-----------------------------------------------------------------------------------------+--+
+----------------------------------------------------------------------------------------+--+
|                                     createtab_stmt                                     |
+----------------------------------------------------------------------------------------+--+
| CREATE TABLE `loft_xref_indiv_orphan_vw`(                                              |
|   `indiv_id` decimal(13,0),                                                            |
|   `brand_cd` string,                                                                   |
|   `new_indiv_id` decimal(13,0),                                                        |
|   `new_brand_cd` string,                                                               |
|   `rmb_dt` timestamp)                                                                  |
| ROW FORMAT DELIMITED                                                                   |
|   FIELDS TERMINATED BY '|'                                                             |
| STORED AS INPUTFORMAT                                                                  |
|   'org.apache.hadoop.mapred.TextInputFormat'                                           |
| OUTPUTFORMAT                                                                           |
|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                         |
| LOCATION                                                                               |
|   'hdfs://ascenaprod/apps/hive/warehouse/ascena_staging.db/loft_xref_indiv_orphan_vw'  |
| TBLPROPERTIES (                                                                        |
|   'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}',                                |
|   'numFiles'='0',                                                                      |
|   'numRows'='0',                                                                       |
|   'rawDataSize'='0',                                                                   |
|   'serialization.null.format'='',                                                      |
|   'skip.header.line.count'='1',                                                        |
|   'totalSize'='0',                                                                     |
|   'transient_lastDdlTime'='1564738282')                                                |
+----------------------------------------------------------------------------------------+--+
