-- ============================================================
-- virtue-facility-finder — app-facing schema (DDL export)
-- Exported 2026-11-25 from Unity Catalog (read-only SHOW/DESCRIBE).
-- Views are the app's read surface; two base tables carry the index and
-- silver records. Notebook pipeline that builds these: notebooks/.
-- ============================================================

-- ------------------------------------------------------------
-- VIEW vf.app.vw_facility_search_documents
-- ------------------------------------------------------------
CREATE VIEW app.vw_facility_search_documents (
  record_id,
  source_field,
  element_position,
  evidence_text,
  search_text_norm)
WITH SCHEMA COMPENSATION
AS SELECT record_id, source_field, element_position, evidence_text, search_text_norm
  FROM vf.search.facility_search_documents_v1
;

-- ------------------------------------------------------------
-- VIEW vf.app.vw_facilities_public
-- ------------------------------------------------------------
CREATE VIEW app.vw_facilities_public (
  _source_row_id,
  name_clean,
  city_clean,
  state_clean,
  pincode_clean,
  coordinate_status)
WITH SCHEMA COMPENSATION
AS SELECT _source_row_id, name_clean, city_clean, state_clean, pincode_clean, coordinate_status
  FROM vf.silver.facilities_current_v1
;

-- ------------------------------------------------------------
-- VIEW vf.app.vw_facilities_coordinates
-- ------------------------------------------------------------
CREATE VIEW app.vw_facilities_coordinates (
  _source_row_id,
  latitude,
  longitude,
  coordinate_status)
WITH SCHEMA COMPENSATION
AS SELECT _source_row_id, latitude, longitude, coordinate_status
FROM vf.silver.facilities_current_v1
;

-- ------------------------------------------------------------
-- VIEW vf.app.vw_facilities_contacts
-- ------------------------------------------------------------
CREATE VIEW app.vw_facilities_contacts (
  _source_row_id,
  phone_numbers,
  websites)
WITH SCHEMA COMPENSATION
AS SELECT _source_row_id, phone_numbers, websites
FROM vf.silver.facilities_current_v1
;

-- ------------------------------------------------------------
-- VIEW vf.app.vw_facilities_review_flags
-- ------------------------------------------------------------
CREATE VIEW app.vw_facilities_review_flags (
  _source_row_id,
  review_priority,
  review_reason_codes)
WITH SCHEMA COMPENSATION
AS SELECT _source_row_id, review_priority, review_reason_codes
  FROM vf.silver.facilities_review_queue_v1
;

-- ------------------------------------------------------------
-- TABLE vf.gold.facility_service_mentions_v2
-- (columns; full build in notebooks/ pipeline)
-- ------------------------------------------------------------
-- DESCRIBE vf.gold.facility_service_mentions_v2
  _source_row_id                    string                      
  service_code                      string                      
  facility_name                     string                      
  city_clean                        string                      
  state_clean                       string                      
  pincode_clean                     string                      
  latitude                          double                      
  longitude                         double                      
  coordinate_status                 string                      
  source_url_candidates             array<string>               
  review_priority                   string                      
  review_reason_codes               array<string>               
  matched_terms_all                 array<string>               
  evidence                          array<struct<source_field:string,element_position:int,element_kind:string,evidence_text:string,matched_terms:array<string>>>
  evidence_count                    int                         
  matching_rule_version             string                      
  verification_status               string                      
  current_availability_status       string                      
  human_review_status               string                      
  notice                            string                      
  ai_enrichment_status              string                      
  ai_assertion                      string                      
  ai_patient_scope                  string                      
  ai_treatment_types_for_display    array<string>               
  ai_has_generic_dialysis_mention   boolean                     
  ai_facility_attribution           string                      
  ai_possible_mixed_facility_record boolean                     
  ai_evidence_quotations            array<string>               
  ai_patient_scope_evidence         array<string>               
  ai_attribution_evidence           array<string>               
  ai_validator_version              string                      
  ai_enrichment_note                string                      

-- ------------------------------------------------------------
-- TABLE vf.gold.dialysis_jaipur_pilot_v2
-- (columns; full build in notebooks/ pipeline)
-- ------------------------------------------------------------
-- DESCRIBE vf.gold.dialysis_jaipur_pilot_v2
  record_id                         string                      
  facility_name                     string                      
  service_code                      string                      
  city                              string                      
  state                             string                      
  pin_code                          string                      
  latitude                          double                      
  longitude                         double                      
  coordinate_status                 string                      
  assertion                         string                      
  patient_scope                     string                      
  facility_attribution              string                      
  possible_mixed_facility_record    boolean                     
  needs_attribution_review          boolean                     
  evidence_quotations               array<struct<source_field:string,exact_quote:string>>
  attribution_evidence              array<struct<source_field:string,exact_quote:string>>
  unattributed_source_url_candidatesarray<string>               
  human_review_status               string                      
  verification_status               string                      
  current_availability_status       string                      
  evidence_label                    string                      
  location_label                    string                      
  notice                            string                      
  retrieved_evidence_quote          string                      
  retrieved_evidence_url            string                      
  retrieved_evidence_retrieved_at   string                      
  retrieved_temporal_context        string                      
  retrieved_branch_address_quote    string                      
  retrieved_branch_address_url      string                      
  retrieved_branch_assessment       string                      
  retrieved_unresolved_issues       string                      

-- ------------------------------------------------------------
-- TABLE vf.search.facility_search_documents_v1
-- (columns; full build in notebooks/ pipeline)
-- ------------------------------------------------------------
-- DESCRIBE vf.search.facility_search_documents_v1
  record_id                         string                      
  source_field                      string                      
  element_position                  int                         
  element_kind                      string                      
  evidence_text                     string                      
  search_text_norm                  string                      
  rule_version                      string                      
  built_at                          timestamp                   

-- ------------------------------------------------------------
-- TABLE vf.silver.facilities_current_v1
-- (columns; full build in notebooks/ pipeline)
-- ------------------------------------------------------------
-- DESCRIBE vf.silver.facilities_current_v1
  unique_id                         string                      
  source_types                      string                      
  source_ids                        string                      
  source_content_id                 string                      
  name                              string                      
  organization_type                 string                      
  content_table_id                  string                      
  phone_numbers                     string                      
  officialPhone                     string                      
  email                             string                      
  websites                          string                      
  officialWebsite                   string                      
  yearEstablished                   string                      
  acceptsVolunteers                 string                      
  facebookLink                      string                      
  address_line1                     string                      
  address_line2                     string                      
  address_line3                     string                      
  address_city                      string                      
  address_stateOrRegion             string                      
  address_zipOrPostcode             string                      
  address_country                   string                      
  address_countryCode               string                      
  countries                         string                      
  facilityTypeId                    string                      
  operatorTypeId                    string                      
  affiliationTypeIds                string                      
  description                       string                      
  area                              string                      
  numberDoctors                     string                      
  capacity                          string                      
  specialties                       string                      
  procedure                         string                      
  equipment                         string                      
  capability                        string                      
  recency_of_page_update            string                      
  distinct_social_media_presence_countstring                      
  affiliated_staff_presence         string                      
  custom_logo_presence              string                      
  number_of_facts_about_the_organizationstring                      
  post_metrics_most_recent_social_media_post_datestring                      
  post_metrics_post_count           string                      
  engagement_metrics_n_followers    string                      
  engagement_metrics_n_likes        string                      
  engagement_metrics_n_engagements  string                      
  source                            string                      
  coordinates                       string                      
  latitude                          double                      
  longitude                         double                      
  cluster_id                        string                      
  source_urls                       string                      
  name_clean                        string                      
  pincode_status                    string                      
  pincode_clean                     string                      
  number_doctors_status             string                      
  number_doctors_clean              bigint                      
  capacity_status                   string                      
  capacity_clean                    bigint                      
  accepts_volunteers_status         string                      
  accepts_volunteers_clean          boolean                     
  coordinate_status                 string                      
  specialties_status                string                      
  specialties_clean                 array<string>               
  affiliationTypeIds_status         string                      
  affiliationTypeIds_clean          array<string>               
  source_urls_parsed                array<string>               
  source_urls_status                string                      
  source_url_candidates             array<string>               
  source_url_candidate_count        int                         
  recency_of_page_update_status     string                      
  recency_of_page_update_clean      date                        
  post_metrics_most_recent_social_media_post_date_statusstring                      
  post_metrics_most_recent_social_media_post_date_cleandate                        
  year_established_status           string                      
  year_established_clean            int                         
  facility_type_clean               string                      
  operator_type_clean               string                      
  city_clean                        string                      
  state_clean                       string                      
  description_clean                 string                      
  capability_clean                  string                      
  procedure_clean                   string                      
  equipment_clean                   string                      
  quality_issue_codes               array<string>               
  quality_issue_count               int                         
  _source_row_id                    string                      
  _processed_at                     timestamp                   
  _source_table                     string                      
  _cleaning_rule_version            string                      
  postal_match_status               string                      
  postal_pair_candidates            array<struct<state:string,district:string>>
  postal_state_candidate            string                      
  postal_district_candidate         string                      
  postal_state_comparison           string                      
  state_normalized                  string                      
  state_normalization_method        string                      
  postal_state_comparison_v3        string                      
  exact_duplicate_group_size        bigint                      
  exact_duplicate_rank              int                         
  keep_exact_representative         boolean                     
  unique_id_status                  string                      

