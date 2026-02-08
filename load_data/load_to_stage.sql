-- Downloaded json files saved from https://register-of-charities.charitycommission.gov.uk/en/register/full-register-download
-- to local folder `data/`

-- -- to load the following json files in data/ folder to snowflake internal stage
-- -- run `snow sql -f load_data.sql` in terminal using snow CLI

-- publicextract.charity.json
-- publicextract.charity_trustee.json
-- publicextract.charity_published_report.json
-- publicextract.charity_policy.json
-- publicextract.charity_other_regulators.json
-- publicextract.charity_other_names.json
-- publicextract.charity_governing_document.json
-- publicextract.charity_event_history.json
-- publicextract.charity_annual_return_history.json                        
-- publicextract.charity_annual_return_parta.json                     
-- publicextract.charity_annual_return_partb.json                           
-- publicextract.charity_area_of_operation.json            
-- publicextract.charity_classification.json               


-- in case of connection issues during above operation
-- disable MFA for 30 mins in snowflake by running the following command using ACCOUNTADMIN role
--  ALTER USER <username> SET MINS_TO_BYPASS_MFA = 30;

put file://./data/** @give.landing.raw_stage/charitycommission/
auto_compress = true;

-- -- check files in snowflake stage using CLI or snowflake extension
-- ls @give.landing.raw_stage/charitycommission/;