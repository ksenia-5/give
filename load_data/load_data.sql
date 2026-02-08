
-- in snowflake create DB, schema, and stage to load files from local filesystem
CREATE DATABASE IF NOT EXISTS give;
CREATE SCHEMA IF NOT EXISTS give.landing;
CREATE STAGE give.landing.raw_stage;


-- load files to stage from local machine by running code in load_to_stage.sql

-- create file format
CREATE OR REPLACE FILE FORMAT give.landing.ff_json
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;  -- use TRUE if the JSON file is one big array 

-- test file format
SELECT
$1::variant AS payload,
METADATA$FILENAME AS filename
FROM @give.landing.raw_stage/charitycommission/charity/publicextract.charity.zip
(FILE_FORMAT => give.landing.ff_json)
-- PATTERN = '.*\.json\.zip$'
    limit 10
    ;

-- Create a landing table for charity data (store raw JSON in VARIANT)
CREATE OR REPLACE TABLE give.landing.charity (
  payload variant,
  filename string,
  file_last_modified_at timestamp_ntz,
  file_row_number number,
  file_content_key varchar(100),
  loaded_at TIMESTAMP_NTZ DEFAULT SYSDATE()
);

-- load data from stage to table
COPY INTO give.landing.charity
FROM (
  SELECT
    $1::VARIANT AS payload,
    METADATA$FILENAME AS filename,
    METADATA$FILE_LAST_MODIFIED::TIMESTAMP_NTZ AS file_last_modified_at,
    METADATA$FILE_ROW_NUMBER AS file_row_number,
    METADATA$FILE_CONTENT_KEY AS file_content_key,
    CURRENT_TIMESTAMP() AS loaded_at
  FROM @give.landing.raw_stage/charitycommission/publicextract.charity.json.gz
    ( FILE_FORMAT => give.landing.ff_json )
)
ON_ERROR = 'SKIP_FILE';
;

create schema give.raw;


-- load charity records to table to raw table `give.raw.charity`, parsing json payload fields, 
-- and casting to data types according to data definition file
create or replace table give.raw.charity as (
select distinct
    payload:date_of_extract::date as date_of_extract
    , payload:organisation_number::number as organisation_number
    , payload:registered_charity_number::number as registered_charity_number
    , payload:linked_charity_number::number as linked_charity_number
    , payload:charity_name::string as charity_name
    , payload:charity_type::string as charity_type
    , payload:charity_registration_status::string as charity_registration_status
    , payload:date_of_registration::date as date_of_registration
    , payload:date_of_removal::date as date_of_removal
    , payload:charity_reporting_status::string as charity_reporting_status
    , payload:latest_acc_fin_period_start_date::date as latest_acc_fin_period_start_date
    , payload:latest_acc_fin_period_end_date::date as latest_acc_fin_period_end_date
    , payload:latest_income::float as latest_income
    , payload:latest_expenditure::float as latest_expenditure
    , payload:charity_contact_address1::string as charity_contact_address1
    , payload:charity_contact_address2::string as charity_contact_address2
    , payload:charity_contact_address3::string as charity_contact_address3
    , payload:charity_contact_address4::string as charity_contact_address4
    , payload:charity_contact_address5::string as charity_contact_address5
    , payload:charity_contact_postcode::string as charity_contact_postcode
    , payload:charity_contact_phone::string as charity_contact_phone
    , payload:charity_contact_email::string as charity_contact_email
    , payload:charity_contact_web::string as charity_contact_web
    , payload:charity_company_registration_number::string as charity_company_registration_number

    , payload:charity_insolvent::boolean as charity_insolvent
    , payload:charity_in_administration::boolean as charity_in_administration
    , payload:charity_previously_excepted::boolean as charity_previously_excepted
    
    , payload:charity_is_cdf_or_cif::string as charity_is_cdf_or_cif

    , payload:charity_is_cio::boolean as charity_is_cio
    , payload:cio_is_dissolved::boolean as cio_is_dissolved
    , payload:date_cio_dissolution_notice::datetime as date_cio_dissolution_notice

    , payload:charity_activities::string as charity_activities
    
    , payload:charity_gift_aid::boolean as charity_gift_aid
    , payload:charity_has_land::boolean as charity_has_land
    from give.landing.charity
    )
;