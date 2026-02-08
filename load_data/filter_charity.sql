select count(*)
from charity;
-- 395452 charity records in charity table.

-- distribution of charities by registration status
with charities as (
select * 
from charity 

)
select distinct 
    charity_registration_status
    , count(*) as charity_count
    , round(ratio_to_report(count(*)) over () * 100 , 1) as pct_of_total
from charities
group by 1
order by charity_count desc;
-- 185,014 registred charities, of these 171,177 are parent charities and 13,837 are linked charities;



--  check range of start and end dates of latest financial accounting period
with charities as (
select * from give.raw.charity
where 1 = 1 
    and linked_charity_number =  0
    and charity_registration_status <> 'Removed'
)
select
    min(latest_acc_fin_period_start_date)::string || ' to ' || max(latest_acc_fin_period_start_date)::string  as start_date
    , min(latest_acc_fin_period_end_date)::string || ' to ' || max(latest_acc_fin_period_end_date)::string  as end_date
from charities
;

-- distribution of charities by reporting status
with charities as (
select * 
from charity 
where 1=1
    and linked_charity_number =  0
    and charity_registration_status <> 'Removed'
)
select distinct 
    charity_reporting_status
    , count(*) as charity_count
    , round(ratio_to_report(count(*)) over () * 100 , 1) as pct_of_total
from charities
group by 1
order by charity_count desc
;


--  check range of start and end dates of latest financial accounting period
with charities as (
    select 
        * 
    from give.raw.charity
    where 1 = 1 
        and linked_charity_number =  0
        and charity_registration_status <> 'Removed'
)
select
    charity_reporting_status
    , count(*) as charity_count
    , round(ratio_to_report(count(*)) over () * 100 , 1) as pct_of_total
    , min(latest_acc_fin_period_start_date)::string as min_start_date
    , max(latest_acc_fin_period_start_date)::string  as max_start_date
    , min(latest_acc_fin_period_end_date)::string as min_end_date
    , max(latest_acc_fin_period_end_date)::string  as max_end_date
from charities
group by 1
order by charity_count desc
;

-- create new schema
create schema give.dim;

-- create filtered Charity table in dim schema
-- add derived fields
create or replace table give.dim.charity as (
    select distinct
        registered_charity_number
        , charity_name
        , charity_type
        , date_of_registration
        , datediff(year, date_of_registration, sysdate()) as age 
        , latest_acc_fin_period_start_date
        , latest_acc_fin_period_end_date
        , latest_income
        , latest_expenditure
        , latest_income - latest_expenditure as latest_surplus
        , charity_contact_postcode
        , case when charity_contact_postcode is not null then true else false end as has_post_code
        , charity_contact_phone
        , case when length(charity_contact_phone) > 6 then true else false end as has_phone
        , charity_contact_email
        , case when charity_contact_email is not null then true else false end as has_email
        , charity_contact_web
        , case when charity_contact_web is not null then true else false end as has_website
        , charity_company_registration_number
        , case when charity_company_registration_number is not null then true else false end as is_registered_company
        , charity_insolvent as is_insolvent
        , charity_in_administration as is_in_administration
        , charity_previously_excepted as is_previously_excepted
        , charity_is_cio as is_cio
        , cio_is_dissolved as is_cio_dissolved
        , charity_activities
        , charity_gift_aid as is_gift_aid
        , charity_has_land as has_land
    from give.raw.charity
    where 1 = 1 
        and linked_charity_number =  0
        and charity_registration_status <> 'Removed'
        and charity_reporting_status = 'Submission Received'
)
;

-- preview filtered data
select * from give.dim.charity
limit 5;