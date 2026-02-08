-- -- publicextract.charity.json
-- -- publicextract.charity_trustee.json
-- -- publicextract.charity_published_report.json
-- -- publicextract.charity_policy.json
-- -- publicextract.charity_other_regulators.json
-- -- publicextract.charity_other_names.json
-- -- publicextract.charity_governing_document.json
-- -- publicextract.charity_event_history.json
-- -- publicextract.charity_annual_return_history.json                        
-- -- publicextract.charity_annual_return_parta.json                     
-- -- publicextract.charity_annual_return_partb.json                           
-- -- publicextract.charity_area_of_operation.json            
-- -- publicextract.charity_classification.json               

put file://./data/** @give.landing.raw_stage/charitycommission/
auto_compress = true;

ls @give.landing.raw_stage/charitycommission/;