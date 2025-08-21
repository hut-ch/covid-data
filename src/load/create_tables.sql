CREATE TABLE IF NOT EXISTS dim_date AS
(
    "dim_date_key"  INT,
    "date"          TIMESTAMP,
    "year"          INT,
    "month"         INT,
    "month_name"    VARCHAR(10),
    "day"           INT,
    "day_name"      VARCHAR(10),
    "week"          INT,
    "start_of_week" BOOLEAN, 
    "end_of_week"   BOOLEAN     
);

CREATE TABLE IF NOT EXISTS dim_country AS
(
    "dim_country_key"   INT,
    "country_code"      VARCHAR(10),
    "country_name"      VARCHAR(255),
    "territory_code"    VARCHAR(10), 
    "continent"         VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_region AS
(
    "dim_region_key"    INT,
    "region_code"       VARCHAR(10),
    "region_name"       VARCHAR(255),
    "dim_country_key"   INT
);

CREATE TABLE IF NOT EXISTS dim_vaccine AS
(
    "dim_vaccine_key"   INT,
    "vaccine_code"      INT,
    "vaccine_name"      VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_source AS
(
    "dim_source_key"    INT,
    "source_name"       VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_status AS
(
    "dim_status_key"        INT,
    "colour_name"           VARCHAR(255),
    "colour_description"    VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_age AS
(
    "dim_age_key"       INT,
    "age_group"         VARCHAR(255),
    "age_lower_limit"   INT,
    "age_upper_limit"   INT
);

CREATE TABLE IF NOT EXISTS fact_vaccine_tracker_country AS
(
    "dim_date_key"          INT,
    "dim_country_key"       INT,
    "dim_vaccine_key"       INT,
    "dim_source_key"        INT,
    "dim_age_key"           INT,
    "first_dose"            INT,
    "first_dose_refused"    BOOLEAN
    "second_dose"           INT,
    "additional_dose_1"     INT,
    "additional_dose_2"     INT,
    "additional_dose_3"     INT,
    "additional_dose_4"     INT,
    "additional_dose_5"     INT,
    "unknown_dose"          INT,
    "doses_received"        INT,
    "doses_exported"        INT,
    "population"            INT,
    "denominator"           INT
);

CREATE TABLE IF NOT EXISTS fact_vaccine_tracker_region AS
(
    "dim_date_key"          INT,
    "dim_country_key"       INT,
    "dim_region_key"        INT,
    "dim_vaccine_key"       INT,
    "dim_source_key"        INT,
    "dim_age_key"           INT,
    "first_dose"            INT,
    "first_dose_refused"    BOOLEAN
    "second_dose"           INT,
    "additional_dose_1"     INT,
    "additional_dose_2"     INT,
    "additional_dose_3"     INT,
    "additional_dose_4"     INT,
    "additional_dose_5"     INT,
    "unknown_dose"          INT,
    "doses_received"        INT,
    "doses_exported"        INT,
    "population"            INT,
    "denominator"           INT
);

CREATE TABLE IF NOT EXISTS fact_movement_indicators_country AS
(
    "dim_date_key"          INT,
    "dim_country_key"       INT,
    "dim_status_key"        INT,
    "population"            INT,
    "new_cases_last_7days"  INT,
    "new_cases_last_14days" INT,
    "notification_rate"     INT,
    "testing_rate"          INT,
    "positivity_rate"       INT
    "vaccination_rate"      INT
);

CREATE TABLE IF NOT EXISTS fact_movement_indicators_region AS
(
    "dim_date_key"              INT,
    "dim_country_key"           INT,
    "dim_region_key"            INT,
    "dim_status_key"            INT,
    "population"                INT,
    "new_cases_last_7days"      INT,
    "new_cases_last_14days"     INT,
    "notification_rate"         INT,
    "testing_rate"              INT,
    "positivity_rate"           INT,
    "vaccination_rate"          INT,
    "positivity_rate_combined"  INT,
    "testing_rate_combined"     INT
);

CREATE TABLE IF NOT EXISTS fact_cases_deaths_country_daily AS
(
    "dim_date_key"              INT,
    "dim_country_key"           INT,
    "cases",                    INT,
    "cases_cumulative",         INT,
    "cases_notification_rate",  NUMBER(18,3)
    "deaths",                   INT,
    "deaths_cumulative",        INT,
    "deaths_notification_rate", NUMBER(18,3)
);

CREATE TABLE IF NOT EXISTS fact_cases_deaths_country_weekly AS
(
    "dim_date_key"              INT,
    "dim_country_key"           INT,
    "cases",                    INT,
    "cases_cumulative",         INT,
    "cases_notification_rate",  NUMBER(18,3)
    "deaths",                   INT,
    "deaths_cumulative",        INT,
    "deaths_notification_rate", NUMBER(18,3)
);