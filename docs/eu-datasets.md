# EU Covid Datasets

All datasets are sourced from https://www.ecdc.europa.eu/en/covid-19/data

## Datasets
The below datasets have been as part of the ETL pipeline and are availabe in the warehosue schema

### Movement Indicators
Contains an archive of the data on which the maps requested in the Council Recommendation on a coordinated approach to the restriction of free movement in response to the COVID-19 pandemic were based. [[source]](https://www.ecdc.europa.eu/en/publications-data/archive-data-maps-support-council-recommendation-coordinated-approach-travel)

Data dictionary - [movement-indicators.pdf](./eu-docs/movement-indicators.pdf)
Data dictionary - [movement-indicators-2022.pdf](./eu-docs/movement-indicators-2022.pdf)

### National Cases and Deaths
[1] Contains weekly data on 14-day notification rate of new COVID-19 cases and deaths reported by EU/EEA Member States to the European Surveillance System (TESSy). [[source]](https://www.ecdc.europa.eu/en/publications-data/data-national-14-day-notification-rate-covid-19)

[2] Each row/entry contains the number of new cases and deaths reported per day and per country in the EU/EEA. You may use the data in line with ECDC’s copyright policy.[[source]](https://www.ecdc.europa.eu/en/publications-data/data-daily-new-cases-covid-19-eueea-country)

[3] The weekly number of new reported COVID-19 cases and deaths worldwide [[source]](https://www.ecdc.europa.eu/en/publications-data/download-historical-data-20-june-2022-weekly-number-new-reported-covid-19-cases)

Data dictionary [1] [3] - [national-cases-deaths.pdf](./eu-docs/national-cases-deaths.pdf)
Data dictionary [2] - [national-cases-deaths-daily.pdf](./eu-docs/national-cases-deaths-daily.pdf)

### Vaccine Tracker
Vaccine Tracker contains information on COVID-19 vaccination in the EU/EEA.

The data are presented in the Vaccine Tracker and collected through The European Surveillance System (TESSy). EU/EEA Member States are requested to report basic indicators (number of vaccine doses distributed by manufacturers, number of first, second, additional and unspecified doses administered) and data by target groups at national level every four weeks.

Data are subject to retrospective corrections. Corrected datasets are released every four weeks as soon as the processing of updated national data has been completed at ECDC level. You may use the data in line with ECDC’s copyright policy. [[source]](https://www.ecdc.europa.eu/en/publications-data/data-covid-19-vaccination-eu-eea)

Data dictionary - [vaccine-tracker.pdf](./eu-docs/vaccine-tracker.pdf)


## Datasets Not Yet Included
The below datasets have been extracted but have not yet been incorporated into the ETL pipeline

### Age Cases National
Contains information on the 14-day notification rate of newly reported COVID-19 cases per 100 000 population by age group, week and country. Each row contains the corresponding data for a certain week and country. The file is updated weekly. You may use the data in line with ECDC’s copyright policy. [[source]](https://www.ecdc.europa.eu/en/publications-data/covid-19-data-14-day-age-notification-rate-new-cases)

Data dictionary - [age-cases-national.docx](./eu-docs/age-cases-national.docx)

### COVID-19 vaccination data
Contains information on COVID-19 vaccination in the EU/EEA since September 2023, when ECDC updated its COVID-19 vaccination coverage data analysis process, in view of the evolving timing and objective of the 2023–24 season vaccination campaigns.

The data are collected through The European Surveillance System (TESSy). EU/EEA Member States are requested to report basic indicators (number of individuals receiving one vaccine during the reporting period starting from 01 September 2023, vaccine product) and data by target groups at national level.

Data are subject to retrospective corrections. Corrected datasets are released as soon as the processing of updated national data has been completed at ECDC level. You may use the data in line with ECDC’s copyright policy.[[source]](https://www.ecdc.europa.eu/en/publications-data/covid-19-data-vaccination)

Data dictionary - [covid-19-vc-data.pdf](./eu-docs/covid-19-vc-data.pdf)

### Hospital ICU Admission Rates
Contains information about hospitalisation and Intensive Care Unit (ICU) admission rates and current occupancy for COVID-19 by date and country. Each row contains the corresponding data for a certain date (day or week) and per country. The file is updated weekly. You may use the data in line with ECDC’s copyright policy. [[source]](https://www.ecdc.europa.eu/en/publications-data/download-data-hospital-and-icu-admission-rates-and-current-occupancy-covid-19)

Data dictionary - [hospital-icu-admission-rates.pdf](./eu-docs/hospital-icu-admission-rates.pdf)

### Subnational Cases (Daily and Weekly)
Contains information on the 14-day notification rate of new cases per 100 000 inhabitants for COVID-19 by day/week and subnational region up to week 36, 2022. Each row contains the corresponding data for a certain day and per subnational region.

Please note that daily data on cases per subnational region are not available for all countries. Weekly data on new cases per subnational region for all EU/EEA countries up to week 36, 2022, can be found at 'Download data on the weekly subnational 14-day notification rate of new COVID-19 cases'. There may be differences between the rates shown in these two datasets since they are based on different sources of data. [[source daily]](https://www.ecdc.europa.eu/en/publications-data/subnational-14-day-notification-rate-covid-19)
[[source weekly]](https://www.ecdc.europa.eu/en/publications-data/weekly-subnational-14-day-notification-rate-covid-19)

Data dictionary - [subnational-daily-data.pdf](./eu-docs/subnational-daily-data.pdf)
Data dictionary - [subnational-weekly-data.pdf](./eu-docs/subnational-weekly-data.pdf)

### Testing
Contains information about testing volume for COVID-19 by week and country. Each row contains the corresponding data for a country and a week. The file is updated weekly. You may use the data in line with ECDC’s copyright policy.

Source: The figures displayed for weekly testing rate and weekly test positivity are based on multiple data sources. The main source is data submitted by Member States to the European Surveillance System (TESSy), however, when not available, ECDC compiles data from public online sources. EU/EEA Member States report in TESSy all tests performed (i.e. both PCR and antigen tests). [[source]](https://www.ecdc.europa.eu/en/publications-data/covid-19-testing)

Data dictionary - [testing-data.pdf](./eu-docs/testing-data.pdf)

### Virus Variants
Contains information about the volume of COVID-19 sequencing, the number and percentage distribution of variants of concern (VOC) by week and country. Each row contains the corresponding data for a country, variant and week (the data are in long format). The file is updated weekly. You may use the data in line with ECDC’s copyright policy and with GISAID’s data usage policy. We gratefully acknowledge both the originating and submitting laboratories for the sequence data in GISAID EpiCoV on which these outputs are partially based.

Available data on the volume of COVID-19 sequencing, the number and percentage distribution of VOC for each country, week and variant submitted since 2020-W40 to the GISAID EpiCoV database (https://www.gisaid.org/) and TESSy (as either case-based or aggregate data) are displayed. [[source]](https://www.ecdc.europa.eu/en/publications-data/data-virus-variants-covid-19-eueea)

Data dictionary - [virus-variant.pdf](./eu-docs/virus-variant.pdf)
