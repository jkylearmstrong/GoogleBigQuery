condition_occurrence <-tbl(con, 'condition_occurrence')

condition_occurrence %>%
  filter(condition_concept_id == 3004501) %>% #s1$concept_id) %>%
  show_query()

#cohortNameLong : Pregnant

excel_sheets(here::here('data/pone.0192033.s001.xlsx'))
s1 <- read_excel(here::here('data/pone.0192033.s001.xlsx'))

copy_to(con, s1)


# The first part is going to be some preprocessing of the concept codes like what we already did.
# We may think of inclusion and exclusion requirements of concept codes

# we will need these dates in a little bit

current_date_time <- Sys.time()

library('lubridate')

lookback_start_date_time <- current_date_time - months(4)
lookback_start_yr_mo <- year(lookback_start_date_time)*100 + month(lookback_start_date_time)


source('02_Libraries_Functions/2_2_config.r')
sc <- spark_connect(master = "yarn-client", config = config)

time_table <- readRDS('02a_Create_Concept_Sets/DATA/S4.outcomes.RDS')

time_table_spark <- copy_to(sc, time_table,'time_table', overwrite = TRUE)

Preg_events_concept_codes <- readRDS('02a_Create_Concept_Sets/DATA/Preg_events_concept_codes.RDS')

source_concept_codes <- Preg_events_concept_codes

source_concept_codes_spark <- copy_to(sc , Preg_events_concept_codes, 'source_concept_codes', overwrite = TRUE)

condition_occurrence <- tbl(sc, 'ds_omop_cdm.condition_occurrence')

colnames(condition_occurrence)

pregnancy_outcome_table <- source_concept_codes_spark %>% # these are the concept codes
  filter(SRC == 'pone.0192033') %>% # i only want to work with codes from the paper for now
  filter(!is.na(pregnancy_outcome)) %>% # i want to find all members that have an outcome
  filter(!(trimws(Category) == 'DELIV')) %>% # we don't want to include the DELIV outcome
  select(pregnancy_outcome, Category_Desc, concept_id) %>% # i only need these columns to make the query
  left_join(condition_occurrence, by=c('concept_id' = 'condition_concept_id')) %>% # we're restricting our seach to the condition_occurrence table and grabbing all records that meet the condition
  arrange(person_id, desc(condition_start_date)) %>% #we're going to select the most recent one
  group_by(person_id) %>%
  mutate( rn = row_number()) %>%
  filter( rn == 1 ) %>%
  ungroup() %>%
  rename( target_end_date = condition_start_date ) %>% # getting some additional output
  rename( target_concept_id = concept_id ) %>%
  select( pregnancy_outcome, person_id, target_concept_id, target_end_date) %>%
  left_join(time_table_spark, by=c('pregnancy_outcome'='Outcome')) %>%   # here i'm joining on the window table to define the approprate lookback for that outcome to find the start of the pregancy
  mutate( min_preg_start = date_sub(target_end_date, Minimum_Pregnancy_Term)) %>%
  mutate( max_preg_start = date_sub(target_end_date, Maximum_Pregnancy_Term)) %>%
  filter( person_id > 0 ) %>%
  select( pregnancy_outcome, person_id, target_concept_id, target_end_date, min_preg_start, max_preg_start ) %>%
  compute('pregnancy_outcome_table')

pregnancy_outcome_table %>%
  left_join(source_concept_codes_spark %>%
              filter(SRC == 'pone.0192033') %>%
              filter(!is.na(pregnancy_outcome)) , by =c('target_concept_id'='concept_id')) %>%
  select(Category_Desc, Category, concept_name, vocabulary_id, concept_class_id, concept_code, target_concept_id, person_id) %>%
  group_by(Category_Desc, Category, concept_name, vocabulary_id, concept_class_id, concept_code, target_concept_id) %>%
  summarise(cnt = n_distinct(person_id)) %>%
  arrange(-cnt) %>%
  collect() %>%
  readr::write_csv("02a_Create_Concept_Sets/DATA/output/retrospective_preg_end_by_category_no_DELIV.csv")

# For the members who had one of our outcomes, I will now look to estimate the start of the pregancy:

# Pregnancy episode start marker types:
#  LMP: Last menstrual period date,
#  GEST: Gestational age record,
#  FERT: Assisted conception procedure date,
#  ULS: Nuchal ultrasound date,
#  AFP: Alpha feto protein test date,
#  AMEN: Amenorrhea record date,
#  URINE: Urine pregnancy test date - should be UP
###
#  PCONF: Confirmation of pregnancy

Preg_start_categories <- c('GEST', 'ULS', 'ULS', 'AFP', 'AMEN', 'UP', 'PCONF')


preg_start <- source_concept_codes_spark %>%
  filter(is.na(pregnancy_outcome)) %>% # not outcomes
  filter( Category %in% Preg_start_categories ) %>% # must be one of our start codes
  select(Category, Category_Desc, concept_id) %>% #columns I need
  left_join(condition_occurrence, by=c('concept_id' = 'condition_concept_id')) %>% # condition occurance
  left_join(pregnancy_outcome_table %>% select(person_id, target_end_date, min_preg_start, max_preg_start) , #i need to refrence the outcome table
            by = c('person_id'='person_id')) %>%
  filter( max_preg_start <= condition_start_date &
            condition_start_date <= min_preg_start ) %>% # my members are within the window expected
  arrange(condition_start_date) %>% # earilst indication within the window
  group_by(person_id) %>%
  mutate( rn = row_number()) %>%
  ungroup() %>%
  filter(rn == 1 ) %>%
  rename(start_Category = Category ) %>%
  rename(start_concept_id = concept_id ) %>%
  rename(target_start_date = condition_start_date ) %>%
  select(start_Category, person_id, start_concept_id, target_start_date) %>%
  compute('preg_start')

# some outputs that we're going to review:

#Category_Desc, Category, concept_name, vocabulary_id, concept_class_id, concept_code, target_concept_id

preg_start %>%
  left_join(source_concept_codes_spark , by =c('start_concept_id'='concept_id')) %>%
  group_by(Category_Desc, start_Category, concept_name, vocabulary_id, concept_class_id, concept_code, start_concept_id) %>%
  summarise(cnt = n_distinct(person_id)) %>%
  arrange(-cnt) %>%
  collect() %>%
  readr::write_csv("02a_Create_Concept_Sets/DATA/output/retrospective_preg_start_by_category_no_DELIV.csv")

# I can test exporting this to PostGre to test in OMOP-learn:

training_cohort <- preg_start %>%
  left_join(pregnancy_outcome_table) %>%
  mutate(preg_duration = datediff(target_end_date, target_start_date)) %>%
  mutate(outcome_date = target_end_date) %>%
  mutate(end_date = target_start_date) %>%
  mutate(start_date = date_sub(end_date,365)) %>%
  compute('training_cohort')

glimpse(training_cohort)

training_cohort_postgre.local <- training_cohort %>%
  mutate(y = if_else(pregnancy_outcome == "LB",1,0)) %>%
  select(person_id, start_date, end_date, outcome_date, y) %>%
  collect() %>%
  arrange(person_id) %>%
  mutate(example_id = row_number() -1 ) %>%
  select(example_id, person_id, start_date, end_date, outcome_date, y) %>%
  mutate(start_date = as_date(start_date)) %>%
  mutate(end_date = as_date(end_date)) %>%
  mutate(outcome_date = as_date(outcome_date))


training_cohort_postgre.local %>%
  readr::write_csv("DATA/cohorts/maternity_training_cohort_LB_no_DELIV.csv")


training_cohort_postgre.local <- training_cohort %>%
  mutate(y = if_else(pregnancy_outcome == "ECT",1,0)) %>%
  select(person_id, start_date, end_date, outcome_date, y) %>%
  collect() %>%
  arrange(person_id) %>%
  mutate(example_id = row_number() -1 ) %>%
  select(example_id, person_id, start_date, end_date, outcome_date, y) %>%
  mutate(start_date = as_date(start_date)) %>%
  mutate(end_date = as_date(end_date)) %>%
  mutate(outcome_date = as_date(outcome_date))


training_cohort_postgre.local %>%
  readr::write_csv("DATA/cohorts/maternity_training_cohort_ect_no_DELIV.csv")


training_cohort_multiclass_postgre.local <- training_cohort %>%
  mutate(y = pregnancy_outcome) %>%
  select(person_id, start_date, end_date, outcome_date, y) %>%
  collect() %>%
  arrange(person_id) %>%
  mutate(example_id = row_number() -1 ) %>%
  select(example_id, person_id, start_date, end_date, outcome_date, y) %>%
  mutate(start_date = as_date(start_date)) %>%
  mutate(end_date = as_date(end_date)) %>%
  mutate(outcome_date = as_date(outcome_date))


training_cohort_multiclass_postgre.local %>%
  readr::write_csv("DATA/cohorts/maternity_training_cohort_multiclass_no_DELIV.csv")

# some other output we're going to review:

training_cohort %>%
  group_by(start_Category, pregnancy_outcome) %>%
  summarise(cnt_n = n_distinct(person_id),
            min_preg_dur = min(preg_duration, na.rm = TRUE),
            mean_preg_dur = mean(preg_duration, na.rm = TRUE),
            max_preg_dur = max(preg_duration, na.rm = TRUE)) %>%
  collect() %>%
  readr::write_csv("02a_Create_Concept_Sets/DATA/output/retrospective_preg_duration_no_DELIV.csv")


