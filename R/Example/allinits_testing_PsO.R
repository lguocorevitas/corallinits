# Testing allinits package using PsO data

# function to find a baseline visit for a specific drug
library(dplyr)
library(rlang)

# testing
library(stringr)
library(haven)
library(lubridate)
library(glue)
library(tidyverse)

tdy_date <- Sys.Date()
tdy_year  <- sprintf("%04d", year(tdy_date))
tdy_month <- sprintf("%02d", month(tdy_date))
cut_date    <- as.Date(glue("{tdy_year}-{tdy_month}-05"))
sharepoint <- "~/../../Thermo Fisher Scientific/"
dir_ra_monthly  <- glue("{sharepoint}/Biostat Data Files - PsO/monthly/")
analytic_data  <- glue("{dir_ra_monthly}/{tdy_year}/{cut_date}/")

# Step 0, find visits and drug data
visits <-readRDS(glue("{analytic_data}/Analytic Data/PsO_visits_{cut_date}.rds"))
# create drug_category variable for step 2, finding prior b/tsDMARDs, PDE4 or TYK2 by excluding cDMARDs
drug <-readRDS(glue("{analytic_data}/Analytic Data/PsO_drugexp_details_{cut_date}.rds"))%>%
  mutate(
    drugkey_clean = str_to_lower(str_trim(drugkey)),
    drug_category = case_when(

      # biologic = 250
      drugkey_clean %in% c(
        "adalimumab biosimilar (abrilada)",
        "adalimumab biosimilar (hadlima)",
        "adalimumab biosimilar (hulio)",
        "adalimumab biosimilar (hyrimoz)",
        "adalimumab biosimilar (simlandi)",
        "adalimumab biosimilar (yuflyma)",
        "adalimumab-aacf (idacio)",
        "amevive",
        "amjevita",
        "bimzelx",
        "cimzia",
        "cosentyx",
        "dupixant",
        "enbrel",
        "erelzi",
        "etanercept biosimilar (brenzys)",
        "humira",
        "ilumya",
        "inflectra",
        "infliximab biosimilar",
        "infliximab biosimilar (avsola)",
        "infliximab biosimilar (renflexis)",
        "ixifi",
        "raptiva",
        "remicade",
        "renflexis",
        "siliq",
        "simponi",
        "skyrizi",
        "stelara",
        "taltz",
        "tremfya",
        "ustekinumab biosimilar (jamteki)",
        "ustekinumab biosimilar (wezlana)",
        "ustekinumab biosimilar (yesintek)",
        "ustekinumab-ttwe",
        "yesintek (stelara biosimilar)"
      ) ~ 250L,

      # JAKi = 390
      drugkey_clean %in% c(
        "xeljanz"
      ) ~ 390L,

      # PDE4 = 380
      drugkey_clean %in% c(
        "apremilast"
      ) ~ 380L,

      # TYK2 = 395
      drugkey_clean %in% c(
        "deucravacitinib"
      ) ~ 395L,

      # conventional systemic = 100
      drugkey_clean %in% c(
        "acitretin",
        "azulfidine",
        "cyclos",
        "hydroxy",
        "mtx",
        "mycoph",
        "thiog"
      ) ~ 100L,

      # unknown/other = 999
      drugkey_clean %in% c(
        "biosim",
        "humira vs biosimilar",
        "invest_agent",
        "other"
      ) ~ 999L,

      TRUE ~ 999L
    )
  ) %>%
  select(-drugkey_clean)
# create reason1-3 categories for step 2
# helper function
map_reason_3cat <- function(x) {
  case_when(
    x %in% c(10, 13, 14, 15, 16) ~ 1L,  # effectiveness
    x %in% c(1, 2, 3, 7, 18, 19, 20) ~ 2L,  # safety
    x %in% c(4, 5, 6, 8, 9, 11, 12, 17) ~ 9L,  # other
    is.na(x) ~ NA_integer_,
    TRUE ~ 9L
  )
}

drug <- drug %>%
  mutate(
    reason1_3cat = map_reason_3cat(reason1),
    reason2_3cat = map_reason_3cat(reason2),
    reason3_3cat = map_reason_3cat(reason3)
  )
drug <- drug %>%
  mutate(
    reason1_3cat = map_reason_3cat(reason1),
    reason2_3cat = map_reason_3cat(reason2),
    reason3_3cat = map_reason_3cat(reason3),
    reason1_3cat_f = factor(reason1_3cat, levels = c(1, 2, 9),
                            labels = c("effectiveness", "safety", "other")),
    reason2_3cat_f = factor(reason2_3cat, levels = c(1, 2, 9),
                            labels = c("effectiveness", "safety", "other")),
    reason3_3cat_f = factor(reason3_3cat, levels = c(1, 2, 9),
                            labels = c("effectiveness", "safety", "other"))
  )
table(drug$reason1, drug$reason1_3cat, useNA = "ifany")

# create drug_stop_date column for step3
drug <- drug %>%
  mutate(
    drug_stop_date = if_else(!is.na(stp), drugdate, as.Date(NA))
  )
# create drug_start_date column for step4
drug <- drug %>%
  mutate(
    drug_start_date = if_else(!is.na(st), drugdate, as.Date(NA))
  )
# Step 1, find baseline visit with user defined cutoff days. Currently using 183 days.

init_enbrel<- corallinits::make_drug_baseline_visit_dataset(
  visits_df = visits,
  drug_df = drug,
  target_generic_key = "enbrel",
  baseline_cutoff_days = 183,
  generic_key_col = drugkey, # PsO use drugkey
  init_generic_col = init,  # PsO use init
  generic_start_date_col = drugdate # PsO has drugdate
)

# step 2, find the prior generic name and the reason(s) for changing

init_enbrel <- corallinits::add_prior_btsdmard_info(
  base_visit_df = init_enbrel,
  drug_df = drug,
  id_col = id,
  init_date_col = init_date,
  drug_category_col = drug_category,
  generic_key_col = drugkey,
  generic_start_date_col = drugdate,
  reason_1_col = reason1,
  reason_2_col = reason2,
  reason_3_col = reason3,
  reason_1_category_col = reason1_3cat,
  reason_2_category_col = reason2_3cat,
  reason_3_category_col = reason3_3cat,
  eligible_categories = c(250, 390,380,395)
)

# Step 3, find the first stop for initiations and the reasons for stop.

init_enbrel <- corallinits::map_stop_to_visits(
  visit_df = init_enbrel,
  drug_df = drug,
  id_col = id,
  visit_date_col = visitdate,
  generic_key_col = drugkey,
  generic_stop_date_col = drug_stop_date, # calculated for PsO
  reason_1_col = reason1,
  reason_2_col = reason2,
  reason_3_col = reason3,
  reason_1_category_col = reason1_3cat,
  reason_2_category_col = reason2_3cat,
  reason_3_category_col = reason3_3cat,
  target_generic_keys = "enbrel"
)

# Step 4, find first switch and carry it forward

init_enbrel <- corallinits::map_switch_to_visits(
  visit_df = init_enbrel,
  drug_df = drug,
  id_col = id,
  visit_date_col = visitdate,
  first_stop_date_col = first_stop_date,
  druggrp_col = druggrp,
  generic_key_col = drugkey,
  generic_start_date_col = drug_start_date,
  drug_category_col = drug_category,
  eligible_categories = c(250, 390,380,395)
)

# Step 5, find fu_grp

init_enbrel <- corallinits::identify_fu_visits(
  visit_df = init_enbrel,
  id_col = id,
  visit_date_col = visitdate,
  init_date_col = init_date,
  stop_date_col = first_stop_date,
  fu6_window = c(91, 273),
  fu12_window = c(274, 457),
  fu18_window = c(458, 638),
  fu24_window = c(639, 819)
)
