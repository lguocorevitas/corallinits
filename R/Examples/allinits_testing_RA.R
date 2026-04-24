

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
cut_date    <- as.Date(glue("{tdy_year}-{tdy_month}-01"))
sharepoint <- "~/../../Thermo Fisher Scientific/"
dir_ra_monthly  <- glue("{sharepoint}/Biostat Data Files - RA/monthly/")
analytic_data  <- glue("{dir_ra_monthly}/{tdy_year}/{cut_date}/")

# Step 0, find visits and drug data
visits <-readRDS(glue("{analytic_data}/rewrite/RA_visits_calc_{cut_date}.rds"))
drug <-readRDS(glue("{analytic_data}/rewrite/RA_drugexpdetails_{cut_date}.rds"))

# Step 1, find baseline visit with user defined cutoff days. Currently using 183 days.

init_upadacitinib<- corallinits::make_drug_baseline_visit_dataset(
  visits_df = visits,
  drug_df = drug,
  target_generic_key = "upadacitinib",
  baseline_cutoff_days = 183
)

# step 2, find the prior generic name and the reason(s) for changing

init_upadacitinib <- corallinits::add_prior_btsdmard_info(
  base_visit_df = init_upadacitinib,
  drug_df = drug,
  id_col = id,
  init_date_col = init_date,
  drug_category_col = drug_category,
  generic_key_col = generic_key,
  generic_start_date_col = generic_start_date,
  eligible_categories = c(250, 390)
)

# Step 3, find the first stop for initiations and the reasons for stop.

init_upadacitinib <- corallinits::map_stop_to_visits(
  visit_df = init_upadacitinib,
  drug_df = drug,
  id_col = id,
  visit_date_col = visitdate,
  generic_key_col = generic_key,
  generic_stop_date_col = generic_stop_date,
  reason_1_col = reason_1,
  reason_2_col = reason_2,
  reason_3_col = reason_3,
  reason_1_category_col = reason_1_category,
  reason_2_category_col = reason_2_category,
  reason_3_category_col = reason_3_category,
  target_generic_keys = "upadacitinib"
)

# Step 4 find first switch and carry it forward

init_upadacitinib <- corallinits::map_switch_to_visits(
  visit_df = init_upadacitinib,
  drug_df = drug,
  id_col = id,
  visit_date_col = visitdate,
  first_stop_date_col = first_stop_date,
  druggrp_col = druggrp,
  generic_key_col = generic_key,
  generic_start_date_col = generic_start_date,
  drug_category_col = drug_category,
  eligible_categories = c(250, 390)
)

# Step 5, find fu_grp

init_upadacitinib <- corallinits::identify_fu_visits(
  visit_df = init_upadacitinib,
  id_col = id,
  visit_date_col = visitdate,
  init_date_col = init_date,
  stop_date_col = first_stop_date,
  fu6_window = c(91, 273),
  fu12_window = c(274, 457),
  fu18_window = c(458, 638),
  fu24_window = c(639, 819)
)
# output data for QC
saveRDS(init_upadacitinib, file = "./allinits_package/init_upadacitinib.rds")
write_dta(init_upadacitinib,  "./allinits_package/init_upadacitinib.dta")


data_name <-"allinits"

test_data <- init_upadacitinib
current_data  <- glue("{analytic_data}/rewrite/RA_{data_name}_{cut_date}.rds")
id_cols <- c("id", "druggrp", "visitdate")

# running the code
master_df <- readRDS(test_data)

using_df  <- read_dta(current_data)
if (!"id" %in% names(using_df)) using_df <- using_df %>% mutate(id = subject_number)


res <- corcf::corcf(
  master = master_df,
  using  = using_df,
  vars   = "_all",
  id     = id_cols
  # ,verbose1 = 200
)

# names(res$label_conflicts)
#
# res$ecode
# if code =106 means you hit at least one type mismatch (string vs numeric, like Stata)



corcf::write_corcf_word(
  res,
  master_path = test_data,
  using_path  = current_data,
  id_cols = id_cols,
  path = glue("{analytic_data}/rewrite/QC/corcf_results_{data_name}_{tdy_date}.docx"),
  report_date = Sys.Date()
)


# Step 6, Optional to other registries,
# add base_X and X variables needed from visits data, calculate acr and mACR
