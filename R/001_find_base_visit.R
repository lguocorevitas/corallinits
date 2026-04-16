# if init_date == visitdate, use that visit
# otherwise, if init_date and visitdate are in the same year-month, use that visit
# otherwise, use the previous visitdate if it is within baseline_cutoff_days before init_date
library(dplyr)
library(rlang)
library(dplyr)
library(rlang)

make_drug_baseline_visit_dataset <- function(
    visits_df,
    drug_df,
    target_generic_key,
    baseline_cutoff_days = 183,
    id_col = id,
    visitdate_col = visitdate,
    indexn_col = indexn,
    indexN_col = indexN,
    generic_key_col = generic_key,
    init_generic_col = init_generic,
    generic_start_date_col = generic_start_date,
    assign_output = FALSE,
    output_env = .GlobalEnv
) {
  
  id_col <- rlang::enquo(id_col)
  visitdate_col <- rlang::enquo(visitdate_col)
  indexn_col <- rlang::enquo(indexn_col)
  indexN_col <- rlang::enquo(indexN_col)
  generic_key_col <- rlang::enquo(generic_key_col)
  init_generic_col <- rlang::enquo(init_generic_col)
  generic_start_date_col <- rlang::enquo(generic_start_date_col)
  
  output_name <- paste0(gsub("[^A-Za-z0-9_]", "_", target_generic_key), "_base_visit")
  
  visits2 <- visits_df |>
    dplyr::transmute(
      id = !!id_col,
      visitdate = as.Date(as.character(!!visitdate_col)),
      indexn = !!indexn_col,
      indexN = !!indexN_col
    ) |>
    dplyr::filter(!is.na(id), !is.na(visitdate)) |>
    dplyr::distinct()
  
  inits2 <- drug_df |>
    dplyr::filter(
      !!init_generic_col == 1,
      !!generic_key_col == target_generic_key
    ) |>
    dplyr::transmute(
      id = !!id_col,
      generic_key = !!generic_key_col,
      init_date = as.Date(as.character(!!generic_start_date_col))
    ) |>
    dplyr::filter(!is.na(id), !is.na(init_date)) |>
    dplyr::distinct() |>
    dplyr::group_by(id, generic_key) |>
    dplyr::slice_min(init_date, n = 1, with_ties = FALSE) |>
    dplyr::ungroup()
  
  baseline_df <- inits2 |>
    dplyr::left_join(
      visits2 |>
        dplyr::select(id, visitdate),
      by = "id",
      relationship = "one-to-many"
    ) |>
    dplyr::mutate(
      exact_match = visitdate == init_date,
      same_month = format(visitdate, "%Y-%m") == format(init_date, "%Y-%m"),
      prev_days_diff = as.integer(init_date - visitdate),
      eligible_prev = !exact_match &
        !same_month &
        !is.na(prev_days_diff) &
        prev_days_diff >= 0 &
        prev_days_diff <= baseline_cutoff_days,
      baseline_priority = dplyr::case_when(
        exact_match ~ 1L,
        !exact_match & same_month ~ 2L,
        eligible_prev ~ 3L,
        TRUE ~ 99L
      ),
      closeness = dplyr::case_when(
        exact_match ~ 0L,
        same_month ~ abs(as.integer(init_date - visitdate)),
        eligible_prev ~ prev_days_diff,
        TRUE ~ NA_integer_
      )
    ) |>
    dplyr::filter(baseline_priority < 99L) |>
    dplyr::group_by(id, generic_key, init_date) |>
    dplyr::arrange(
      baseline_priority,
      closeness,
      dplyr::desc(visitdate),
      .by_group = TRUE
    ) |>
    dplyr::slice_head(n = 1) |>
    dplyr::ungroup() |>
    dplyr::transmute(
      id,
      init_date,
      base_visit = visitdate
    )
  
  out <- visits2 |>
    dplyr::left_join(baseline_df, by = "id") |>
    dplyr::mutate(
      druggrp = target_generic_key
    ) |>
    dplyr::filter(!is.na(base_visit) & visitdate >= base_visit) |>
    dplyr::select(id, visitdate, init_date, base_visit, indexn, indexN, druggrp) |>
    dplyr::arrange(id, visitdate)
  
  if (assign_output) {
    assign(output_name, out, envir = output_env)
  }
  
  return(out)
}