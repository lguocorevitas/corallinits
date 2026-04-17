
#' Title
#'
#' @param visit_df
#' @param drug_df
#' @param id_col
#' @param visit_date_col
#' @param first_stop_date_col
#' @param druggrp_col
#' @param generic_key_col
#' @param generic_start_date_col
#' @param drug_category_col
#' @param eligible_categories
#'
#' @returns
#' @export
#'
#' @examples
#' # It identifies, for each id, the first visit row where first_stop_date is available.
#' # It uses that row’s first_stop_date and druggrp as the current initiator stop context.
#' # It then searches drug_df for eligible switched drugs with generic_start_date
#' # on or after that first_stop_date,
#' # limited to the specified eligible_categories,
#' # and excludes drugs whose generic_key matches the current druggrp
#' init_upadacitinib <- corallinits::map_switch_to_visits(
#'   visit_df = init_upadacitinib,
#'   drug_df = drug,
#'   id_col = id,
#'   visit_date_col = visitdate,
#'   first_stop_date_col = first_stop_date,
#'   druggrp_col = druggrp,
#'   generic_key_col = generic_key,
#'   generic_start_date_col = generic_start_date,
#'   drug_category_col = drug_category,
#'   eligible_categories = c(250, 390)
#' )
map_switch_to_visits <- function(
    visit_df,
    drug_df,
    id_col = id,
    visit_date_col = visitdate,
    first_stop_date_col = first_stop_date,
    druggrp_col = druggrp,
    generic_key_col = generic_key,
    generic_start_date_col = generic_start_date,
    drug_category_col = drug_category,
    eligible_categories = c(250, 390)
) {

  id_col <- rlang::enquo(id_col)
  visit_date_col <- rlang::enquo(visit_date_col)
  first_stop_date_col <- rlang::enquo(first_stop_date_col)
  druggrp_col <- rlang::enquo(druggrp_col)
  generic_key_col <- rlang::enquo(generic_key_col)
  generic_start_date_col <- rlang::enquo(generic_start_date_col)
  drug_category_col <- rlang::enquo(drug_category_col)

  visits0 <- visit_df |>
    dplyr::mutate(
      .row_id__ = dplyr::row_number()
    )

  visits <- visits0 |>
    dplyr::mutate(
      .id__ = !!id_col,
      .visitdate__ = as.Date(as.character(!!visit_date_col)),
      .first_stop_date__ = as.Date(as.character(!!first_stop_date_col)),
      .druggrp__ = as.character(!!druggrp_col)
    ) |>
    dplyr::filter(
      !is.na(.id__),
      !is.na(.visitdate__)
    )

  # first row within each id where first_stop_date becomes available
  stop_info <- visits |>
    dplyr::filter(
      !is.na(.first_stop_date__),
      !is.na(.druggrp__)
    ) |>
    dplyr::arrange(.id__, .visitdate__, .row_id__) |>
    dplyr::group_by(.id__) |>
    dplyr::slice_head(n = 1) |>
    dplyr::ungroup() |>
    dplyr::transmute(
      id = .id__,
      first_stop_date = .first_stop_date__,
      druggrp = .druggrp__,
      druggrp_std = stringr::str_to_lower(
        stringr::str_trim(.druggrp__)
      )
    )

  switch_candidates <- drug_df |>
    dplyr::transmute(
      id = !!id_col,
      generic_key = as.character(!!generic_key_col),
      generic_key_std = stringr::str_to_lower(
        stringr::str_trim(as.character(!!generic_key_col))
      ),
      generic_start_date = as.Date(as.character(!!generic_start_date_col)),
      drug_category = !!drug_category_col
    ) |>
    dplyr::filter(
      !is.na(id),
      !is.na(generic_key),
      !is.na(generic_start_date),
      drug_category %in% eligible_categories
    )

  first_switch_df <- stop_info |>
    dplyr::inner_join(
      switch_candidates,
      by = "id",
      relationship = "one-to-many"
    ) |>
    dplyr::filter(
      generic_start_date >= first_stop_date,
      generic_key_std != druggrp_std
    ) |>
    dplyr::arrange(id, generic_start_date, generic_key_std) |>
    dplyr::group_by(id) |>
    dplyr::slice_head(n = 1) |>
    dplyr::ungroup() |>
    dplyr::transmute(
      id,
      switch_start_date = generic_start_date,
      switchname = generic_key
    )

  mapped_switch_visit <- first_switch_df |>
    dplyr::inner_join(
      visits |>
        dplyr::transmute(
          id = .id__,
          visitdate = .visitdate__
        ),
      by = "id",
      relationship = "many-to-many"
    ) |>
    dplyr::filter(visitdate >= switch_start_date) |>
    dplyr::arrange(id, switch_start_date, visitdate) |>
    dplyr::group_by(id, switch_start_date, switchname) |>
    dplyr::slice_head(n = 1) |>
    dplyr::ungroup() |>
    dplyr::transmute(
      id,
      visitdate,
      first_switch = 1L,
      switchname,
      switch_start_date
    )

  out <- visits0 |>
    dplyr::left_join(
      mapped_switch_visit,
      by = stats::setNames(
        c("id", "visitdate"),
        c(rlang::as_name(id_col), rlang::as_name(visit_date_col))
      )
    ) |>
    dplyr::mutate(
      first_switch = dplyr::coalesce(first_switch, 0L)
    ) |>
    dplyr::group_by(!!id_col) |>
    dplyr::arrange(!!visit_date_col, .row_id__, .by_group = TRUE) |>
    dplyr::mutate(
      cumswitch = dplyr::if_else(cumsum(first_switch) > 0, 1L, 0L)
    ) |>
    dplyr::mutate(
      switchname_first = {
        x <- switchname[!is.na(switchname)]
        if (length(x) == 0) NA_character_ else dplyr::first(x)
      },
      switch_start_date_first = {
        x <- switch_start_date[!is.na(switch_start_date)]
        if (length(x) == 0) as.Date(NA) else dplyr::first(x)
      }
    ) |>
    dplyr::mutate(
      switchname = dplyr::if_else(
        cumswitch == 1L,
        switchname_first,
        NA_character_
      ),
      switch_start_date = dplyr::if_else(
        cumswitch == 1L,
        switch_start_date_first,
        as.Date(NA)
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::arrange(.row_id__) |>
    dplyr::select(
      -.row_id__,
      -switchname_first,
      -switch_start_date_first
    )

  return(out)
}
