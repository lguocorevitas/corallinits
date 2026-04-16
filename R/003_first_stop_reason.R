# find the first stop and create cumulative stop indicator for all fu visits,
# carrying first-stop date/reasons/categories only from the mapped stop visit forward
map_stop_to_visits <- function(
    visit_df,
    drug_df,
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
    target_generic_keys = NULL
) {
  
  id_col <- rlang::enquo(id_col)
  visit_date_col <- rlang::enquo(visit_date_col)
  generic_key_col <- rlang::enquo(generic_key_col)
  generic_stop_date_col <- rlang::enquo(generic_stop_date_col)
  reason_1_col <- rlang::enquo(reason_1_col)
  reason_2_col <- rlang::enquo(reason_2_col)
  reason_3_col <- rlang::enquo(reason_3_col)
  reason_1_category_col <- rlang::enquo(reason_1_category_col)
  reason_2_category_col <- rlang::enquo(reason_2_category_col)
  reason_3_category_col <- rlang::enquo(reason_3_category_col)
  
  # capture source value labels / variable labels from drug_df
  label_meta <- list(
    stop_reason_1 = list(
      labels = attr(dplyr::pull(drug_df, !!reason_1_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_1_col), "label", exact = TRUE)
    ),
    stop_reason_2 = list(
      labels = attr(dplyr::pull(drug_df, !!reason_2_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_2_col), "label", exact = TRUE)
    ),
    stop_reason_3 = list(
      labels = attr(dplyr::pull(drug_df, !!reason_3_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_3_col), "label", exact = TRUE)
    ),
    stop_reason_1_category = list(
      labels = attr(dplyr::pull(drug_df, !!reason_1_category_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_1_category_col), "label", exact = TRUE)
    ),
    stop_reason_2_category = list(
      labels = attr(dplyr::pull(drug_df, !!reason_2_category_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_2_category_col), "label", exact = TRUE)
    ),
    stop_reason_3_category = list(
      labels = attr(dplyr::pull(drug_df, !!reason_3_category_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_3_category_col), "label", exact = TRUE)
    )
  )
  
  apply_label_attrs <- function(x, meta, var_label = NULL) {
    if (!is.null(meta$labels)) {
      attr(x, "labels") <- meta$labels
    }
    if (!is.null(var_label)) {
      attr(x, "label") <- var_label
    } else if (!is.null(meta$label)) {
      attr(x, "label") <- meta$label
    }
    x
  }
  
  visits0 <- visit_df |>
    dplyr::mutate(.row_id__ = dplyr::row_number()) |>
    dplyr::transmute(
      .row_id__,
      id = !!id_col,
      visitdate = as.Date(!!visit_date_col),
      dplyr::across(dplyr::everything())
    )
  
  visits <- visits0 |>
    dplyr::filter(!is.na(id), !is.na(visitdate))
  
  stops <- drug_df |>
    dplyr::mutate(.drug_row__ = dplyr::row_number()) |>
    dplyr::transmute(
      .drug_row__,
      id = !!id_col,
      generic_key = !!generic_key_col,
      generic_stop_date = as.Date(!!generic_stop_date_col),
      stop_reason_1 = !!reason_1_col,
      stop_reason_2 = !!reason_2_col,
      stop_reason_3 = !!reason_3_col,
      stop_reason_1_category = !!reason_1_category_col,
      stop_reason_2_category = !!reason_2_category_col,
      stop_reason_3_category = !!reason_3_category_col
    ) |>
    dplyr::filter(!is.na(id), !is.na(generic_stop_date))
  
  if (!is.null(target_generic_keys)) {
    stops <- stops |>
      dplyr::filter(generic_key %in% target_generic_keys)
  }
  
  # first stop per patient for the target initiator drug
  first_stop <- stops |>
    dplyr::arrange(id, generic_stop_date, .drug_row__) |>
    dplyr::group_by(id) |>
    dplyr::slice_head(n = 1) |>
    dplyr::ungroup()
  
  # map first stop date to the first visit on/after stop date
  mapped_stop_visit <- first_stop |>
    dplyr::select(id, generic_stop_date) |>
    dplyr::inner_join(
      visits |> dplyr::select(id, visitdate),
      by = "id",
      relationship = "many-to-many"
    ) |>
    dplyr::filter(generic_stop_date <= visitdate) |>
    dplyr::group_by(id, generic_stop_date) |>
    dplyr::slice_min(visitdate, n = 1, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::transmute(
      id,
      visitdate,
      has_stop = 1L
    )
  
  # patient-level first stop info
  first_stop_info <- first_stop |>
    dplyr::transmute(
      id,
      first_stop_date = generic_stop_date,
      stop_reason_1,
      stop_reason_2,
      stop_reason_3,
      stop_reason_1_category,
      stop_reason_2_category,
      stop_reason_3_category
    )
  
  out <- visits0 |>
    dplyr::left_join(mapped_stop_visit, by = c("id", "visitdate")) |>
    dplyr::mutate(
      has_stop = dplyr::coalesce(has_stop, 0L)
    ) |>
    dplyr::group_by(id) |>
    dplyr::arrange(visitdate, .row_id__, .by_group = TRUE) |>
    dplyr::mutate(
      cumstop = dplyr::if_else(cumsum(has_stop) > 0, 1L, 0L)
    ) |>
    dplyr::ungroup() |>
    dplyr::left_join(first_stop_info, by = "id") |>
    dplyr::mutate(
      first_stop_date = dplyr::if_else(cumstop == 1L, first_stop_date, as.Date(NA)),
      stop_reason_1 = ifelse(cumstop == 1L, stop_reason_1, NA),
      stop_reason_2 = ifelse(cumstop == 1L, stop_reason_2, NA),
      stop_reason_3 = ifelse(cumstop == 1L, stop_reason_3, NA),
      stop_reason_1_category = ifelse(cumstop == 1L, stop_reason_1_category, NA),
      stop_reason_2_category = ifelse(cumstop == 1L, stop_reason_2_category, NA),
      stop_reason_3_category = ifelse(cumstop == 1L, stop_reason_3_category, NA)
    ) |>
    dplyr::arrange(.row_id__) |>
    dplyr::select(-.row_id__)
  
  # reapply labels after ifelse() strips attributes
  out <- out |>
    dplyr::mutate(
      stop_reason_1 = apply_label_attrs(
        stop_reason_1,
        label_meta$stop_reason_1,
        "First stop reason 1 for initiator, from stop visit forward"
      ),
      stop_reason_2 = apply_label_attrs(
        stop_reason_2,
        label_meta$stop_reason_2,
        "First stop reason 2 for initiator, from stop visit forward"
      ),
      stop_reason_3 = apply_label_attrs(
        stop_reason_3,
        label_meta$stop_reason_3,
        "First stop reason 3 for initiator, from stop visit forward"
      ),
      stop_reason_1_category = apply_label_attrs(
        stop_reason_1_category,
        label_meta$stop_reason_1_category,
        "First stop reason 1 category for initiator, from stop visit forward"
      ),
      stop_reason_2_category = apply_label_attrs(
        stop_reason_2_category,
        label_meta$stop_reason_2_category,
        "First stop reason 2 category for initiator, from stop visit forward"
      ),
      stop_reason_3_category = apply_label_attrs(
        stop_reason_3_category,
        label_meta$stop_reason_3_category,
        "First stop reason 3 category for initiator, from stop visit forward"
      )
    )
  
  return(out)
}