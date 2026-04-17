
# find prior drug and reasons/categories
# Summary: For each baseline initiation, this function finds the most recent
# prior eligible b/tsDMARD started before init_date and adds its generic name,
# discontinuation reasons,
# and reason categories to the baseline visit data,
# while preserving source labels for the reason fields.
#' Title
#'
#' @param base_visit_df
#' @param drug_df
#' @param id_col
#' @param init_date_col
#' @param drug_category_col
#' @param generic_key_col
#' @param generic_start_date_col
#' @param reason_1_col
#' @param reason_2_col
#' @param reason_3_col
#' @param reason_1_category_col
#' @param reason_2_category_col
#' @param reason_3_category_col
#' @param eligible_categories
#'
#' @returns
#' @export
#'
#' @examples
#' # step 2, find the prior generic name and the reason(s) for changing
#' init_upadacitinib <- corallinits::add_prior_btsdmard_info(
#'   base_visit_df = init_upadacitinib,
#'   drug_df = drug,
#'   id_col = id,
#'   init_date_col = init_date,
#'   drug_category_col = drug_category,
#'   generic_key_col = generic_key,
#'   generic_start_date_col = generic_start_date,
#'   eligible_categories = c(250, 390)
#' )
add_prior_btsdmard_info <- function(
    base_visit_df,
    drug_df,
    id_col = id,
    init_date_col = init_date,
    drug_category_col = drug_category,
    generic_key_col = generic_key,
    generic_start_date_col = generic_start_date,
    reason_1_col = reason_1,
    reason_2_col = reason_2,
    reason_3_col = reason_3,
    reason_1_category_col = reason_1_category,
    reason_2_category_col = reason_2_category,
    reason_3_category_col = reason_3_category,
    eligible_categories = c(250, 390)
) {

  id_col <- rlang::enquo(id_col)
  init_date_col <- rlang::enquo(init_date_col)
  drug_category_col <- rlang::enquo(drug_category_col)
  generic_key_col <- rlang::enquo(generic_key_col)
  generic_start_date_col <- rlang::enquo(generic_start_date_col)
  reason_1_col <- rlang::enquo(reason_1_col)
  reason_2_col <- rlang::enquo(reason_2_col)
  reason_3_col <- rlang::enquo(reason_3_col)
  reason_1_category_col <- rlang::enquo(reason_1_category_col)
  reason_2_category_col <- rlang::enquo(reason_2_category_col)
  reason_3_category_col <- rlang::enquo(reason_3_category_col)

  label_meta <- list(
    base_prev_reason_1 = list(
      labels = attr(dplyr::pull(drug_df, !!reason_1_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_1_col), "label", exact = TRUE)
    ),
    base_prev_reason_2 = list(
      labels = attr(dplyr::pull(drug_df, !!reason_2_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_2_col), "label", exact = TRUE)
    ),
    base_prev_reason_3 = list(
      labels = attr(dplyr::pull(drug_df, !!reason_3_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_3_col), "label", exact = TRUE)
    ),
    base_prev_reason_1_category = list(
      labels = attr(dplyr::pull(drug_df, !!reason_1_category_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_1_category_col), "label", exact = TRUE)
    ),
    base_prev_reason_2_category = list(
      labels = attr(dplyr::pull(drug_df, !!reason_2_category_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_2_category_col), "label", exact = TRUE)
    ),
    base_prev_reason_3_category = list(
      labels = attr(dplyr::pull(drug_df, !!reason_3_category_col), "labels", exact = TRUE),
      label  = attr(dplyr::pull(drug_df, !!reason_3_category_col), "label", exact = TRUE)
    )
  )

  apply_label_attrs <- function(x, meta) {
    if (!is.null(meta$labels)) {
      attr(x, "labels") <- meta$labels
    }
    if (!is.null(meta$label)) {
      attr(x, "label") <- meta$label
    }
    x
  }

  last_nonmissing <- function(x) {
    idx <- which(!is.na(x))
    if (length(idx) == 0) {
      x[NA_integer_]
    } else {
      x[idx[length(idx)]]
    }
  }

  base_init_df <- base_visit_df |>
    dplyr::transmute(
      id = !!id_col,
      init_date = as.Date(!!init_date_col)
    ) |>
    dplyr::filter(!is.na(id), !is.na(init_date)) |>
    dplyr::distinct()

  drug2 <- drug_df |>
    dplyr::mutate(.drug_row__ = dplyr::row_number()) |>
    dplyr::transmute(
      .drug_row__,
      id = !!id_col,
      generic_key = !!generic_key_col,
      drug_category = !!drug_category_col,
      generic_start_date = as.Date(!!generic_start_date_col),
      reason_1 = !!reason_1_col,
      reason_2 = !!reason_2_col,
      reason_3 = !!reason_3_col,
      reason_1_category = !!reason_1_category_col,
      reason_2_category = !!reason_2_category_col,
      reason_3_category = !!reason_3_category_col
    ) |>
    dplyr::filter(
      !is.na(id),
      !is.na(generic_start_date),
      drug_category %in% eligible_categories
    ) |>
    dplyr::arrange(id, generic_key, generic_start_date, .drug_row__) |>
    dplyr::group_by(id, generic_key, generic_start_date) |>
    dplyr::summarise(
      drug_category = dplyr::last(drug_category),
      reason_1 = last_nonmissing(reason_1),
      reason_2 = last_nonmissing(reason_2),
      reason_3 = last_nonmissing(reason_3),
      reason_1_category = last_nonmissing(reason_1_category),
      reason_2_category = last_nonmissing(reason_2_category),
      reason_3_category = last_nonmissing(reason_3_category),
      .groups = "drop"
    )

  prior_df <- base_init_df |>
    dplyr::left_join(drug2, by = "id") |>
    dplyr::filter(generic_start_date < init_date) |>
    dplyr::group_by(id, init_date) |>
    dplyr::slice_max(generic_start_date, n = 1, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::transmute(
      id,
      init_date,
      base_prev_generic = generic_key,
      base_prev_reason_1 = reason_1,
      base_prev_reason_2 = reason_2,
      base_prev_reason_3 = reason_3,
      base_prev_reason_1_category = reason_1_category,
      base_prev_reason_2_category = reason_2_category,
      base_prev_reason_3_category = reason_3_category
    )

  join_by <- stats::setNames(
    c("id", "init_date"),
    c(rlang::as_name(id_col), rlang::as_name(init_date_col))
  )

  out <- base_visit_df |>
    dplyr::left_join(prior_df, by = join_by)

  out <- out |>
    dplyr::mutate(
      base_prev_reason_1 = apply_label_attrs(base_prev_reason_1, label_meta$base_prev_reason_1),
      base_prev_reason_2 = apply_label_attrs(base_prev_reason_2, label_meta$base_prev_reason_2),
      base_prev_reason_3 = apply_label_attrs(base_prev_reason_3, label_meta$base_prev_reason_3),
      base_prev_reason_1_category = apply_label_attrs(base_prev_reason_1_category, label_meta$base_prev_reason_1_category),
      base_prev_reason_2_category = apply_label_attrs(base_prev_reason_2_category, label_meta$base_prev_reason_2_category),
      base_prev_reason_3_category = apply_label_attrs(base_prev_reason_3_category, label_meta$base_prev_reason_3_category)
    )

  return(out)
}
