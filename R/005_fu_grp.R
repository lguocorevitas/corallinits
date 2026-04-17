
#' Title
#'
#' @param visit_df
#' @param id_col
#' @param visit_date_col
#' @param init_date_col
#' @param stop_date_col
#' @param fu6_window
#' @param fu12_window
#' @param fu18_window
#' @param fu24_window
#' @param fu6_target
#' @param fu12_target
#' @param fu18_target
#' @param fu24_target
#'
#' @returns
#' @export
#'
#' @examples
#' assign fu_grp when fu_days falls in the window
#' only apply the de-duplication rule when there is more than one candidate
#' for the same id and follow-up window
#' use stop_date logic to choose one row among duplicate
#' init_upadacitinib <- corallinits::identify_fu_visits(
#' visit_df = init_upadacitinib,
#' id_col = id,
#' visit_date_col = visitdate,
#' init_date_col = init_date,
#' stop_date_col = first_stop_date,
#' fu6_window = c(91, 273),
#' fu12_window = c(274, 457),
#' fu18_window = c(458, 638),
#' fu24_window = c(639, 819)
#' )
identify_fu_visits <- function(
    visit_df,
    id_col = id,
    visit_date_col = visitdate,
    init_date_col = init_date,
    stop_date_col = stop_date,
    fu6_window = c(91, 274),
    fu12_window = c(275, 457),
    fu18_window = c(458, 638),
    fu24_window = c(639, 819),
    fu6_target = 183,
    fu12_target = 365,
    fu18_target = 548,
    fu24_target = 730
) {

  id_col <- rlang::enquo(id_col)
  visit_date_col <- rlang::enquo(visit_date_col)
  init_date_col <- rlang::enquo(init_date_col)
  stop_date_col <- rlang::enquo(stop_date_col)

  dat <- visit_df |>
    dplyr::mutate(
      .row_id__ = dplyr::row_number(),
      .id__ = !!id_col,
      .visitdate__ = as.Date(!!visit_date_col),
      .init_date__ = as.Date(!!init_date_col),
      .stop_date__ = as.Date(!!stop_date_col),
      fu_days = as.integer(.visitdate__ - .init_date__)
    )

  choose_one_window <- function(df_id, grp_value, day_range, target_day) {

    lower <- day_range[1]
    upper <- day_range[2]

    cand <- df_id |>
      dplyr::filter(!is.na(fu_days), fu_days >= lower, fu_days <= upper)

    if (nrow(cand) == 0) {
      return(NULL)
    }

    if (nrow(cand) == 1) {
      return(
        cand |>
          dplyr::mutate(fu_grp = as.integer(grp_value))
      )
    }

    stop_dates <- df_id$.stop_date__[!is.na(df_id$.stop_date__)]
    first_stop_date <- if (length(stop_dates) == 0) as.Date(NA) else min(stop_dates)

    if (!is.na(first_stop_date)) {
      stop_fu_days <- as.integer(first_stop_date - df_id$.init_date__[1])

      # Rule 1: if stopped during the FU window, use the stopped visit
      if (!is.na(stop_fu_days) && stop_fu_days >= lower && stop_fu_days <= upper) {

        # First choice: exact stopped visit
        exact_stop_visit <- cand |>
          dplyr::filter(.visitdate__ == first_stop_date)

        if (nrow(exact_stop_visit) > 0) {
          chosen <- exact_stop_visit |>
            dplyr::arrange(.row_id__) |>
            dplyr::slice_head(n = 1)

          return(
            chosen |>
              dplyr::mutate(fu_grp = as.integer(grp_value))
          )
        }

        # Second choice: earliest visit after stop date
        after_stop_visit <- cand |>
          dplyr::filter(.visitdate__ > first_stop_date) |>
          dplyr::arrange(.visitdate__, .row_id__) |>
          dplyr::slice_head(n = 1)

        if (nrow(after_stop_visit) > 0) {
          return(
            after_stop_visit |>
              dplyr::mutate(fu_grp = as.integer(grp_value))
          )
        }

        # Third choice: latest visit before stop date
        before_stop_visit <- cand |>
          dplyr::filter(.visitdate__ < first_stop_date) |>
          dplyr::arrange(dplyr::desc(.visitdate__), dplyr::desc(.row_id__)) |>
          dplyr::slice_head(n = 1)

        if (nrow(before_stop_visit) > 0) {
          return(
            before_stop_visit |>
              dplyr::mutate(fu_grp = as.integer(grp_value))
          )
        }
      }

      # Rule 2: if stopped prior to the FU window, use the earlier visit
      if (!is.na(stop_fu_days) && stop_fu_days < lower) {
        chosen <- cand |>
          dplyr::arrange(.visitdate__, .row_id__) |>
          dplyr::slice_head(n = 1)

        return(
          chosen |>
            dplyr::mutate(fu_grp = as.integer(grp_value))
        )
      }
    }

    # Rule 3: stopped later than FU window or no stop
    # choose closest to target day; if tie, use later visit
    chosen <- cand |>
      dplyr::mutate(.dist__ = abs(fu_days - target_day)) |>
      dplyr::arrange(.dist__, dplyr::desc(.visitdate__), dplyr::desc(.row_id__)) |>
      dplyr::slice_head(n = 1) |>
      dplyr::mutate(fu_grp = as.integer(grp_value)) |>
      dplyr::select(-.dist__)

    chosen
  }

  selected_rows <- dat |>
    dplyr::group_by(.id__) |>
    dplyr::group_modify(function(.x, .y) {
      dplyr::bind_rows(
        choose_one_window(.x, 6,  fu6_window,  fu6_target),
        choose_one_window(.x, 12, fu12_window, fu12_target),
        choose_one_window(.x, 18, fu18_window, fu18_target),
        choose_one_window(.x, 24, fu24_window, fu24_target)
      )
    }) |>
    dplyr::ungroup() |>
    dplyr::select(.row_id__, fu_grp)

  out <- dat |>
    dplyr::left_join(selected_rows, by = ".row_id__") |>
    dplyr::mutate(
      fu_grp = as.integer(fu_grp)
    ) |>
    dplyr::select(-.row_id__, -.id__, -.visitdate__, -.init_date__, -.stop_date__)

  out
}
