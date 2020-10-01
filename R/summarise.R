#' Summarise each group to fewer rows
#'
#' @description
#' `summarise()` creates a new data frame. It will have one (or more) rows for
#' each combination of grouping variables; if there are no grouping variables,
#' the output will have a single row summarising all observations in the input.
#' It will contain one column for each grouping variable and one column
#' for each of the summary statistics that you have specified.
#'
#' `summarise()` and `summarize()` are synonyms.
#'
#' @section Useful functions:
#'
#' * Center: [mean()], [median()]
#' * Spread: [sd()], [IQR()], [mad()]
#' * Range: [min()], [max()], [quantile()]
#' * Position: [first()], [last()], [nth()],
#' * Count: [n()], [n_distinct()]
#' * Logical: [any()], [all()]
#'
#' @section Backend variations:
#'
#' The data frame backend supports creating a variable and using it in the
#' same summary. This means that previously created summary variables can be
#' further transformed or combined within the summary, as in [mutate()].
#' However, it also means that summary variables with the same names as previous
#' variables overwrite them, making those variables unavailable to later summary
#' variables.
#'
#' This behaviour may not be supported in other backends. To avoid unexpected
#' results, consider using new names for your summary variables, especially when
#' creating multiple summaries.
#'
#' @export
#' @inheritParams arrange
#' @param ... <[`data-masking`][dplyr_data_masking]> Name-value pairs of summary
#'   functions. The name will be the name of the variable in the result.
#'
#'   The value can be:
#'
#'   * A vector of length 1, e.g. `min(x)`, `n()`, or `sum(is.na(y))`.
#'   * A vector of length `n`, e.g. `quantile()`.
#'   * A data frame, to add multiple columns from a single expression.
#' @param .groups \Sexpr[results=rd]{lifecycle::badge("experimental")} Grouping structure of the result.
#'
#'   * "drop_last": dropping the last level of grouping. This was the
#'   only supported option before version 1.0.0.
#'   * "drop": All levels of grouping are dropped.
#'   * "keep": Same grouping structure as `.data`.
#'   * "rowwise": Each row is it's own group.
#'
#'   When `.groups` is not specified, it is chosen
#'   based on the number of rows of the results:
#'   * If all the results have 1 row, you get "drop_last".
#'   * If the number of rows varies, you get "keep".
#'
#'   In addition, a message informs you of that choice, unless the
#'   option "dplyr.summarise.inform" is set to `FALSE`, or when `summarise()`
#'   is called from a function in a package.
#'
#' @family single table verbs
#' @return
#' An object _usually_ of the same type as `.data`.
#'
#' * The rows come from the underlying [group_keys()].
#' * The columns are a combination of the grouping keys and the summary
#'   expressions that you provide.
#' * The grouping structure is controlled by the `.groups=` argument, the
#'   output may be another [grouped_df], a [tibble] or a [rowwise] data frame.
#' * Data frame attributes are **not** preserved, because `summarise()`
#'   fundamentally creates a new data frame.
#' @section Methods:
#' This function is a **generic**, which means that packages can provide
#' implementations (methods) for other classes. See the documentation of
#' individual methods for extra arguments and differences in behaviour.
#'
#' The following methods are currently available in loaded packages:
#' \Sexpr[stage=render,results=rd]{dplyr:::methods_rd("summarise")}.
#' @examples
#' # A summary applied to ungrouped tbl returns a single row
#' mtcars %>%
#'   summarise(mean = mean(disp), n = n())
#'
#' # Usually, you'll want to group first
#' mtcars %>%
#'   group_by(cyl) %>%
#'   summarise(mean = mean(disp), n = n())
#'
#' # dplyr 1.0.0 allows to summarise to more than one value:
#' mtcars %>%
#'    group_by(cyl) %>%
#'    summarise(qs = quantile(disp, c(0.25, 0.75)), prob = c(0.25, 0.75))
#'
#' # You use a data frame to create multiple columns so you can wrap
#' # this up into a function:
#' my_quantile <- function(x, probs) {
#'   tibble(x = quantile(x, probs), probs = probs)
#' }
#' mtcars %>%
#'   group_by(cyl) %>%
#'   summarise(my_quantile(disp, c(0.25, 0.75)))
#'
#' # Each summary call removes one grouping level (since that group
#' # is now just a single row)
#' mtcars %>%
#'   group_by(cyl, vs) %>%
#'   summarise(cyl_n = n()) %>%
#'   group_vars()
#'
#' # BEWARE: reusing variables may lead to unexpected results
#' mtcars %>%
#'   group_by(cyl) %>%
#'   summarise(disp = mean(disp), sd = sd(disp))
#'
#' # Refer to column names stored as strings with the `.data` pronoun:
#' var <- "mass"
#' summarise(starwars, avg = mean(.data[[var]], na.rm = TRUE))
#' # Learn more in ?dplyr_data_masking
summarise <- function(.data, ..., .groups = NULL) {
  UseMethod("summarise")
}
#' @rdname summarise
#' @export
summarize <- summarise

#' @export
summarise.data.frame <- function(.data, ..., .groups = NULL) {
  cols <- summarise_cols(.data, ...)
  out <- summarise_build(.data, cols)
  if (identical(.groups, "rowwise")) {
    out <- rowwise_df(out, character())
  }
  out
}

#' @export
summarise.grouped_df <- function(.data, ..., .groups = NULL) {
  cols <- summarise_cols(.data, ...)
  out <- summarise_build(.data, cols)
  verbose <- summarise_verbose(.groups, caller_env())

  if (is.null(.groups)) {
    if (cols$all_one) {
      .groups <- "drop_last"
    } else {
      .groups <- "keep"
    }
  }

  group_vars <- group_vars(.data)
  if (identical(.groups, "drop_last")) {
    n <- length(group_vars)
    if (n > 1) {
      if (verbose) {
        new_groups <- glue_collapse(paste0("'", group_vars[-n], "'"), sep = ", ")
        summarise_inform("regrouping output by {new_groups}")
      }
      out <- grouped_df(out, group_vars[-n], group_by_drop_default(.data))
    } else {
      if (verbose) {
        summarise_inform("ungrouping output")
      }
    }
  } else if (identical(.groups, "keep")) {
    if (verbose) {
      new_groups <- glue_collapse(paste0("'", group_vars, "'"), sep = ", ")
      summarise_inform("regrouping output by {new_groups}")
    }
    out <- grouped_df(out, group_vars, group_by_drop_default(.data))
  } else if (identical(.groups, "rowwise")) {
    out <- rowwise_df(out, group_vars)
  } else if(!identical(.groups, "drop")) {
    abort(c(
      paste0("`.groups` can't be ", as_label(.groups)),
      i = 'Possible values are NULL (default), "drop_last", "drop", "keep", and "rowwise"'
    ))
  }

  out
}

#' @export
summarise.rowwise_df <- function(.data, ..., .groups = NULL) {
  cols <- summarise_cols(.data, ...)
  out <- summarise_build(.data, cols)
  verbose <- summarise_verbose(.groups, caller_env())

  group_vars <- group_vars(.data)
  if (is.null(.groups) || identical(.groups, "keep")) {
    if (verbose) {
      if (length(group_vars)) {
        new_groups <- glue_collapse(paste0("'", group_vars, "'"), sep = ", ")
        summarise_inform("regrouping output by {new_groups}")
      } else {
        summarise_inform("ungrouping output")
      }
    }
    out <- grouped_df(out, group_vars)
  } else if (identical(.groups, "rowwise")) {
    out <- rowwise_df(out, group_vars)
  } else if (!identical(.groups, "drop")) {
    abort(c(
      paste0("`.groups` can't be ", as_label(.groups)),
      i = 'Possible values are NULL (default), "drop", "keep", and "rowwise"'
    ))
  }

  out
}

summarise_cols <- function(.data, ...) {
  mask <- DataMask$new(.data, caller_env())

  ## apply
  dots <- enquos(...)
  dots_names <- names(dots)
  auto_named_dots <- names(enquos(..., .named = TRUE))

  lists <- mask$eval_all(dots, fn = "summarise", auto_names = names(exprs_auto_name(dots)))

  ## combine
  tibbles <- withCallingHandlers(map2(lists, seq_along(lists), function(.x, .group) {
    # skip data frames with 0 columns - should df_list() do this ?
    .x <- map2(.x, names(.x), function(result, name) {
      if (is.data.frame(result) && name == "" && ncol(result) == 0) {
        result <- NULL
      }
      result
    })
    mask$set_current_group(.group)
    new_data_frame(df_list(!!!.x))
  }), error = function(e) {
    if (inherits(e, "vctrs_error_incompatible_size")) {
      bullets <- c(
        glue("Problem with `summarise()` input `{e$y_arg}.`"),
        x = glue("Input `{e$y_arg}` must be size {or_1(e$x_size)}, not {e$y_size}."),
        i = glue("Input `{e$x_arg}` had size {e$x_size}.")
      )

      index_group <- mask$get_current_group()
      if (is_grouped_df(.data)) {
        keys <- group_keys(.data)[index_group, ]
        bullets <- c(bullets, i = glue("The error occurred in group {index_group}: {group_labels_details(keys)}."))
      } else if (inherits(.data, "rowwise_df")) {
        keys <- group_keys(.data)[index_group, ]
        if (ncol(keys)) {
          bullets <- c(bullets, i = glue("The error occurred in row {index_group}: {group_labels_details(keys)}."))
        } else {
          bullets <- c(bullets, i = glue("The error occurred in row {index_group}."))
        }
      }

      index_error_expression <- which(names(lists[[index_group]]) == e$y_arg)
      error_expression <- deparse(quo_squash(dots[[index_error_expression]]))

      bullets <- c(bullets,
        i = glue("Input `{e$y_arg}` is `{error_expression}`.")
      )

    } else {
      bullets <- conditionMessage(e)
    }
    abort(bullets, parent = e)
  })
  sizes <- list_sizes(tibbles)

  cols <- withCallingHandlers(vec_rbind(!!!tibbles), error = function(e) {
    if (inherits(e, "vctrs_error_incompatible_type")) {
      input_name <- sub("^.*[$]", "", e$x_arg)
      bullets <- c(
        glue("Problem with `summarise()` input `{input_name}`."),
        x = glue("Input `{input_name}` must return compatible vectors across groups."),
        i = cnd_bullet_combine_details2(e$x, e$x_arg, .data),
        i = cnd_bullet_combine_details2(e$y, e$y_arg, .data)

        # TODO: this misses "Input {input_name} is .." because this happens after
        #       the df_list() call and so the problem might come from
        #       one column that has been auto spliced.
        #
        #       or maybe this should rather be:
        #
        #  Error: Problem with combining `summarise()` results.
        #  x Can't combine `..1$a` <double> and `..2$a` <character>.
      )
    } else {
      bullets <- conditionMessage(e)
    }
    abort(bullets, parent = e)
  })

  list(new = cols, size = sizes, all_one = all(sizes == 1L) || length(cols) == 0L)
}

summarise_build <- function(.data, cols) {
  out <- group_keys(.data)
  if (!cols$all_one) {
    out <- vec_slice(out, rep(seq_len(nrow(out)), cols$size))
  }
  dplyr_col_modify(out, cols$new)
}


# messaging ---------------------------------------------------------------

summarise_verbose <- function(.groups, .env) {
  is.null(.groups) &&
    is_reference(topenv(.env), global_env()) &&
    !identical(getOption("dplyr.summarise.inform"), FALSE)
}

summarise_inform <- function(..., .env = parent.frame()) {
  inform(paste0(
    "`summarise()` ", glue(..., .envir = .env), " (override with `.groups` argument)"
  ))
}
