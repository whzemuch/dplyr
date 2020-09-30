DataMask <- R6Class("DataMask",
  public = list(
    initialize = function(data, caller) {
      private$data <- data

      names_bindings <- chr_unserialise_unicode(names2(data))
      if (anyDuplicated(names_bindings)) {
        abort("Can't transform a data frame with duplicate names.")
      }

      private$caller <- caller

      rows <- group_rows(data)
      # workaround for when there are 0 groups
      if (length(rows) == 0) {
        rows <- list(integer())
      }
      private$rows <- rows

      frame <- caller_env(n = 2)
      local_mask(self, frame)

      private$chops <- dplyr_lazy_vec_chop(data, rows, caller)
      private$masks <- dplyr_data_masks(private$chops, data, rows)

      private$keys <- group_keys(data)
      private$group_vars <- group_vars(data)
    },

    add = function(name, chunks) {
      if (inherits(private$data, "rowwise_df")){
        is_scalar_list <- function(.x) {
          vec_is_list(.x) && length(.x) == 1L
        }
        if (all(map_lgl(chunks, is_scalar_list))) {
          chunks <- map(chunks, `[[`, 1L)
        }
      }

      .Call(`dplyr_mask_add`, private, name, chunks)
    },

    set = function(name, chunks) {
      .Call(`dplyr_mask_set`, private, name, chunks)
    },

    remove = function(name) {
      # self$set(name, NULL)
      # env_unbind(private$bindings, name)
    },

    resolve = function(name) {
      chunks <- self$get_resolved(name)

      if (is.null(chunks)) {
        column <- private$data[[name]]
        chunks <- vec_chop(column, private$rows)
        if (inherits(private$data, "rowwise_df") && vec_is_list(column)) {
          chunks <- map(chunks, `[[`, 1)
        }
        self$set(name, chunks)
      }

      chunks
    },

    eval_all = function(quosures, fn, auto_names = names(quosures) %||% paste0(".quosure_", seq_along(quosures))) {
      withCallingHandlers(
        .Call(dplyr_eval_tidy_all, quosures, private$chops, private$masks, private$caller, auto_names, private, fn),
        error = function(e) {

          # retrieve context information
          .data <- private$data
          index_expression <- private$current_expression
          index_group <- private$current_group

          # TODO: handle when index_group = -1: error in hybrid eval
          local_call_step(dots = quosures, .index = index_expression, .fn = fn,
                          .dot_data = inherits(e, "rlang_error_data_pronoun_not_found"))
          call_step <- peek_call_step()
          error_name <- call_step$error_name
          error_expression <- call_step$error_expression

          msg <- conditionMessage(e)
          rowwise_hint <- FALSE

          if (!is.null(private$current_error)) {
            error_type <- private$current_error$type
            error_data <- private$current_error$data

            msg <- switch(error_type,
              "filter_incompatible_type_in_column" = glue("Input `{error_name}${error_data[[2]]}` must be a logical vector, not {friendly_type_of(error_data[[1]])}."),
              "filter_incompatible_type"           = glue("Input `{error_name}` must be a logical vector, not {friendly_type_of(error_data)}."),
              "incompatible_type"                  = glue("Input `{error_name}` must be a vector, not {friendly_type_of(error_data)}."),
              conditionMessage(e)
            )

            if (error_type == "incompatible_type" && inherits(.data, "rowwise_df")) {
              rowwise_hint <- TRUE
            }
          }

          bullets <- c(
            glue("Problem with `{fn}()` input `{error_name}`."),
            x = msg,
            i = glue("Input `{error_name}` is `{error_expression}`.")
          )

          if (rowwise_hint) {
            bullets <- c(bullets, i = glue("Did you mean: `{error_name} = list({error_expression})` ?"))
          }

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

          abort(bullets)
        }
      )
    },

    combine_filter = function(lists) {
      .Call(`dplyr_combine_filter`, lists, nrow(private$data), private$rows)
    },

    eval_all_summarise = function(quo) {
      .Call(`dplyr_mask_eval_all_summarise`, quo, private)
    },

    eval_all_mutate = function(quo) {
      .Call(`dplyr_mask_eval_all_mutate`, quo, private)
    },

    eval_all_filter = function(quos, env_filter) {
      .Call(`dplyr_mask_eval_all_filter`, quos, private, nrow(private$data), env_filter)
    },

    pick = function(vars) {
      cols <- self$current_cols(vars)
      nrow <- length(self$current_rows())
      new_tibble(cols, nrow = nrow)
    },

    current_cols = function(vars) {
      current_mask <- private$masks[[private$current_group]]

      # TODO: not sure about this
      env_get_list(current_mask$.top_env, vars)
    },

    current_rows = function() {
      private$rows[[self$get_current_group()]]
    },

    current_key = function() {
      vec_slice(private$keys, self$get_current_group())
    },

    current_vars = function() {
      names(private$data)
    },

    current_non_group_vars = function() {
      current_vars <- self$current_vars()
      setdiff(current_vars, private$group_vars)
    },

    get_current_group = function() {
      # The [] is so that we get a copy, which is important for how
      # current_group is dealt with internally, to avoid defining it at each
      # iteration of the dplyr_mask_eval_*() loops.
      private$current_group[]
    },

    set_current_group = function(group) {
      private$current_group <- group
    },

    full_data = function() {
      private$data
    },

    get_used = function() {
      .Call(env_resolved, private$chops, names(private$data))
    },

    unused_vars = function() {
      used <- self$get_used()
      current_vars <- self$current_vars()
      current_vars[!used]
    },

    get_rows = function() {
      private$rows
    },

    across_cols = function() {
      original_data <- self$full_data()
      original_data <- unclass(original_data)

      across_vars <- self$current_non_group_vars()
      unused_vars <- self$unused_vars()
      across_vars_unused <- intersect(across_vars, unused_vars)
      across_vars_used <- setdiff(across_vars, across_vars_unused)

      # Pull unused vars from original data to keep from marking them as used.
      # Column lengths will not match if `original_data` is grouped, but for
      # the usage of tidyselect in `across()` we only need the column names
      # and types to be correct.
      cols_unused <- original_data[across_vars_unused]
      cols_used <- self$current_cols(across_vars_used)

      cols <- vec_c(cols_unused, cols_used)

      # Match original ordering
      cols <- cols[across_vars]

      cols
    },

    across_cache_get = function(key) {
      private$across_cache[[key]]
    },

    across_cache_add = function(key, value) {
      private$across_cache[[key]] <- value
    },

    across_cache_reset = function() {
      private$across_cache <- list()
    }

  ),

  private = list(
    data = NULL,
    caller = NULL, # caller environment

    chops = NULL,  # an environment with chops (maybe promises of each column of data)
    masks = NULL,  # a list of environments, one for each group with (promises to) slices for each column

    current_group = NA_integer_,
    current_expression = NA_integer_,
    current_error = NULL,

    group_vars = character(),
    rows = NULL,
    keys = NULL,
    across_cache = list()
  )
)
