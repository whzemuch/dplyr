#include "dplyr.h"
#include <string>

SEXP new_environment(int size, SEXP parent)  {
  SEXP call = PROTECT(Rf_lang4(Rf_install("new.env"), Rf_ScalarLogical(TRUE), parent, Rf_ScalarInteger(size)));
  SEXP res = Rf_eval(call, R_BaseEnv);
  UNPROTECT(1);
  return res;
}

void dplyr_lazy_vec_chop_grouped(SEXP chops_env, SEXP data, SEXP rows, bool rowwise) {
  SEXP names = PROTECT(Rf_getAttrib(data, R_NamesSymbol));
  R_xlen_t n = XLENGTH(data);

  for (R_xlen_t i = 0; i < n; i++) {
    SEXP prom = PROTECT(Rf_allocSExp(PROMSXP));
    SET_PRENV(prom, R_EmptyEnv);
    SEXP column = VECTOR_ELT(data, i);
    if (rowwise && vctrs::vec_is_list(column)) {
      SET_PRCODE(prom, column);
    } else {
      SET_PRCODE(prom, Rf_lang3(dplyr::functions::vec_chop, column, rows));
    }
    SET_PRVALUE(prom, R_UnboundValue);

    Rf_defineVar(Rf_installChar(STRING_ELT(names, i)), prom, chops_env);
    UNPROTECT(1);
  }

  UNPROTECT(1);
}

void dplyr_lazy_vec_chop_ungrouped(SEXP chops_env, SEXP data) {
  SEXP names = PROTECT(Rf_getAttrib(data, R_NamesSymbol));
  R_xlen_t n = XLENGTH(data);

  for (R_xlen_t i = 0; i < n; i++) {
    SEXP prom = PROTECT(Rf_allocSExp(PROMSXP));
    SET_PRENV(prom, R_EmptyEnv);
    SET_PRCODE(prom, Rf_lang2(dplyr::functions::list, VECTOR_ELT(data, i)));
    SET_PRVALUE(prom, R_UnboundValue);

    Rf_defineVar(Rf_installChar(STRING_ELT(names, i)), prom, chops_env);
    UNPROTECT(1);
  }

  UNPROTECT(1);
}

SEXP dplyr_lazy_vec_chop(SEXP data, SEXP rows, SEXP caller_env) {
  SEXP chops_env = PROTECT(new_environment(XLENGTH(data), caller_env));
  if (Rf_inherits(data, "grouped_df")) {
    dplyr_lazy_vec_chop_grouped(chops_env, data, rows, false);
  } else if (Rf_inherits(data, "rowwise_df")) {
    dplyr_lazy_vec_chop_grouped(chops_env, data, rows, true);
  } else {
    dplyr_lazy_vec_chop_ungrouped(chops_env, data);
  }
  UNPROTECT(1);
  return chops_env;
}

SEXP dplyr_data_masks_setup(SEXP chops_env, SEXP data, SEXP rows) {
  SEXP names = PROTECT(Rf_getAttrib(data, R_NamesSymbol));

  R_xlen_t n_groups = XLENGTH(rows);
  R_xlen_t n_columns = XLENGTH(data);

  // create masks
  R_xlen_t mask_size = n_columns + 20;
  SEXP masks = PROTECT(Rf_allocVector(VECSXP, n_groups));

  for (R_xlen_t i = 0; i < n_groups; i++) {
    SEXP mask_metadata_env = PROTECT(new_environment(2, R_EmptyEnv));
    Rf_defineVar(dplyr::symbols::dot_indices, VECTOR_ELT(rows, i), mask_metadata_env);
    Rf_defineVar(dplyr::symbols::current_group, Rf_ScalarInteger(i+1), mask_metadata_env);

    SEXP mask = PROTECT(new_environment(mask_size, mask_metadata_env));

    SET_VECTOR_ELT(masks, i, mask);
    UNPROTECT(2);
  }

  for (R_xlen_t i = 0; i < n_columns; i++) {
    SEXP name = Rf_installChar(STRING_ELT(names, i));

    for (R_xlen_t j = 0; j < n_groups; j++) {
      // promise of the slice for column {name} and group {j}
      SEXP prom = PROTECT(Rf_allocSExp(PROMSXP));
      SET_PRENV(prom, chops_env);
      SET_PRCODE(prom, Rf_lang3(dplyr::functions::dot_subset2, name, Rf_ScalarInteger(j + 1)));
      SET_PRVALUE(prom, R_UnboundValue);

      Rf_defineVar(name, prom, VECTOR_ELT(masks, j));
      UNPROTECT(1);
    }
  }

  UNPROTECT(2);
  return masks;
}

SEXP env_resolved(SEXP env, SEXP names) {
  R_xlen_t n = XLENGTH(names);
  SEXP res = PROTECT(Rf_allocVector(LGLSXP, n));

  int* p_res = LOGICAL(res);
  for(R_xlen_t i = 0; i < n; i++) {
    SEXP prom = Rf_findVarInFrame(env, Rf_installChar(STRING_ELT(names, i)));
    p_res[i] = PRVALUE(prom) != R_UnboundValue;
  }

  Rf_namesgets(res, names);
  UNPROTECT(1);
  return res;
}

namespace funs {

SEXP eval_hybrid(SEXP quo, SEXP chops) {
  SEXP call = PROTECT(Rf_lang3(dplyr::functions::eval_hybrid, quo, chops));
  SEXP res = PROTECT(Rf_eval(call, R_BaseEnv));
  UNPROTECT(2);

  return res;
}

}

enum Function {
  FILTER,
  SLICE,
  MUTATE,
  SUMMARISE,

  OTHER
};

Function function_case(SEXP fn) {
  std::string fn_name(CHAR(STRING_ELT(fn, 0)));

  if (fn_name == "filter") {
    return FILTER;
  } else if (fn_name == "slice") {
    return SLICE;
  } else if (fn_name == "mutate") {
    return MUTATE;
  } else if (fn_name == "summarise") {
    return SUMMARISE;
  } else {
    return OTHER;
  }
}


SEXP dplyr_eval_tidy_all(SEXP quosures, SEXP chops, SEXP masks, SEXP caller_env, SEXP auto_names, SEXP private_env, SEXP fn) {
  R_xlen_t n_expr = XLENGTH(quosures);
  SEXP names = PROTECT(Rf_getAttrib(quosures, R_NamesSymbol));
  if (names == R_NilValue) {
    UNPROTECT(1);
    names = PROTECT(Rf_allocVector(STRSXP, n_expr));
  }

  R_xlen_t n_masks = XLENGTH(masks);

  Function fn_case = function_case(fn);

  // initialize all results
  SEXP res = PROTECT(Rf_allocVector(VECSXP, n_masks));
  for (R_xlen_t i = 0; i < n_masks; i++) {
    SEXP res_i = PROTECT(Rf_allocVector(VECSXP, n_expr));
    Rf_namesgets(res_i, names);
    SET_VECTOR_ELT(res, i, res_i);
    UNPROTECT(1);
  }

  SEXP list_indices = Rf_findVarInFrame(private_env, dplyr::symbols::rows);

  SEXP index_expression = Rf_findVarInFrame(private_env, dplyr::symbols::current_expression);
  int *p_index_expression = INTEGER(index_expression);

  SEXP index_group = Rf_findVarInFrame(private_env, dplyr::symbols::current_group);
  int* p_index_group = INTEGER(index_group);

  // eval all the things
  for (R_xlen_t i_expr = 0; i_expr < n_expr; i_expr++) {
    *p_index_expression = i_expr + 1;

    SEXP quo = VECTOR_ELT(quosures, i_expr);

    SEXP name = STRING_ELT(names, i_expr);
    SEXP auto_name = STRING_ELT(auto_names, i_expr);

    *p_index_group = -1;
    SEXP hybrid_result = PROTECT(funs::eval_hybrid(quo, chops));
    if (hybrid_result != R_NilValue) {

      if (TYPEOF(hybrid_result) != VECSXP || XLENGTH(hybrid_result) != n_masks) {
        Rf_error("Malformed hybrid result, not a list");
      }

      SEXP ptype = Rf_getAttrib(hybrid_result, dplyr::symbols::ptype);
      if (ptype == R_NilValue) {
        Rf_error("Malformed hybrid result, needs ptype");
      }

      if (XLENGTH(name) == 0) {
        // if @ptype is a data frame, then auto splice as we go
        // this assumes all results exactly match the ptype
        if (Rf_inherits(ptype, "data.frame")) {
          R_xlen_t n_results = XLENGTH(ptype);

          SEXP result_names = Rf_getAttrib(ptype, R_NamesSymbol);
          SEXP result_symbols = Rf_allocVector(VECSXP, n_results);

          // only install once
          for (R_xlen_t i_result = 0; i_result < n_results; i_result++) {
            SET_VECTOR_ELT(result_symbols, i_result, Rf_installChar(STRING_ELT(result_names, i_result)));
          }

          for (R_xlen_t i_group = 0; i_group < n_masks; i_group++) {
            SEXP res_i = VECTOR_ELT(hybrid_result, i_group);
            SET_VECTOR_ELT(VECTOR_ELT(res, i_group), i_expr, res_i);
            SEXP mask = VECTOR_ELT(masks, i_group);

            for (R_xlen_t i_result = 0; i_result < n_results; i_result++) {
              Rf_defineVar(
                VECTOR_ELT(result_symbols, i_result),
                VECTOR_ELT(hybrid_result, i_result),
                mask
              );
            }
          }

        } else {
          // unnamed, but not a data frame, so use the deduced name

          SEXP s_auto_name = Rf_installChar(auto_name);

          for (R_xlen_t i_group = 0; i_group < n_masks; i_group++) {
            SEXP hybrid_res_i = VECTOR_ELT(hybrid_result, i_group);

            SEXP res_i = VECTOR_ELT(res, i_group);
            SET_VECTOR_ELT(res_i, i_expr, hybrid_res_i);

            SEXP names_res_i = Rf_getAttrib(res_i, R_NamesSymbol);
            SET_STRING_ELT(names_res_i, i_expr, auto_name);

            Rf_defineVar(s_auto_name, hybrid_res_i, VECTOR_ELT(masks, i_group));
          }
        }

      } else {
        SEXP s_name = Rf_installChar(name);

        // we have a proper name, so no auto splice or auto name use
        for (R_xlen_t i_group = 0; i_group < n_masks; i_group++) {
          SEXP res_i = VECTOR_ELT(hybrid_result, i_group);
          SET_VECTOR_ELT(VECTOR_ELT(res, i_group), i_expr, res_i);
          Rf_defineVar(s_name, res_i, VECTOR_ELT(masks, i_group));
        }
      }

    } else {
      for (R_xlen_t i_group = 0; i_group < n_masks; i_group++) {
        *p_index_group = i_group + 1;
        SEXP mask = VECTOR_ELT(masks, i_group);

        SEXP result = PROTECT(rlang::eval_tidy(quo, mask, caller_env));

        // check type
        if (fn_case == FILTER) {
          if (TYPEOF(result) != LGLSXP) {
            if (!Rf_inherits(result, "data.frame") || !all_lgl_columns(result)) {
              Rf_error("incompatible type: must be a logical vector");
            }
          }
        }

        // check size
        if (fn_case == FILTER || fn_case == MUTATE) {
          R_xlen_t result_size = vctrs::short_vec_size(result);
          SEXP indices = VECTOR_ELT(list_indices, i_group);
          R_xlen_t expected_size = XLENGTH(indices);

          if (result_size != 1 && result_size != expected_size) {
            if (expected_size == 1) {
              Rf_error("incompatible size: must be size 1, not size %d", result_size);
            } else {
              Rf_error("incompatible size: must be size 1 or %d, not size %d", expected_size, result_size);
            }
          }
        }

        SET_VECTOR_ELT(VECTOR_ELT(res, i_group), i_expr, result);

        if (XLENGTH(name) == 0) {
          if (Rf_inherits(result, "data.frame")) {
            R_xlen_t n_columns = XLENGTH(result);
            SEXP names_columns = PROTECT(Rf_getAttrib(result, R_NamesSymbol));
            for (R_xlen_t i_column = 0; i_column < n_columns; i_column++) {
              SEXP name_i = Rf_installChar(STRING_ELT(names_columns, i_column));
              Rf_defineVar(name_i, VECTOR_ELT(result, i_column), mask);
            }
            UNPROTECT(1);
          } else {
            SEXP s_auto_name = Rf_installChar(auto_name);

            // this uses an auto name instead of ""
            SEXP names_res_i = Rf_getAttrib(VECTOR_ELT(res, i_group), R_NamesSymbol);
            SET_STRING_ELT(names_res_i, i_expr, auto_name);

            Rf_defineVar(s_auto_name, result, mask);
          }
        } else {
          Rf_defineVar(Rf_installChar(name), result, mask);
        }

        UNPROTECT(1);
      }
    }
    UNPROTECT(1);
  }

  UNPROTECT(2);
  return res;
}
