#include "dplyr.h"

namespace dplyr {

void stop_filter_incompatible_size(R_xlen_t i, SEXP quos, R_xlen_t nres, R_xlen_t n) {
  DPLYR_ERROR_INIT(3);
    DPLYR_ERROR_SET(0, "index", Rf_ScalarInteger(i + 1));
    DPLYR_ERROR_SET(1, "size", Rf_ScalarInteger(nres));
    DPLYR_ERROR_SET(2, "expected_size", Rf_ScalarInteger(n));

  DPLYR_ERROR_MESG_INIT(1);
    DPLYR_ERROR_MSG_SET(0, "Input `..{index}` must be of size {or_1(expected_size)}, not size {size}.");

  DPLYR_ERROR_THROW("dplyr:::filter_incompatible_size");
}

void stop_filter_incompatible_type(R_xlen_t i, SEXP quos, SEXP column_name, SEXP result){
  DPLYR_ERROR_INIT(3);
    DPLYR_ERROR_SET(0, "index", Rf_ScalarInteger(i + 1));
    DPLYR_ERROR_SET(1, "column_name", column_name);
    DPLYR_ERROR_SET(2, "result", result);

  DPLYR_ERROR_MESG_INIT(1);
    if (column_name == R_NilValue) {
      DPLYR_ERROR_MSG_SET(0, "Input `..{index}` must be a logical vector, not a {vec_ptype_full(result)}.");
    } else {
      DPLYR_ERROR_MSG_SET(0, "Input `..{index}${column_name}` must be a logical vector, not a {vec_ptype_full(result)}.");
    }

  DPLYR_ERROR_THROW("dplyr:::filter_incompatible_type");
}

}

bool all_lgl_columns(SEXP data) {
  R_xlen_t nc = XLENGTH(data);

  for (R_xlen_t i = 0; i < nc; i++) {
    if (TYPEOF(VECTOR_ELT(data, i)) != LGLSXP) return false;
  }

  return true;
}

void reduce_lgl(SEXP reduced, SEXP x, int n) {
  R_xlen_t nres = XLENGTH(x);
  int* p_reduced = LOGICAL(reduced);
  if (nres == 1) {
    if (LOGICAL(x)[0] != TRUE) {
      for (R_xlen_t i = 0; i < n; i++, ++p_reduced) {
        *p_reduced = FALSE;
      }
    }
  } else {
    int* p_x = LOGICAL(x);
    for (R_xlen_t i = 0; i < n; i++, ++p_reduced, ++p_x) {
      *p_reduced = *p_reduced == TRUE && *p_x == TRUE ;
    }
  }
}

void filter_check_size(SEXP res, int i, R_xlen_t n, SEXP quos) {
  R_xlen_t nres = vctrs::short_vec_size(res);
  if (nres != n && nres != 1) {
    dplyr::stop_filter_incompatible_size(i, quos, nres, n);
  }
}

void filter_check_type(SEXP res, R_xlen_t i, SEXP quos) {
  if (TYPEOF(res) == LGLSXP) return;

  if (Rf_inherits(res, "data.frame")) {
    R_xlen_t ncol = XLENGTH(res);
    if (ncol == 0) return;

    for (R_xlen_t j=0; j<ncol; j++) {
      SEXP res_j = VECTOR_ELT(res, j);
      if (TYPEOF(res_j) != LGLSXP) {
        SEXP colnames = PROTECT(Rf_getAttrib(res, R_NamesSymbol));
        SEXP colnames_j = PROTECT(Rf_allocVector(STRSXP, 1));
        SET_STRING_ELT(colnames_j, 0, STRING_ELT(colnames, j));
        dplyr::stop_filter_incompatible_type(i, quos, colnames_j, res_j);
        UNPROTECT(2);
      }
    }
  } else {
    dplyr::stop_filter_incompatible_type(i, quos, R_NilValue, res);
  }
}

SEXP eval_filter_one(SEXP quos, SEXP mask, SEXP caller, R_xlen_t n, SEXP env_filter) {
  // then reduce to a single logical vector of size n
  SEXP reduced = PROTECT(Rf_allocVector(LGLSXP, n));

  // init with TRUE
  int* p_reduced = LOGICAL(reduced);
  for (R_xlen_t i = 0; i < n ; i++, ++p_reduced) {
    *p_reduced = TRUE;
  }

  // reduce
  R_xlen_t nquos = XLENGTH(quos);
  for (R_xlen_t i=0; i < nquos; i++) {
    SEXP current_expression = PROTECT(Rf_ScalarInteger(i+1));
    Rf_defineVar(dplyr::symbols::current_expression, current_expression, env_filter);

    SEXP res = PROTECT(rlang::eval_tidy(VECTOR_ELT(quos, i), mask, caller));

    filter_check_size(res, i, n, quos);
    filter_check_type(res, i, quos);

    if (TYPEOF(res) == LGLSXP) {
      reduce_lgl(reduced, res, n);
    } else if(Rf_inherits(res, "data.frame")) {
      R_xlen_t ncol = XLENGTH(res);
      for (R_xlen_t j=0; j<ncol; j++) {
        reduce_lgl(reduced, VECTOR_ELT(res, j), n);
      }
    }

    UNPROTECT(2);
  }

  UNPROTECT(1);
  return reduced;
}

SEXP dplyr_combine_filter(SEXP lists, SEXP s_n, SEXP rows) {
  R_xlen_t n = Rf_asInteger(s_n);
  R_xlen_t n_groups = XLENGTH(rows);

  SEXP keep = PROTECT(Rf_allocVector(LGLSXP, n));
  int* p_keep = LOGICAL(keep);
  for (R_xlen_t i = 0; i < n; i++) {
    p_keep[i] = TRUE;
  }

  for (R_xlen_t i = 0; i < n_groups; i++) {
    SEXP rows_i = VECTOR_ELT(rows, i);
    R_xlen_t n_i = XLENGTH(rows_i);
    int* p_rows_i = INTEGER(rows_i);

    SEXP list_i = VECTOR_ELT(lists, i);
    R_xlen_t n_exprs = XLENGTH(list_i);
    for (R_xlen_t j = 0; j < n_exprs; j++) {
      SEXP result_i_j = PROTECT(VECTOR_ELT(list_i, j));
      R_xlen_t n_result_i_j = vctrs::short_vec_size(result_i_j);
      if (n_result_i_j != n_i) {
        if (n_result_i_j == 1) {
          UNPROTECT(1);

          // TODO: there should be no need for recycling
          result_i_j = PROTECT(vctrs::short_vec_recycle(result_i_j, n_i));
        }
      }

      if (Rf_inherits(result_i_j, "data.frame")) {
        R_xlen_t ncol_result_i_j = XLENGTH(result_i_j);
        for (R_xlen_t k = 0; k < ncol_result_i_j; k++) {
          SEXP result_i_j_k = VECTOR_ELT(result_i_j, k);
          int* p_result_i_j_k = LOGICAL(result_i_j_k);
          for (R_xlen_t i_keep = 0; i_keep < n_i; i_keep++) {
            if (p_result_i_j_k[i_keep] != TRUE) {
              p_keep[p_rows_i[i_keep] - 1] = FALSE;
            }
          }
        }
      } else if (TYPEOF(result_i_j) == LGLSXP){
        int* p_result_i_j = LOGICAL(result_i_j);
        for (R_xlen_t i_keep = 0; i_keep < n_i; i_keep++) {
          if (p_result_i_j[i_keep] != TRUE) {
            p_keep[p_rows_i[i_keep] - 1] = FALSE;
          }
        }
      }

      UNPROTECT(1);
    }
  }

  UNPROTECT(1);
  return keep;
}


