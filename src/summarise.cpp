#include "dplyr.h"

namespace dplyr {

void stop_summarise_unsupported_type(SEXP result) {
  DPLYR_ERROR_INIT(1);
    DPLYR_ERROR_SET(0, "result", result);
  DPLYR_ERROR_MESG_INIT(0);
  DPLYR_ERROR_THROW("dplyr:::summarise_unsupported_type");
}

void stop_summarise_incompatible_size(int index_group, int index_expression, int expected_size, int size) {
  DPLYR_ERROR_INIT(4);
    DPLYR_ERROR_SET(0, "group", Rf_ScalarInteger(index_group + 1));
    DPLYR_ERROR_SET(1, "index", Rf_ScalarInteger(index_expression + 1));
    DPLYR_ERROR_SET(2, "expected_size", Rf_ScalarInteger(expected_size));
    DPLYR_ERROR_SET(3, "size", Rf_ScalarInteger(size));
  DPLYR_ERROR_MESG_INIT(0);
  DPLYR_ERROR_THROW("dplyr:::summarise_incompatible_size");
}

}


SEXP dplyr_mask_eval_all_summarise(SEXP quo, SEXP env_private) {
  DPLYR_MASK_INIT();

  SEXP chunks = PROTECT(Rf_allocVector(VECSXP, ngroups));
  for (R_xlen_t i = 0; i < ngroups; i++) {
    DPLYR_MASK_SET_GROUP(i);

    SEXP result_i = PROTECT(DPLYR_MASK_EVAL(quo, i));
    SET_VECTOR_ELT(chunks, i, result_i);

    if (!vctrs::vec_is_vector(result_i)) {
      dplyr::stop_summarise_unsupported_type(result_i);
    }

    UNPROTECT(1);
  }
  DPLYR_MASK_FINALISE();

  UNPROTECT(1);
  return chunks;
}

namespace funs {

SEXP eval_hybrid(SEXP quo, SEXP chops) {
  SEXP call = PROTECT(Rf_lang3(dplyr::functions::eval_hybrid, quo, chops));
  SEXP res = PROTECT(Rf_eval(call, R_BaseEnv));
  UNPROTECT(2);
  return res;
}

}

SEXP list_ptype_common(SEXP x) {
  SEXP ptype = Rf_getAttrib(x, dplyr::symbols::ptype);
  if (ptype != R_NilValue) {
    return ptype;
  }

  SEXP call = PROTECT(Rf_lang2(dplyr::symbols::bang, x));
  call = PROTECT(Rf_lang2(dplyr::symbols::bang, call));
  call = PROTECT(Rf_lang2(dplyr::symbols::bang, call));
  call = PROTECT(Rf_lang2(Rf_install("vec_ptype_common"), call));

  SETCAR( CDR(CADR(CADR(CADR(call)))), x ) ;
  ptype = Rf_eval(call, dplyr::envs::ns_vctrs);
  UNPROTECT(4);
  return ptype;
}

SEXP dplyr_eval_summarise(SEXP quosures, SEXP auto_names, SEXP env_private) {
  R_xlen_t n_expr = XLENGTH(quosures);
  SEXP names = PROTECT(Rf_getAttrib(quosures, R_NamesSymbol));
  if (names == R_NilValue) {
    UNPROTECT(1);
    names = PROTECT(Rf_allocVector(STRSXP, n_expr));
  }

  SEXP rows = PROTECT(Rf_findVarInFrame(env_private, dplyr::symbols::rows));
  R_xlen_t n_groups = XLENGTH(rows);
  SEXP caller = PROTECT(Rf_findVarInFrame(env_private, dplyr::symbols::caller));
  SEXP masks = PROTECT(Rf_findVarInFrame(env_private, dplyr::symbols::masks));
  SEXP chops = PROTECT(Rf_findVarInFrame(env_private, dplyr::symbols::chops));

  SEXP current_group = PROTECT(Rf_findVarInFrame(env_private, dplyr::symbols::current_group));
  int* p_current_group = INTEGER(current_group);

  SEXP current_expression = PROTECT(Rf_findVarInFrame(env_private, dplyr::symbols::current_expression));
  int* p_current_expression = INTEGER(current_expression);

  SEXP current_step = PROTECT(Rf_findVarInFrame(env_private, dplyr::symbols::current_step));
  int* p_current_step = INTEGER(current_step);

  // initialise results
  SEXP results = PROTECT(Rf_allocVector(VECSXP, n_expr));
  Rf_namesgets(results, names);

  bool all_one = true;
  bool need_splice = false;
  SEXP sizes = PROTECT(Rf_allocVector(INTSXP, n_groups));
  int* p_sizes = INTEGER(sizes);
  for (R_xlen_t i = 0; i < n_groups; i++) {
    p_sizes[i] = 1;
  }

  for (R_xlen_t i_expr = 0; i_expr < n_expr; i_expr++) {
    // --------- apply
    *p_current_expression = i_expr + 1;
    *p_current_step = 1;
    SEXP quo = VECTOR_ELT(quosures, i_expr);
    SEXP name = STRING_ELT(names, i_expr);
    SEXP auto_name = STRING_ELT(auto_names, i_expr);

    // try hybrid first
    *p_current_group = -1;
    SEXP result;
    SET_VECTOR_ELT(results, i_expr, result = funs::eval_hybrid(quo, chops));

    // if that does not work, loop around
    if (result == R_NilValue){
      SET_VECTOR_ELT(results, i_expr, result = Rf_allocVector(VECSXP, n_groups));

      for (R_xlen_t i_group = 0; i_group < n_groups; i_group++) {
        *p_current_group = i_group + 1;
        SET_VECTOR_ELT(result, i_group,
          rlang::eval_tidy(quo, VECTOR_ELT(masks, i_group), caller)
        );
      }
    }

    // deal with sizes
    *p_current_step = 2;
    for (R_xlen_t i_group = 0; i_group < n_groups; i_group++) {
      R_xlen_t size_i = XLENGTH(VECTOR_ELT(result, i_group));
      R_xlen_t previous_size_i = p_sizes[i_group];

      if (size_i == 1) {
        if (previous_size_i != 1) {
          // recycle
        }
      } else {
        all_one = false;

        if (previous_size_i == 1) {
          // update sizes
          p_sizes[i_group] = size_i;
        } else if (previous_size_i == size_i) {
          // all good
        } else {
          Rf_error("incompatible sizes");
        }
      }

    }

    // -------- install chops, slices in each mask
    *p_current_step = 3;
    if (XLENGTH(name) > 0) {
      SEXP symb_name = Rf_installChar(name);

      for (R_xlen_t i_group = 0; i_group < n_groups; i_group++) {
        Rf_defineVar(symb_name, VECTOR_ELT(result, i_group), ENCLOS(VECTOR_ELT(masks, i_group)));
      }
      Rf_defineVar(symb_name, result, chops);
    } else {
      SEXP ptype = PROTECT(list_ptype_common(result));

      if (Rf_inherits(ptype, "data.frame")){
        // auto splice
        need_splice = true;
        R_xlen_t n_columns = XLENGTH(ptype);
        SEXP names_result = Rf_getAttrib(ptype, R_NamesSymbol);
        for (R_xlen_t i_col = 0; i_col < n_columns; i_col++) {
          SEXP symb_name = Rf_installChar(STRING_ELT(names_result, i_col));

          SEXP chop = PROTECT(Rf_allocVector(VECSXP, n_groups));
          for (R_xlen_t i_group = 0; i_group < n_groups; i_group++) {
            SEXP slice = VECTOR_ELT(VECTOR_ELT(result, i_group), i_col);
            Rf_defineVar(symb_name, slice, ENCLOS(VECTOR_ELT(masks, i_group)));
            SET_VECTOR_ELT(chop, i_group, slice);
          }
          Rf_defineVar(symb_name, chop, chops);
          UNPROTECT(1);
        }

      } else {
        // use auto name
        SET_STRING_ELT(names, i_expr, auto_name);

        SEXP symb_name = Rf_installChar(auto_name);

        for (R_xlen_t i_group = 0; i_group < n_groups; i_group++) {
          Rf_defineVar(symb_name, VECTOR_ELT(result, i_group), ENCLOS(VECTOR_ELT(masks, i_group)));
        }
        Rf_defineVar(symb_name, result, chops);
      }

      UNPROTECT(1);
    }
  }

  // recycle maybe
  if (!all_one) {
    for (R_xlen_t i_expr = 0; i_expr < n_expr; i_expr++) {
      SEXP results_i = VECTOR_ELT(results, i_expr);
      for (R_xlen_t i_group = 0; i_group < n_groups; i_group++) {
        SET_VECTOR_ELT(results_i,
                       i_group,
                       vctrs::short_vec_recycle(VECTOR_ELT(results_i, i_group), p_sizes[i_group])
        );
      }
    }
  }

  // TODO: lump and splice, aka df_list() all results at once

  // structure results
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 3));
  SET_VECTOR_ELT(out, 0, results);
  SET_VECTOR_ELT(out, 1, Rf_ScalarLogical(all_one));
  SET_VECTOR_ELT(out, 2, sizes);
  SEXP out_names = PROTECT(Rf_allocVector(STRSXP, 3));
  SET_STRING_ELT(out_names, 0, Rf_mkChar("new"));
  SET_STRING_ELT(out_names, 1, Rf_mkChar("all_one"));
  SET_STRING_ELT(out_names, 2, Rf_mkChar("sizes"));
  Rf_namesgets(out, out_names);

  UNPROTECT(12);

  return out;
}


bool is_useful_chunk(SEXP ptype) {
  return !Rf_inherits(ptype, "data.frame") || XLENGTH(ptype) > 0;
}

SEXP dplyr_summarise_recycle_chunks(SEXP chunks, SEXP rows, SEXP ptypes) {
  R_len_t n_chunks = LENGTH(chunks);
  R_len_t n_groups = LENGTH(rows);

  SEXP res = PROTECT(Rf_allocVector(VECSXP, 2));
  Rf_namesgets(res, dplyr::vectors::names_summarise_recycle_chunks);
  SET_VECTOR_ELT(res, 0, chunks);

  SEXP useful = PROTECT(Rf_allocVector(LGLSXP, n_chunks));
  int* p_useful = LOGICAL(useful);
  int n_useful = 0;
  for (R_len_t j = 0; j < n_chunks; j++) {
    n_useful += p_useful[j] = is_useful_chunk(VECTOR_ELT(ptypes, j));
  }

  // early exit if there are no useful chunks, this includes
  // when there are no chunks at all
  if (n_useful == 0) {
    SET_VECTOR_ELT(res, 1, Rf_ScalarInteger(1));
    UNPROTECT(2);
    return res;
  }

  bool all_one = true;
  int k = 1;
  SEXP sizes = PROTECT(Rf_allocVector(INTSXP, n_groups));
  int* p_sizes = INTEGER(sizes);
  for (R_xlen_t i = 0; i < n_groups; i++, ++p_sizes) {
    R_len_t n_i = 1;

    R_len_t j = 0;
    for (; j < n_chunks; j++) {
      // skip useless chunks before looking for chunk size
      for (; j < n_chunks && !p_useful[j]; j++);
      if (j == n_chunks) break;

      R_len_t n_i_j = vctrs::short_vec_size(VECTOR_ELT(VECTOR_ELT(chunks, j), i));

      if (n_i != n_i_j) {
        if (n_i == 1) {
          n_i = n_i_j;
        } else if (n_i_j != 1) {
          dplyr::stop_summarise_incompatible_size(i, j, n_i, n_i_j);
        }
      }
    }

    k = k + n_i;
    *p_sizes = n_i;
    if (n_i != 1) {
      all_one = false;
    }
  }

  if (all_one) {
    SET_VECTOR_ELT(res, 1, Rf_ScalarInteger(1));
  } else {
    // perform recycling
    for (int j = 0; j < n_chunks; j++){
      // skip useless chunks before recycling
      for (; j < n_chunks && !p_useful[j]; j++);
      if (j == n_chunks) break;

      SEXP chunks_j = VECTOR_ELT(chunks, j);
      int* p_sizes = INTEGER(sizes);
      for (int i = 0; i < n_groups; i++, ++p_sizes) {
        SET_VECTOR_ELT(chunks_j, i,
          vctrs::short_vec_recycle(VECTOR_ELT(chunks_j, i), *p_sizes)
        );
      }
    }
    SET_VECTOR_ELT(res, 0, chunks);
    SET_VECTOR_ELT(res, 1, sizes);
  }

  UNPROTECT(3);
  return res;
}
