#include <ocilib.h>
#include "erl_nif.h"

#define BUFF_LEN 1024
#define COL_LEN 256
struct prophet_oci {
  OCI_Connection *cn;
};

typedef struct prophet_oci prophet_oci;
static ErlNifResourceType* prophet_rsrc;
  
static ERL_NIF_TERM prophet_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {

  if (!OCI_Initialize(NULL, NULL, OCI_ENV_DEFAULT)) {
    return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_atom(env, "could_not_initialize"));
  }
  
  char db[BUFF_LEN];
  char user[BUFF_LEN];
  char pass[BUFF_LEN];
  
  enif_get_string(env, argv[0], db, BUFF_LEN, ERL_NIF_LATIN1);
  enif_get_string(env, argv[1], user, BUFF_LEN, ERL_NIF_LATIN1);
  enif_get_string(env, argv[2], pass, BUFF_LEN, ERL_NIF_LATIN1);

  prophet_oci* prophet_oci_handle = enif_alloc_resource(prophet_rsrc, sizeof(prophet_oci));
  prophet_oci_handle->cn = OCI_ConnectionCreate(db, user, pass, OCI_SESSION_DEFAULT);
  
  ERL_NIF_TERM result = enif_make_resource(env, prophet_oci_handle);
  enif_release_resource(prophet_oci_handle);
  
  return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
}

static ERL_NIF_TERM prophet_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  prophet_oci* prophet_oci_handle;
  ERL_NIF_TERM result;
  
  if (!enif_get_resource(env, argv[0], prophet_rsrc, (void**)&prophet_oci_handle)) {
    return enif_make_badarg(env);
  }
  
  if (prophet_oci_handle->cn != NULL) {
    OCI_ConnectionFree(prophet_oci_handle->cn);
  }
  
  OCI_Cleanup();
  return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM prophet_select(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  prophet_oci* prophet_oci_handle;
	const wchar_t *oci_value;
	char query[BUFF_LEN];
	int i, cols, row_count, value_len = 0;
	int col_prec, col_scale = 0;
  ERL_NIF_TERM results;
	ERL_NIF_TERM columns;
  ERL_NIF_TERM rows;
	ERL_NIF_TERM row;
	ErlNifBinary value;
  OCI_Statement *st;
  OCI_Resultset *rs;

  if (!enif_get_resource(env, argv[0], prophet_rsrc, (void**)&prophet_oci_handle)) {
    return enif_make_badarg(env);
  }

  enif_get_string(env, argv[1], query, BUFF_LEN, ERL_NIF_LATIN1);
  st = OCI_StatementCreate(prophet_oci_handle->cn);

  OCI_ExecuteStmt(st, query);
  rs = OCI_GetResultset(st);

  results = enif_make_list(env, 0);
	cols = OCI_GetColumnCount(rs);
	columns = enif_make_list(env, 0);

	for (i = 1; i <= cols; i++) {
		OCI_Column *col = OCI_GetColumn(rs, i);
		oci_value = (wchar_t *) OCI_GetColumnName(col);
		value_len = wcslen(oci_value) * sizeof(wchar_t);
		enif_alloc_binary(value_len, &value);
		memcpy(value.data, oci_value, value_len);

		columns = enif_make_list_cell(env, enif_make_binary(env, &value), columns);
	}

	results = enif_make_list_cell(env, 
		enif_make_tuple2(env, enif_make_atom(env, "columns"), columns), results);
	rows = enif_make_list(env, 0);

	while (OCI_FetchNext(rs)) {
		row = enif_make_list(env, 0);
		for (i = 1; i <= cols; i++) {
			OCI_Column *col = OCI_GetColumn(rs, i);
			// infer type
			switch (OCI_ColumnGetType(col)) {
				case OCI_CDT_NUMERIC:
				col_scale = OCI_ColumnGetScale(col);
				col_prec = OCI_ColumnGetPrecision(col);
					if (col_prec == 0 && col_scale == 0) {
						row = enif_make_list_cell(env, enif_make_double(env, OCI_GetDouble(rs, i)), row);
					} else if (col_prec > 0 && col_scale > 0) {
						row = enif_make_list_cell(env, enif_make_double(env, OCI_GetDouble(rs, i)), row);
					} else if (col_prec > 0 && col_scale == 0) {
						row = enif_make_list_cell(env, enif_make_int(env, OCI_GetInt(rs, i)), row);
					} else if (col_prec >= 0 && col_scale < 0) {
						row = enif_make_list_cell(env, enif_make_double(env, OCI_GetDouble(rs, i)), row);
					} else {
						oci_value = (wchar_t *) OCI_GetString(rs, i);
						value_len = wcslen(oci_value) * sizeof(wchar_t);
						enif_alloc_binary(value_len, &value);
						memcpy(value.data, oci_value, value_len);
						row = enif_make_list_cell(env, enif_make_binary(env, &value), row);
					}
					break;
				default:
					oci_value = (wchar_t *) OCI_GetString(rs, i);
					value_len = wcslen(oci_value) * sizeof(wchar_t);
					enif_alloc_binary(value_len, &value);
					memcpy(value.data, oci_value, value_len);
					row = enif_make_list_cell(env, enif_make_binary(env, &value), row);
					break;
			}
		}
		rows = enif_make_list_cell(env, row, rows);
		row_count++;
	}
	
	results = enif_make_list_cell(env, 
		enif_make_tuple2(env, 
			enif_make_atom(env, "data"), rows), results);
	results = enif_make_list_cell(env, 
		enif_make_tuple2(env, 
			enif_make_atom(env, "rows"), 
			enif_make_int(env, row_count)), results);

	OCI_FreeStatement(st);
  return enif_make_tuple2(env, enif_make_atom(env, "ok"), results);
}

static ERL_NIF_TERM prophet_execute(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  prophet_oci* prophet_oci_handle;
  OCI_Statement *st;
  ERL_NIF_TERM result;
 	char query[BUFF_LEN];

  if (!enif_get_resource(env, argv[0], prophet_rsrc, (void**)&prophet_oci_handle)) {
    return enif_make_badarg(env);
  }

	if (!enif_get_string(env, argv[1], query, BUFF_LEN, ERL_NIF_LATIN1)) {
		return enif_make_badarg(env);
	}

	st = OCI_StatementCreate(prophet_oci_handle->cn);

	OCI_ExecuteStmt(st, "delete from test_fetch where code > 1");
	OCI_Commit(prophet_oci_handle->cn);

	result = enif_make_tuple2(env, enif_make_atom(env, "affected"), enif_make_int(env, OCI_GetAffectedRows(st)));
  return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
}

static void unload(ErlNifEnv* env, void* arg) {}

static int load_init(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
  prophet_rsrc = enif_open_resource_type(env, "prophet", "prophet_rsrc", &unload, ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, 0);
  return 0;
}

static ErlNifFunc nif_funcs[] = {
    {"open", 3, prophet_open},
    {"close", 1, prophet_close},
    {"select", 2, prophet_select},
    {"execute", 2, prophet_execute}
};

ERL_NIF_INIT(prophet, nif_funcs, &load_init, NULL, NULL, NULL)