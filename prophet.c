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
  ERL_NIF_TERM results;
	ERL_NIF_TERM columns;
  ERL_NIF_TERM rows;
	ERL_NIF_TERM row;
	ErlNifBinary value;
	const wchar_t *row_value;
	const wchar_t *col_value;
	char query[BUFF_LEN];
	int value_len;
	
  if (!enif_get_resource(env, argv[0], prophet_rsrc, (void**)&prophet_oci_handle)) {
    return enif_make_badarg(env);
  }
  
  OCI_Statement *st;
  OCI_Resultset *rs;

  enif_get_string(env, argv[1], query, BUFF_LEN, ERL_NIF_LATIN1);
  st = OCI_StatementCreate(prophet_oci_handle->cn);

  OCI_ExecuteStmt(st, query);
  rs = OCI_GetResultset(st);
	int i, cols, row_count;
  results = enif_make_list(env, 0);
	cols = OCI_GetColumnCount(rs);
	columns = enif_make_list(env, 0);
	for (i = 1; i <= cols; i++) {
		OCI_Column *col = OCI_GetColumn(rs, i);
		col_value = (wchar_t *) OCI_GetColumnName(col);
		value_len = wcslen(col_value) * sizeof(wchar_t);
		enif_alloc_binary(value_len, &value);
		memcpy(value.data, col_value, value_len);

		columns = enif_make_list_cell(env, 
			enif_make_tuple2(env, 
				enif_make_binary(env, &value), 
				enif_make_int(env, OCI_ColumnGetType(col))), columns);
	}

	results = enif_make_list_cell(env, 
		enif_make_tuple2(env, enif_make_atom(env, "columns"), columns), results);
	rows = enif_make_list(env, 0);
	row_count = 0;
	while (OCI_FetchNext(rs)) {
		row = enif_make_list(env, 0);
		for (i = 1; i <= cols; i++) {
			OCI_Column *col = OCI_GetColumn(rs, i);
			row_value = (wchar_t *) OCI_GetString(rs, i);
			value_len = wcslen(row_value) * sizeof(wchar_t);
			enif_alloc_binary(value_len, &value);
			memcpy(value.data, row_value, value_len);

			row = enif_make_list_cell(env, enif_make_binary(env, &value), row);
		}
		rows = enif_make_list_cell(env, row, rows);
		row_count++;
	}
	
	results = enif_make_list_cell(env, 
		enif_make_tuple2(env, 
			enif_make_atom(env, "data"), rows), results);
	results = enif_make_list_cell(env, 
		enif_make_tuple2(env, 
			enif_make_atom(env, "num_rows"), 
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
    {"perform", 2, prophet_perform}
};

ERL_NIF_INIT(prophet, nif_funcs, &load_init, NULL, NULL, NULL)