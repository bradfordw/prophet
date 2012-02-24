#include <ocilib.h>
#include "erl_nif.h"

#define BUFF_LEN 4096

struct prophet_oci {
  OCI_Connection *cn;
};

typedef struct prophet_oci prophet_oci;
static ErlNifResourceType* prophet_rsrc;
// configuration atoms (used when casting)
static ERL_NIF_TERM ATOM_STRING;
static ERL_NIF_TERM ATOM_DOUBLE;
static ERL_NIF_TERM ATOM_INTEGER;
static ERL_NIF_TERM ATOM_UINTEGER;

void bind_statement(ErlNifEnv *env, ERL_NIF_TERM bindings, OCI_Statement *st) {
	ERL_NIF_TERM head, tail, list = bindings;
	const ERL_NIF_TERM* tuple;
	char bind_type[BUFF_LEN];
	char bind_label[BUFF_LEN], string_value[BUFF_LEN];
  double double_value;
	unsigned int uint_value;
	int int_value;
	int tup_arity = 3;

	while(enif_get_list_cell(env, list, &head, &tail)) {
		if (!enif_get_tuple(env, head, &tup_arity, &tuple)) {
			break;
		}
		if (!enif_get_atom(env, tuple[2], bind_type, sizeof(bind_type), ERL_NIF_LATIN1)) {
			break;
		}
		if (!enif_get_string(env, tuple[0], bind_label, sizeof(bind_label), ERL_NIF_LATIN1)) {
			break;
		}
		switch (enif_make_atom(env, bind_type)) {
			ATOM_STRING:
				if (enif_get_string(env, tuple[1], string_value, sizeof(string_value), ERL_NIF_LATIN1)) {
					OCI_BindString(st, MT(bind_label), (char *) string_value, sizeof(string_value));
				}
			break;
			ATOM_DOUBLE:
				if (enif_get_double(env, tuple[1], &double_value)) {
					OCI_BindDouble(st, MT(bind_label), &double_value);
				}
			break;
			ATOM_INTEGER:
				if (enif_get_int(env, tuple[1], &int_value)) {
					OCI_BindInt(st, MT(bind_label), &int_value);
				}
			break;
			ATOM_UINTEGER:
				if (enif_get_uint(env, tuple[1], &uint_value)) {
					OCI_BindUnsignedInt(st, MT(bind_label), &uint_value);
				}
			break;
			default: // string
				if (enif_get_string(env, tuple[1], string_value, sizeof(string_value), ERL_NIF_LATIN1)) {
					OCI_BindString(st, MT(bind_label), (char *) string_value, sizeof(string_value));
				}
			break;
		}
		list = tail;
	}
}

static ERL_NIF_TERM prophet_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {

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

static ERL_NIF_TERM prophet_perform(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  prophet_oci* prophet_oci_handle;
	int i, cols, row_count, value_len, col_prec, col_scale = 0;
	ERL_NIF_TERM result, bind_vars, columns, rows, row;
	char query[BUFF_LEN];
	const wchar_t *oci_value;
	ErlNifBinary value;
  OCI_Statement *st;
	OCI_Resultset *rs;
  if (!enif_get_resource(env, argv[0], prophet_rsrc, (void**)&prophet_oci_handle)) {
    return enif_make_badarg(env);
  }
	if (!enif_get_string(env, argv[1], query, sizeof(query), ERL_NIF_LATIN1)) {
		return enif_make_badarg(env);
	}

	st = OCI_StatementCreate(prophet_oci_handle->cn);
	if (!enif_is_empty_list(env, argv[2])) {
		bind_statement(env, argv[2], st);
	}

	OCI_ExecuteStmt(st, query);
	// what kind of statement are we dealing with here
	switch (OCI_GetStatementType(st)) {
		case OCI_CST_SELECT:
		  rs = OCI_GetResultset(st);

		  result = enif_make_list(env, 0);
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

			result = enif_make_list_cell(env, 
				enif_make_tuple2(env, enif_make_atom(env, "columns"), columns), result);
			rows = enif_make_list(env, 0);

			while (OCI_FetchNext(rs)) {
				row = enif_make_list(env, 0);
				for (i = 1; i <= cols; i++) {
					OCI_Column *col = OCI_GetColumn(rs, i);
					// infer type [needs to be moved to separate function]
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

			result = enif_make_list_cell(env, 
				enif_make_tuple2(env, 
					enif_make_atom(env, "data"), rows), result);
			result = enif_make_list_cell(env, 
				enif_make_tuple2(env, 
					enif_make_atom(env, "rows"), 
					enif_make_int(env, row_count)), result);
		break;
		default:
			OCI_Commit(prophet_oci_handle->cn);
			result = enif_make_tuple2(env, enif_make_atom(env, "affected"), enif_make_int(env, OCI_GetAffectedRows(st)));
		break;
	}
	//cleanup and return
	OCI_FreeStatement(st);
  return enif_make_tuple2(env, enif_make_atom(env, "ok"), result);
}

static void unload(ErlNifEnv* env, void* arg) {}

static int load_init(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
	ATOM_STRING = enif_make_atom(env, "string");
	ATOM_DOUBLE = enif_make_atom(env, "double");
	ATOM_INTEGER = enif_make_atom(env, "integer");
	ATOM_UINTEGER = enif_make_atom(env, "unsigned_integer");
  prophet_rsrc = enif_open_resource_type(env, "prophet", "prophet_rsrc", &unload, ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, 0);
  return 0;
}

static ErlNifFunc nif_funcs[] = {
    {"open", 3, prophet_open},
    {"close", 1, prophet_close},
    {"perform", 3, prophet_perform}
};

ERL_NIF_INIT(prophet, nif_funcs, &load_init, NULL, NULL, NULL)