-module(prophet).
-export([init/0,open/3,select/2,perform/2,close/1]).

-export([decode_oci_string/1]).

-onload(init/0).

-opaque oci_handle() :: binary().

-spec init() -> ok | {error, any()}.
init() ->
  erlang:load_nif("prophet",0).

-spec open(string(), string(), string()) -> {ok, oci_handle()} | {error, any()}.
open(DB, User, Pass) ->
  erlang:nif_error({error, not_loaded}).

-spec select(oci_handle(), string()) -> {ok, list()} | {error, any()}.
select(Connection, Query) ->
  erlang:nif_error({error, not_loaded}).

-spec perform(oci_handle(), string()) -> {ok, {affected, integer()}} | {error, any()}.
perform(Connection, Query) ->
  erlang:nif_error({error, not_loaded}).

-spec close(oci_handle()) -> ok | {error, any()}.
close(DB) ->
  erlang:nif_error({error, not_loaded}).

%% Internal

-spec decode_oci_string(binary()) -> binary() | {error, any()}.
decode_oci_string(Value) ->
  case size(Value) rem 4 of
    0 ->
      decode_oci_string_(Value, <<>>);
    _ ->
      {error, invalid_oci_string}
  end.

decode_oci_string_(<<>>, Acc) ->
  Acc;
decode_oci_string_(<<V:1/binary,_:3/binary,Rest/binary>>,Acc) ->
  decode_oci_string_(Rest, <<Acc/binary,V/binary>>).

-spec format_value(binary(), pos_integer()) -> {ok, any()} | {error, unknown_type} | {error, invalid_binary}.
format_value(Value, Type) ->
  Result = case Type of
    1 -> %% OCI_CDT_NUMERIC
		  list_to_integer(binary_to_list(Value));
    3 -> %% OCI_CDT_DATETIME
		  decode_oci_string(Value);
    4 -> %% OCI_CDT_TEXT
		  decode_oci_string(Value);
    5 -> %% OCI_CDT_LONG
		  list_to_integer(binary_to_list(Value));
    6 -> %% OCI_CDT_CURSOR
		  Value;
    7 -> %% OCI_CDT_LOB
		  Value;
    8 -> %% OCI_CDT_FILE
		  Value;
    9 -> %% OCI_CDT_TIMESTAMP
		  Value;
    10 -> %% OCI_CDT_INTERVAL
		  list_to_integer(binary_to_list(Value));
    11 -> %% OCI_CDT_RAW
		  Value;
    12 -> %% OCI_CDT_OBJECT
		  Value;
    13 -> %% OCI_CDT_COLLECTION
		  Value;
    14 -> %% OCI_CDT_REF
		  Value;
    _ -> {error, unknown_type}
  end.
