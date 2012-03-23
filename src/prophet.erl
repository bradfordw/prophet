-module(prophet).
-export([init/0,open/3,perform/3,perform/2,close/1,ping/1,test/1]).

-export([decode_oci_string/1]).

-onload(init/0).

-opaque oci_handle() :: binary().

-spec init() -> ok | {error, any()}.
init() ->
  erlang:load_nif("prophet",0).

-spec open(string(), string(), string()) -> {ok, oci_handle()} | {error, any()}.
open(_DB, _User, _Pass) ->
  erlang:nif_error({error, not_loaded}).

-spec perform(oci_handle(), string()) -> {ok, list()}|{ok, {affected, integer()}} | {error, any()}.
perform(Connection, Query) ->
  perform(Connection, Query, []).
-spec perform(oci_handle(), string(), list()) -> {ok, list()}|{ok, {affected, integer()}} | {error, any()}.
perform(_Connection, _Query, _BindVars) ->
  erlang:nif_error({error, not_loaded}).

-spec close(oci_handle()) -> ok | {error, any()}.
close(_Connection) ->
  erlang:nif_error({error, not_loaded}).

-spec ping(oci_handle()) -> pong | pang.
ping(_Connection) ->
  erlang:nif_error({error, not_loaded}).

test(_Input) ->
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