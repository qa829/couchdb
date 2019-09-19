% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_views_reduce).


-export([
    run_reduce/2,
    update_reduce_idx/6,
    read_reduce/6
]).


-include("couch_views.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("fabric/include/fabric2.hrl").

-define(LEVEL_FAN_POW, 4).

log_levels(Db, Sig, ViewId) ->
    #{
        db_prefix := DbPrefix
    } = Db,

    Levels = lists:seq(0, ?MAX_SKIP_LIST_LEVELS),
    ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),
    Opts = [{streaming_mode, want_all}],

    fabric2_fdb:transactional(Db, fun(#{tx := Tx} = TxDb) ->
        lists:foreach(fun (Level) ->
            {StartKey, EndKey} = erlfdb_tuple:range({Level},
                ReduceIdxPrefix),

            Acc0 = #{
                sig => Sig,
                view_id => ViewId,
                reduce_idx_prefix => ReduceIdxPrefix,
                next => key,
                key => undefined,
                rows => []
            },

            Fun = fun fold_fwd_cb/2,
            Acc = erlfdb:fold_range(Tx, StartKey, EndKey, Fun, Acc0, Opts),
            #{
                rows := Rows
            } = Acc,
            io:format("~n ~n LEVEL ~p rows ~p ~n", [Level, Rows]),
            {ok, Rows}
        end, Levels),
        {ok, []}
    end).


read_reduce(Db, Sig, ViewId, UserCallback, UserAcc0, Args) ->
    #{
        db_prefix := DbPrefix
    } = Db,

    Levels = lists:seq(0, ?MAX_SKIP_LIST_LEVELS),
    ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),
    fabric2_fdb:transactional(Db, fun(#{tx := Tx} = TxDb) ->
        log_levels(TxDb, Sig, ViewId),
%%        Levels = lists:seq(0, ?MAX_SKIP_LIST_LEVELS),


        Acc0 = #{
            sig => Sig,
            view_id => ViewId,
            user_acc => UserAcc0,
            args => Args,
            callback => UserCallback,
            reduce_idx_prefix => ReduceIdxPrefix,
            next => key,
            rows => []
        },


%%        Opts = [{limit, 2}, {streaming_mode, want_all}],
%%        EK = couch_views_encoding:encode(0, key),
%%        {StartKey, EndKey} = erlfdb_tuple:range({?MAX_SKIP_LIST_LEVELS, EK},
%%            ReduceIdxPrefix),
%%
%%        Fun = fun fold_fwd_cb/2,
%%        Acc = erlfdb:fold_range(Tx, StartKey, EndKey, Fun, Acc0, Opts),
        #{
            rows := Rows
        } = Acc0,
        {ok, Rows}
    end).

args_to_fdb_opts(#mrargs{} = Args) ->
    #mrargs{
        limit = Limit,
        start_key = StartKey,
        end_key = EndKey
    } = Args,
    ok.


fold_fwd_cb({FullEncodedKey, EV}, #{next := key} = Acc) ->
    #{
        reduce_idx_prefix := ReduceIdxPrefix
    } = Acc,

    {Level, EK, ?VIEW_ROW_KEY}
        = erlfdb_tuple:unpack(FullEncodedKey, ReduceIdxPrefix),

%%    Key = couch_views_encoding:decode(EV),
    Val = couch_views_encoding:decode(EV),
    Acc#{next := value, key := Val};

fold_fwd_cb({FullEncodedKey, EV}, #{next := value} = Acc) ->
    #{
        reduce_idx_prefix := ReduceIdxPrefix,
        rows := Rows,
        key := Key
    } = Acc,

    {Level, EK, ?VIEW_ROW_VALUE}
        = erlfdb_tuple:unpack(FullEncodedKey, ReduceIdxPrefix),

%%    Key = couch_views_encoding:decode(EV),
    Val = couch_views_encoding:decode(EV),
    Acc#{next := key, key := undefined, rows := Rows ++ [{Key, Val}]}.


run_reduce(#mrst{views = Views } = Mrst, MappedResults) ->
    ReduceFuns = lists:map(fun(View) ->
        #mrview{
            id_num = Id,
            reduce_funs = ViewReduceFuns
        } = View,

        [{_, Fun}] = ViewReduceFuns,
        Fun
    end, Views),

    lists:map(fun (MappedResult) ->
        #{
            results := Results
        } = MappedResult,

        ReduceResults = lists:map(fun ({ReduceFun, Result}) ->
            reduce(ReduceFun, Result)
        end, lists:zip(ReduceFuns, Results)),

        MappedResult#{
            reduce_results => ReduceResults
        }
    end, MappedResults).


reduce(<<"_count">>, Results) ->
    ReduceResults = lists:foldl(fun ({Key, _}, Acc) ->
        case maps:is_key(Key, Acc) of
            true ->
                #{Key := Val} = Acc,
                Acc#{Key := Val + 1};
            false ->
                Acc#{Key => 1}
        end
    end, #{}, Results),
    maps:to_list(ReduceResults);

% this isn't a real supported reduce function in CouchDB
% But I want a basic reduce function that when we need to update the index
% we would need to re-read multiple rows instead of being able to do an
% atomic update
reduce(<<"_stats">>, Results) ->
    ReduceResults = lists:foldl(fun ({Key, Val}, Acc) ->
        io:format("MAX ~p ~p ~n", [Key, Val]),
        case maps:is_key(Key, Acc) of
            true ->
                #{Key := Max} = Acc,
                case Max >= Val of
                    true ->
                        Acc;
                    false ->
                        Acc#{Key := Val}
                end;
            false ->
                Acc#{Key => Val}
        end
    end, #{}, Results),
    maps:to_list(ReduceResults).


is_builtin(<<"_", _/binary>>) ->
    true;

is_builtin(_) ->
    false.


update_reduce_idx(TxDb, Sig, ViewId, _DocId, _ExistingKeys, ReduceResult) ->
    #{
        db_prefix := DbPrefix
    } = TxDb,

    ViewOpts = #{
        db_prefix => DbPrefix,
        sig => Sig,
        view_id => ViewId
    },
    create_skip_list(TxDb, ?MAX_SKIP_LIST_LEVELS, ViewOpts),

    lists:foreach(fun ({Key, Val}) ->
        io:format("RESULTS KV ~p ~p ~n", [Key, Val]),
        add_kv_to_skip_list(TxDb, ?MAX_SKIP_LIST_LEVELS, ViewOpts, Key, Val)
    end, ReduceResult).


create_skip_list(Db, MaxLevel, #{} = ViewOpts) ->
    #{
        db_prefix := DbPrefix,
        sig := Sig,
        view_id := ViewId
    } = ViewOpts,

    Levels = lists:seq(0, MaxLevel),
    ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),

    fabric2_fdb:transactional(Db, fun(TxDb) ->

        lists:foreach(fun(Level) ->
            add_kv(TxDb, ReduceIdxPrefix, Level, 0, 0)
        end, Levels)
    end).

%% This sucks but its simple for now
should_add_key_to_level(0, _, _) ->
    true;

should_add_key_to_level(?MAX_SKIP_LIST_LEVELS, _, _) ->
    false;

should_add_key_to_level(_, _, false) ->
    false;

should_add_key_to_level(_, _Key, _Prev) ->
    crypto:rand_uniform(0, 2) == 0.

%%should_add_key_to_level(Level, Key) ->
%%    erlang:phash2(Key) band ((1 bsl (Level * ?LEVEL_FAN_POW)) -1) == 0.
%%    keyHash & ((1 << (level * LEVEL_FAN_POW)) - 1)) != 0


add_kv_to_skip_list(Db, MaxLevel, #{} = ViewOpts, Key, Val) ->
    #{
        db_prefix := DbPrefix,
        sig := Sig,
        view_id := ViewId
    } = ViewOpts,

    Levels = lists:seq(0, MaxLevel),
    ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),

    fabric2_fdb:transactional(Db, fun(TxDb) ->
        lists:foldl(fun(Level, PrevCoinFlip) ->
            io:format("PROCESS ~p ~p ~p ~n", [Level, Key, Val]),
            {PrevKey, PrevVal} = get_previous_key(TxDb, ReduceIdxPrefix, Level, Key),
            io:format("PREV VALS ~p ~p ~n", [PrevKey, PrevVal]),
            case should_add_key_to_level(Level, Key, PrevCoinFlip) of
                true ->
                    io:format("Adding ~p ~p ~n", [Level, Key]),
                    add_kv(Db, ReduceIdxPrefix, Level, Key, Val),
                    true;
                false ->
                    {PrevKey, NewVal} = rereduce(<<"_stats">>, {PrevKey, PrevVal}, {Key, Val}),
                    io:format("RE_REDUCE ~p ~p ~p ~p ~n", [Level, Key, PrevKey, NewVal]),
                    add_kv(Db, ReduceIdxPrefix, Level, PrevKey, NewVal),
                    false
            end
        end, true, Levels)
    end).


rereduce(<<"_stats">>, {PrevKey, PrevVal}, {_Key, Val}) ->
    case PrevVal >= Val of
        true -> {PrevKey, PrevVal};
        false -> {PrevKey, Val}
    end.


reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId) ->
    Key = {?DB_VIEWS, Sig, ?VIEW_REDUCE_SK_RANGE, ViewId},
    erlfdb_tuple:pack(Key, DbPrefix).


reduce_idx_key(ReduceIdxPrefix, SkipLevel, ReduceKey, RowType) ->
    Key = {SkipLevel, ReduceKey, RowType},
    erlfdb_tuple:pack(Key, ReduceIdxPrefix).


add_kv(TxDb, ReduceIdxPrefix, Level, Key, Val) ->
    #{
        tx := Tx
    } = TxDb,

    EK = couch_views_encoding:encode(Key, key),
    EVK = couch_views_encoding:encode(Key),
    EV = couch_views_encoding:encode(Val),

    KK = reduce_idx_key(ReduceIdxPrefix, Level, EK, ?VIEW_ROW_KEY),
    VK = reduce_idx_key(ReduceIdxPrefix, Level, EK, ?VIEW_ROW_VALUE),
    ok = erlfdb:set(Tx, KK, EVK),
    ok = erlfdb:set(Tx, VK, EV).


get_previous_key(TxDb, ReduceIdxPrefix, Level, Key) ->
    #{
        tx := Tx
    } = TxDb,

    % TODO: see if we need to add in conflict ranges for this for level=0
    Opts = [{limit, 2}, {reverse, true}, {streaming_mode, want_all}],
%%    LevelPrefix = erlfdb_tuple:pack({Level}, ReduceIdxPrefix),

    EK = couch_views_encoding:encode(Key, key),
    EndKey0 = erlfdb_tuple:pack({Level, EK}, ReduceIdxPrefix),

    {StartKey, EndKey1} = erlfdb_tuple:range({Level}, ReduceIdxPrefix),
%%    EndKey1 = erlfdb_key:first_greater_than(EndKey0),

    Callback = fun row_cb/2,
    Out = erlfdb:fold_range(Tx, StartKey, EndKey1, Callback, {val, ReduceIdxPrefix, {}}, Opts),
    io:format("OUT PRV ~p ~p ~p ~n", [Level, Key, Out]),
    Out.


row_cb({FullEncodedKey, EV}, {val, ReduceIdxPrefix, Acc}) ->
    io:format("ROW VAL ~p ~n", [erlfdb_tuple:unpack(FullEncodedKey, ReduceIdxPrefix)]),
    {_Level, EK, _VIEW_ROW_VALUE}
        = erlfdb_tuple:unpack(FullEncodedKey, ReduceIdxPrefix),
    Val = couch_views_encoding:decode(EV),
%%    io:format("WW ~p ~p ~n", [couch_views_encoding:decode(EK), Val]),

    {key, {EK, ReduceIdxPrefix, Val}};

row_cb({FullEncodedKey, EVK}, {key, {EK, ReduceIdxPrefix, Val}}) ->
    io:format("ROW KEY ~p ~n", [erlfdb_tuple:unpack(FullEncodedKey, ReduceIdxPrefix)]),
    {_Level, EK, ?VIEW_ROW_KEY}
        = erlfdb_tuple:unpack(FullEncodedKey, ReduceIdxPrefix),
    Key = couch_views_encoding:decode(EVK),

    {Key, Val}.




