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

-module(couch_views_fdb).

-export([
    get_update_seq/2,
    set_update_seq/3,

    get_row_count/3,
    get_kv_size/3,

    fold_map_idx/6,
    fold_reduce_idx/6,

    write_doc/4
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(LIST_VALUE, 0).
-define(JSON_VALUE, 1).
-define(VALUE, 2).


-include("couch_views.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("fabric/include/fabric2.hrl").


% View Build Sequence Access
% (<db>, ?DB_VIEWS, Sig, ?VIEW_UPDATE_SEQ) = Sequence

% Id Range
% {<db>, ?DB_VIEWS, Sig, ?VIEW_ID_RANGE, DocId, ViewId}
%   = [TotalKeys, TotalSize, UniqueKeys]

% Map Range
%{<db>, ?DB_VIEWS, Sig, ?VIEW_MAP_RANGE, ViewId, {{Key, DocId}, DupeId, Type}}
%   = Value | UnEncodedKey
% Type = ?VIEW_KEY | ?VIEW_ROW

% Reduce Range
%{<db> ?DB_VIEWS, Sig, ?VIEW_REDUCE_RANGE, ViewId, Key, GroupLevel,
%   ReduceType, RowType} = Value | UnEncodedKey
% ReduceType = VIEW_REDUCE_EXACT | ?VIEW_REDUCE_GROUP
% RowType = ?VIEW_KEY | ?VIEW_ROW


get_update_seq(TxDb, #mrst{sig = Sig}) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,

    case erlfdb:wait(erlfdb:get(Tx, seq_key(DbPrefix, Sig))) of
        not_found -> <<>>;
        UpdateSeq -> UpdateSeq
    end.


set_update_seq(TxDb, Sig, Seq) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,
    ok = erlfdb:set(Tx, seq_key(DbPrefix, Sig), Seq).


get_row_count(TxDb, #mrst{sig = Sig}, ViewId) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,

    case erlfdb:wait(erlfdb:get(Tx, row_count_key(DbPrefix, Sig, ViewId))) of
        not_found -> 0; % Can this happen?
        CountBin -> ?bin2uint(CountBin)
    end.


get_kv_size(TxDb, #mrst{sig = Sig}, ViewId) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,

    case erlfdb:wait(erlfdb:get(Tx, kv_size_key(DbPrefix, Sig, ViewId))) of
        not_found -> 0; % Can this happen?
        SizeBin -> ?bin2uint(SizeBin)
    end.


fold_map_idx(TxDb, Sig, ViewId, Options, Callback, Acc0) ->
    #{
        db_prefix := DbPrefix
    } = TxDb,

    MapIdxPrefix = map_idx_prefix(DbPrefix, Sig, ViewId),
    FoldAcc = #{
        prefix => MapIdxPrefix,
        sort_key => undefined,
        docid => undefined,
        dupe_id => undefined,
        callback => Callback,
        acc => Acc0
    },

    {Fun, Acc} = case fabric2_util:get_value(dir, Options, fwd) of
        fwd ->
            FwdAcc = FoldAcc#{
                next => key,
                key => undefined
            },
            {fun fold_fwd/2, FwdAcc};
        rev ->
            RevAcc = FoldAcc#{
                next => value,
                value => undefined
            },
            {fun fold_rev/2, RevAcc}
    end,

    #{
        acc := Acc1
    } = fabric2_fdb:fold_range(TxDb, MapIdxPrefix, Fun, Acc, Options),

    Acc1.


fold_reduce_idx(TxDb, Sig, ViewId, Options, Callback, Acc0) ->
    #{
        db_prefix := DbPrefix
    } = TxDb,

    #{
        mrargs := MrArgs
    } = Acc0,

    #mrargs{
        group_level = GroupLevel,
        group = Group
    } = MrArgs,

    ReduceIdxPrefix = reduce_idx_prefix(DbPrefix, Sig, ViewId),

    Group1 = case {GroupLevel, Group} of
        {0, true} -> exact;
        _ -> grouping
    end,

    FoldAcc = #{
        prefix => ReduceIdxPrefix,
        sort_key => undefined,
        docid => undefined,
        callback => Callback,
        acc => Acc0,
        group_level => GroupLevel,
        group => Group1,
        prev_group_key => undefined,
        reduce_type => undefined,

        next => key,
        key => undefined
    },

    Fun = fun reduce_fold_fwd/2,

    #{
        acc := Acc1
    } = fabric2_fdb:fold_range(TxDb, ReduceIdxPrefix, Fun, FoldAcc, Options),
    Acc1.


reduce_fold_fwd({RowKey, EncodedOriginalKey}, #{next := key} = Acc) ->
    #{
        prefix := Prefix
    } = Acc,

    {SortKey, _RowGroupLevel, ReduceType, ?VIEW_ROW_KEY} =
        erlfdb_tuple:unpack(RowKey, Prefix),
    Acc#{
        next := value,
        key := couch_views_encoding:decode(EncodedOriginalKey),
        sort_key := SortKey,
        reduce_type := ReduceType
    };

reduce_fold_fwd({RowKey, EncodedValue}, #{next := value} = Acc) ->
    #{
        prefix := Prefix,
        key := Key,
        sort_key := SortKey,
        group_level := GroupLevel,
        group := Group,
        reduce_type := ReduceType,
        callback := UserCallback,
        acc := UserAcc0,
        prev_group_key := PrevGroupKey
    } = Acc,


    % We're asserting there that this row is paired
    % correctly with the previous row by relying on
    % a badmatch if any of these values don't match.
    {SortKey, RowGroupLevel, ReduceType, ?VIEW_ROW_VALUE} =
        erlfdb_tuple:unpack(RowKey, Prefix),

    % TODO: Handle more than uint
    Value = ?bin2uint(EncodedValue),
    io:format("FWD VAL ~p ~p ~p ~p ~n", [Key, RowGroupLevel, Value, ReduceType]),
    io:format("GROUP SETTINGS ~p ~p ~n", [Group, GroupLevel]),
    UserAcc1 = case should_return_row(PrevGroupKey, Key, Group, GroupLevel, RowGroupLevel, ReduceType) of
        true ->
            UserCallback(Key, Value, UserAcc0);
        false ->
            UserAcc0
    end,

    PrevGroupKey1 = maybe_update_prev_group_key(PrevGroupKey, Key, ReduceType),

    Acc#{
        next := key,
        key := undefined,
        sort_key := undefined,
        reduce_type := undefined,
        acc := UserAcc1,
        prev_group_key := PrevGroupKey1
    }.


should_return_row(_PrevGroupKey, _CurrentKey, exact, _GroupLevel,
    _RowGroupLevel, ?VIEW_REDUCE_GROUP) ->
    false;

should_return_row(_PrevGroupKey, _CurrentKey, exact, _GroupLevel,
    _RowGroupLevel, ?VIEW_REDUCE_EXACT) ->
    true;

should_return_row(_PrevGroupKey, _CurrentKey, _Group, GroupLevel,
    RowGroupLevel, ?VIEW_REDUCE_EXACT) when RowGroupLevel < GroupLevel ->
    true;

should_return_row(PrevGroupKey, PrevGroupKey, _Group, GroupLevel, GroupLevel,
    ?VIEW_REDUCE_EXACT) ->
    false;

should_return_row(_PrevExactKey, _CurrentKey, _Group, GroupLevel, GroupLevel,
    ?VIEW_REDUCE_GROUP) ->
    true;

should_return_row(_, _, _, _, _, _) ->
    false.


maybe_update_prev_group_key(_PrevGroupKey, NewKey, ?VIEW_REDUCE_GROUP) ->
    NewKey;

maybe_update_prev_group_key(PrevGroupKey, _NewKey, ?VIEW_REDUCE_EXACT) ->
    PrevGroupKey.


write_doc(TxDb, Sig, _ViewIds, #{deleted := true} = Doc) ->
    #{
        id := DocId
    } = Doc,

    ExistingViewKeys = get_view_keys(TxDb, Sig, DocId),

    clear_id_idx(TxDb, Sig, DocId),
    lists:foreach(fun({ViewId, TotalKeys, TotalSize, UniqueKeys}) ->
        clear_map_idx(TxDb, Sig, ViewId, DocId, UniqueKeys),
        %clear_reduce_idx
        update_row_count(TxDb, Sig, ViewId, -TotalKeys),
        update_kv_size(TxDb, Sig, ViewId, -TotalSize)
    end, ExistingViewKeys);

write_doc(TxDb, Sig, ViewIds, Doc) ->
    #{
        id := DocId,
        results := Results,
        reduce_results := ReduceResults
    } = Doc,

    ExistingViewKeys = get_view_keys(TxDb, Sig, DocId),

    clear_id_idx(TxDb, Sig, DocId),

    %% TODO: handle when there is no reduce
    lists:foreach(fun({ViewId, NewRows, ReduceResult}) ->
        update_id_idx(TxDb, Sig, ViewId, DocId, NewRows),

        ExistingKeys = case lists:keyfind(ViewId, 1, ExistingViewKeys) of
            {ViewId, TotalRows, TotalSize, EKeys} ->
                RowChange = length(NewRows) - TotalRows,
                SizeChange = calculate_row_size(NewRows) - TotalSize,
                update_row_count(TxDb, Sig, ViewId, RowChange),
                update_kv_size(TxDb, Sig, ViewId, SizeChange),
                EKeys;
            false ->
                RowChange = length(NewRows),
                SizeChange = calculate_row_size(NewRows),
                update_row_count(TxDb, Sig, ViewId, RowChange),
                update_kv_size(TxDb, Sig, ViewId, SizeChange),
                []
        end,
        update_map_idx(TxDb, Sig, ViewId, DocId, ExistingKeys, NewRows),
        update_reduce_idx(TxDb, Sig, ViewId, DocId, ExistingKeys, ReduceResult)
    end, lists:zip3(ViewIds, Results, ReduceResults)).


% For each row in a map view there are two rows stored in
% FoundationDB:
%
%   `(EncodedSortKey, EncodedKey)`
%   `(EncodedSortKey, EncodedValue)`
%
% The difference between `EncodedSortKey` and `EndcodedKey` is
% the use of `couch_util:get_sort_key/1` which turns UTF-8
% strings into binaries that are byte comparable. Given a sort
% key binary we cannot recover the input so to return unmodified
% user data we are forced to store the original.
%
% These two fold functions exist so that we can be fairly
% forceful on our assertions about which rows to see. Since
% when we're folding forward we'll see the key first. When
% `descending=true` and we're folding in reverse we'll see
% the value first.

fold_fwd({RowKey, EncodedOriginalKey}, #{next := key} = Acc) ->
    #{
        prefix := Prefix
    } = Acc,

    {{SortKey, DocId}, DupeId, ?VIEW_ROW_KEY} =
            erlfdb_tuple:unpack(RowKey, Prefix),
    Acc#{
        next := value,
        key := couch_views_encoding:decode(EncodedOriginalKey),
        sort_key := SortKey,
        docid := DocId,
        dupe_id := DupeId
    };

fold_fwd({RowKey, EncodedValue}, #{next := value} = Acc) ->
    #{
        prefix := Prefix,
        key := Key,
        sort_key := SortKey,
        docid := DocId,
        dupe_id := DupeId,
        callback := UserCallback,
        acc := UserAcc0
    } = Acc,

    % We're asserting there that this row is paired
    % correctly with the previous row by relying on
    % a badmatch if any of these values don't match.
    {{SortKey, DocId}, DupeId, ?VIEW_ROW_VALUE} =
            erlfdb_tuple:unpack(RowKey, Prefix),

    Value = couch_views_encoding:decode(EncodedValue),
    UserAcc1 = UserCallback(DocId, Key, Value, UserAcc0),

    Acc#{
        next := key,
        key := undefined,
        sort_key := undefined,
        docid := undefined,
        dupe_id := undefined,
        acc := UserAcc1
    }.


fold_rev({RowKey, EncodedValue}, #{next := value} = Acc) ->
    #{
        prefix := Prefix
    } = Acc,

    {{SortKey, DocId}, DupeId, ?VIEW_ROW_VALUE} =
            erlfdb_tuple:unpack(RowKey, Prefix),
    Acc#{
        next := key,
        value := couch_views_encoding:decode(EncodedValue),
        sort_key := SortKey,
        docid := DocId,
        dupe_id := DupeId
    };

fold_rev({RowKey, EncodedOriginalKey}, #{next := key} = Acc) ->
    #{
        prefix := Prefix,
        value := Value,
        sort_key := SortKey,
        docid := DocId,
        dupe_id := DupeId,
        callback := UserCallback,
        acc := UserAcc0
    } = Acc,

    % We're asserting there that this row is paired
    % correctly with the previous row by relying on
    % a badmatch if any of these values don't match.
    {{SortKey, DocId}, DupeId, ?VIEW_ROW_KEY} =
            erlfdb_tuple:unpack(RowKey, Prefix),

    Key = couch_views_encoding:decode(EncodedOriginalKey),
    UserAcc1 = UserCallback(DocId, Key, Value, UserAcc0),

    Acc#{
        next := value,
        value := undefined,
        sort_key := undefined,
        docid := undefined,
        dupe_id := undefined,
        acc := UserAcc1
    }.


clear_id_idx(TxDb, Sig, DocId) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,

    {Start, End} = id_idx_range(DbPrefix, Sig, DocId),
    ok = erlfdb:clear_range(Tx, Start, End).


clear_map_idx(TxDb, Sig, ViewId, DocId, ViewKeys) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,

    lists:foreach(fun(ViewKey) ->
        {Start, End} = map_idx_range(DbPrefix, Sig, ViewId, ViewKey, DocId),
        ok = erlfdb:clear_range(Tx, Start, End)
    end, ViewKeys).


update_id_idx(TxDb, Sig, ViewId, DocId, NewRows) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,

    Unique = lists:usort([K || {K, _V} <- NewRows]),

    Key = id_idx_key(DbPrefix, Sig, DocId, ViewId),
    RowSize = calculate_row_size(NewRows),
    Val = couch_views_encoding:encode([length(NewRows), RowSize, Unique]),
    ok = erlfdb:set(Tx, Key, Val).


update_map_idx(TxDb, Sig, ViewId, DocId, ExistingKeys, NewRows) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,

    Unique = lists:usort([K || {K, _V} <- NewRows]),

    KeysToRem = ExistingKeys -- Unique,
    lists:foreach(fun(RemKey) ->
        {Start, End} = map_idx_range(DbPrefix, Sig, ViewId, RemKey, DocId),
        ok = erlfdb:clear_range(Tx, Start, End)
    end, KeysToRem),

    KVsToAdd = process_rows(NewRows),
    MapIdxPrefix = map_idx_prefix(DbPrefix, Sig, ViewId),

    lists:foreach(fun({DupeId, Key1, Key2, Val}) ->
        KK = map_idx_key(MapIdxPrefix, {Key1, DocId}, DupeId, ?VIEW_ROW_KEY),
        VK = map_idx_key(MapIdxPrefix, {Key1, DocId}, DupeId, ?VIEW_ROW_VALUE),
        ok = erlfdb:set(Tx, KK, Key2),
        ok = erlfdb:set(Tx, VK, Val)
    end, KVsToAdd).


update_reduce_idx(TxDb, Sig, ViewId, DocId, ExistingKeys, NewRows) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,

%%    Unique = lists:usort([K || {K, _V} <- NewRows]),

%%    KeysToRem = ExistingKeys -- Unique,
%%    lists:foreach(fun(RemKey) ->
%%        {Start, End} = reduce_idx_range(DbPrefix, Sig, ViewId, RemKey, DocId),
%%        ok = erlfdb:clear_range(Tx, Start, End)
%%    end, KeysToRem),
%%
    {ExactKVsToAdd, GroupKVsToAdd} = process_reduce_rows(NewRows),
    ReduceIdxPrefix = reduce_idx_prefix(DbPrefix, Sig, ViewId),
    add_reduce_kvs(Tx, ReduceIdxPrefix, ExactKVsToAdd, ?VIEW_REDUCE_EXACT),
    add_reduce_kvs(Tx, ReduceIdxPrefix, GroupKVsToAdd, ?VIEW_REDUCE_GROUP).


add_reduce_kvs(Tx, ReduceIdxPrefix, KVsToAdd, ReduceType) ->
    lists:foreach(fun({Key1, Key2, Val, GroupLevel}) ->
        KK = reduce_idx_key(ReduceIdxPrefix, Key1, GroupLevel,
            ReduceType, ?VIEW_ROW_KEY),
        VK = reduce_idx_key(ReduceIdxPrefix, Key1, GroupLevel,
            ReduceType, ?VIEW_ROW_VALUE),
        ok = erlfdb:set(Tx, KK, Key2),
        ok = erlfdb:add(Tx, VK, Val)
    end, KVsToAdd).


reduce_idx_prefix(DbPrefix, Sig, ViewId) ->
    Key = {?DB_VIEWS, Sig, ?VIEW_REDUCE_RANGE, ViewId},
    erlfdb_tuple:pack(Key, DbPrefix).


reduce_idx_key(ReduceIdxPrefix, ReduceKey, GroupLevel, ReduceType, RowType) ->
    Key = {ReduceKey, GroupLevel, ReduceType, RowType},
    erlfdb_tuple:pack(Key, ReduceIdxPrefix).


%%reduce_idx_range(DbPrefix, Sig, ViewId, GroupKey, DocId) ->
%%    Encoded = couch_views_encoding:encode(MapKey, key),
%%    Key = {?DB_VIEWS, Sig, ?VIEW_MAP_RANGE, ViewId, {Encoded, DocId}},
%%    erlfdb_tuple:range(Key, DbPrefix).


get_view_keys(TxDb, Sig, DocId) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,
    {Start, End} = id_idx_range(DbPrefix, Sig, DocId),
    lists:map(fun({K, V}) ->
        {?DB_VIEWS, Sig, ?VIEW_ID_RANGE, DocId, ViewId} =
                erlfdb_tuple:unpack(K, DbPrefix),
        [TotalKeys, TotalSize, UniqueKeys] = couch_views_encoding:decode(V),
        {ViewId, TotalKeys, TotalSize, UniqueKeys}
    end, erlfdb:get_range(Tx, Start, End, [])).


update_row_count(TxDb, Sig, ViewId, Increment) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,
    Key = row_count_key(DbPrefix, Sig, ViewId),
    erlfdb:add(Tx, Key, Increment).


update_kv_size(TxDb, Sig, ViewId, Increment) ->
    #{
        tx := Tx,
        db_prefix := DbPrefix
    } = TxDb,
    Key = kv_size_key(DbPrefix, Sig, ViewId),
    erlfdb:add(Tx, Key, Increment).


seq_key(DbPrefix, Sig) ->
    Key = {?DB_VIEWS, Sig, ?VIEW_UPDATE_SEQ},
    erlfdb_tuple:pack(Key, DbPrefix).


row_count_key(DbPrefix, Sig, ViewId) ->
    Key = {?DB_VIEWS, Sig, ?VIEW_ID_INFO, ViewId, ?VIEW_ROW_COUNT},
    erlfdb_tuple:pack(Key, DbPrefix).


kv_size_key(DbPrefix, Sig, ViewId) ->
    Key = {?DB_VIEWS, Sig, ?VIEW_ID_INFO, ViewId, ?VIEW_KV_SIZE},
    erlfdb_tuple:pack(Key, DbPrefix).


id_idx_key(DbPrefix, Sig, DocId, ViewId) ->
    Key = {?DB_VIEWS, Sig, ?VIEW_ID_RANGE, DocId, ViewId},
    erlfdb_tuple:pack(Key, DbPrefix).


id_idx_range(DbPrefix, Sig, DocId) ->
    Key = {?DB_VIEWS, Sig, ?VIEW_ID_RANGE, DocId},
    erlfdb_tuple:range(Key, DbPrefix).

map_idx_prefix(DbPrefix, Sig, ViewId) ->
    Key = {?DB_VIEWS, Sig, ?VIEW_MAP_RANGE, ViewId},
    erlfdb_tuple:pack(Key, DbPrefix).


map_idx_key(MapIdxPrefix, MapKey, DupeId, Type) ->
    Key = {MapKey, DupeId, Type},
    erlfdb_tuple:pack(Key, MapIdxPrefix).


map_idx_range(DbPrefix, Sig, ViewId, MapKey, DocId) ->
    Encoded = couch_views_encoding:encode(MapKey, key),
    Key = {?DB_VIEWS, Sig, ?VIEW_MAP_RANGE, ViewId, {Encoded, DocId}},
    erlfdb_tuple:range(Key, DbPrefix).


process_rows(Rows) ->
    Encoded = lists:map(fun({K, V}) ->
        EK1 = couch_views_encoding:encode(K, key),
        EK2 = couch_views_encoding:encode(K, value),
        EV = couch_views_encoding:encode(V, value),
        {EK1, EK2, EV}
    end, Rows),

    Grouped = lists:foldl(fun({K1, K2, V}, Acc) ->
        dict:append(K1, {K2, V}, Acc)
    end, dict:new(), Encoded),

    dict:fold(fun(K1, Vals, DAcc) ->
        Vals1 = lists:keysort(2, Vals),
        {_, Labeled} = lists:foldl(fun({K2, V}, {Count, Acc}) ->
            {Count + 1, [{Count, K1, K2, V} | Acc]}
        end, {0, []}, Vals1),
        Labeled ++ DAcc
    end, [], Grouped).


process_reduce_rows(Rows) ->
    ReduceExact = encode_reduce_rows(Rows),
    ReduceGroups = lists:foldl(fun({Key, Val}, Groupings) ->
        Out = create_grouping(Key, Val, [], Groupings),
        io:format("ROW G ~p ~p ~p ~n", [Key, Val, Out]),
        Out
    end, #{}, Rows),
    io:format("INPUT ROWS ~n Groups ~p ~n Exact ~p ~n", [maps:to_list(ReduceGroups), Rows]),
    ReduceGroups1 = encode_reduce_rows(maps:to_list(ReduceGroups)),
    {ReduceExact, ReduceGroups1}.


encode_reduce_rows(Rows) ->
    lists:map(fun({K, V}) ->
        EK1 = couch_views_encoding:encode(K, key),
        EK2 = couch_views_encoding:encode(K, value),
        {EK1, EK2, V, group_level(K)}
    end, Rows).


group_level(Key) when is_list(Key) ->
    length(Key);

group_level(_Key) ->
    1.


create_grouping([], _Val, _, Groupings) ->
    Groupings;

create_grouping([Head | Rest], Val, Key, Groupings) ->
    Key1 = Key ++ [Head],
    Groupings1 = maps:update_with(Key1, fun(OldVal) ->
        OldVal + Val
        end, Val, Groupings),
    create_grouping(Rest, Val, Key1, Groupings1);

create_grouping(Key, Val, _, Groupings) ->
    maps:update_with(Key, fun(OldVal) ->
        OldVal + Val
    end, Val, Groupings).


calculate_row_size(Rows) ->
    lists:foldl(fun({K, V}, Acc) ->
        Acc + erlang:external_size(K) + erlang:external_size(V)
    end, 0, Rows).
