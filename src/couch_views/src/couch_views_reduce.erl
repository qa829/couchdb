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
    run_reduce/2
]).


-include("couch_views.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").


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
    maps:to_list(ReduceResults).


is_builtin(<<"_", _/binary>>) ->
    true;

is_builtin(_) ->
    false.

