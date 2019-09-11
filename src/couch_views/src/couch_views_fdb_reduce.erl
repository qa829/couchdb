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


-module(couch_views_fdb_reduce).


-export([
%%    write_doc/4
]).

% _id keys = {?DB_VIEWS, Sig, ?VIEW_REDUCE_ID_RANGE, DocId, ViewId} = [TotalKeys, TotalSize, UniqueKeys]

%%write_doc(TxDb, Sig, ViewIds, Doc) ->
%%    #{
%%        id := DocId,
%%        reduce_results := ReduceResults
%%    } = Doc,
%%    lists:foreach(fun({ViewId, NewRows}) ->
%%        % update reduce index
%%        ok
%%    end, lists:zip(ViewIds, ReduceResults)).


