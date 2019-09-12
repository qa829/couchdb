defmodule CouchViewsReduceTest do
  use Couch.Test.ExUnit.Case

  alias Couch.Test.Utils

  alias Couch.Test.Setup

  alias Couch.Test.Setup.Step

  setup_all do
    test_ctx = :test_util.start_couch([:fabric, :couch_views, :couch_jobs])

    on_exit(fn ->
      :test_util.stop_couch(test_ctx)
    end)
  end

  setup do
    db_name = Utils.random_name("db")

    admin_ctx =
      {:user_ctx,
       Utils.erlang_record(:user_ctx, "couch/include/couch_db.hrl", roles: ["_admin"])}

    {:ok, db} = :fabric2_db.create(db_name, [admin_ctx])

    docs = create_docs()
    ddoc = create_ddoc()

    {ok, _} = :fabric2_db.update_docs(db, [ddoc | docs])

    on_exit(fn ->
      :fabric2_db.delete(db_name, [admin_ctx])
    end)

    %{
      :db_name => db_name,
      :db => db,
      :ddoc => ddoc
    }
  end

  test "group=true count reduce", context do
        args = %{
            :reduce => true,
            :group => true
#            :limit => 9
        }

        {:ok, res} = run_query(context, args)
        IO.inspect(res, label: "OUT")

        assert res == [
                   {:row, [key: 1, value: 2]},
                   {:row, [key: 2, value: 2]},
                   {:row, [key: 3, value: 2]},
                   {:row, [key: [1, 1], value: 1]},
                   {:row, [key: [1, 2, 6], value: 1]},
                   {:row, [key: [2, 1], value: 1]},
                   {:row, [key: [2, 3, 6], value: 1]},
                   {:row, [key: [3, 1], value: 1]},
                   {:row, [key: [3, 4, 5], value: 1]}
               ]
    end

  test "group=1 count reduce", context do
      args = %{
          :reduce => true,
          :group_level => 1
#          :limit => 6
      }

      {:ok, res} = run_query(context, args)
      IO.inspect(res, label: "OUT")

      assert res == [
                 {:row, [key: 1, value: 2]},
                 {:row, [key: 2, value: 2]},
                 {:row, [key: 3, value: 2]},
                 {:row, [key: [1], value: 2]},
                 {:row, [key: [2], value: 2]},
                 {:row, [key: [3], value: 2]}
             ]
  end

  test "group=2 count reduce", context do
      args = %{
          :reduce => true,
          :group_level => 2
#          :limit => 9
      }

      {:ok, res} = run_query(context, args)
      IO.inspect(res, label: "OUT")

      assert res == [
                 {:row, [key: 1, value: 2]},
                 {:row, [key: 2, value: 2]},
                 {:row, [key: 3, value: 2]},
                 {:row, [key: [1, 1], value: 1]},
                 {:row, [key: [1, 2], value: 1]},
                 {:row, [key: [2, 1], value: 1]},
                 {:row, [key: [2, 3], value: 1]},
                 {:row, [key: [3, 1], value: 1]},
                 {:row, [key: [3, 4], value: 1]}
             ]
  end

  test "group=2 count reduce with limit = 3", context do
      args = %{
          :reduce => true,
          :group_level => 2,
          :limit => 4
      }

      {:ok, res} = run_query(context, args)
      IO.inspect(res, label: "OUT")

      assert res == [
                 {:row, [key: 1, value: 2]},
                 {:row, [key: 2, value: 2]},
                 {:row, [key: 3, value: 2]},
                 {:row, [key: [1, 1], value: 1]}
             ]
  end

  defp run_query(context, args) do
    db = context[:db]
    ddoc = context[:ddoc]

    :couch_views.query(db, ddoc, "baz", &__MODULE__.default_cb/2, [], args)
  end

  def default_cb(:complete, acc) do
    {:ok, Enum.reverse(acc)}
  end

  def default_cb({:final, info}, []) do
    {:ok, [info]}
  end

  def default_cb({:final, _}, acc) do
    {:ok, acc}
  end

  def default_cb({:meta, _}, acc) do
    {:ok, acc}
  end

  def default_cb(:ok, :ddoc_updated) do
    {:ok, :ddoc_updated}
  end

  def default_cb(row, acc) do
    {:ok, [row | acc]}
  end

  defp create_docs() do
    for i <- 1..3 do
      group =
        if rem(i, 3) == 0 do
          "first"
        else
          "second"
        end

      :couch_doc.from_json_obj(
        {[
           {"_id", "doc-id-#{i}"},
           {"value", i},
           {"some", "field"},
           {"group", group}
         ]}
      )
    end
  end

  defp create_ddoc() do
    :couch_doc.from_json_obj(
      {[
         {"_id", "_design/bar"},
         {"views",
          {[
             {"baz",
              {[
                 {"map",
                  """
                  function(doc) {
                    emit(doc.value, doc.value);
                    emit(doc.value, doc.value);
                    emit([doc.value, 1], doc.value);
                    emit([doc.value, doc.value + 1, doc.group.length], doc.value);
                   }
                  """},
                 {"reduce", "_count"}
               ]}},
             {"boom",
              {[
                 {"map", "function(doc) {emit([doc._id, doc.value], doc.value);}"},
                 {"reduce", "_count"}
               ]}}
           ]}}
       ]}
    )
  end
end
