defmodule Dstar.LiveStoreTest do
  use ExUnit.Case, async: true

  alias Dstar.LiveStore

  test "new/1 returns :no_owner without a claim" do
    assert LiveStore.new({:nobody, "tab-x"}) == {:error, :no_owner}
  end

  test "assign/2 and update/2 send loop messages" do
    store = %LiveStore{pid: self(), key: :k}

    assert LiveStore.assign(store, count: 1) == :ok
    assert LiveStore.update(store, :count, &(&1 + 1)) == :ok

    assert_received {Dstar.LiveStore, {:assign, %{count: 1}}}
    assert_received {Dstar.LiveStore, {:update, :count, fun}} when is_function(fun, 1)
  end

  test "message/1 accepts only store messages" do
    assert LiveStore.message({Dstar.LiveStore, {:assign, %{}}}) ==
             {:ok, {:assign, %{}}}

    assert LiveStore.message(:stray) == :error
    assert LiveStore.message({Dstar.LiveStore, :bogus}) == {:ok, :bogus}
  end

  test "apply_message/2 threads assigns and reports keys" do
    conn = Plug.Test.conn(:post, "/")

    {conn, keys} = LiveStore.apply_message(conn, {:assign, %{count: 2, name: "x"}})
    assert conn.assigns.count == 2
    assert conn.assigns.name == "x"
    assert Enum.sort(keys) == [:count, :name]

    {conn, keys} = LiveStore.apply_message(conn, {:update, :count, &(&1 + 1)})
    assert conn.assigns.count == 3
    assert keys == [:count]
  end

  test "forward/2 routes to the owner and 404s without one" do
    alias Dstar.Utility.StreamRegistry

    assert StreamRegistry.forward({:ghost, "tab"}, :ping) == {:error, :no_owner}
  end
end
