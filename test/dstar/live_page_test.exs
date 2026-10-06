defmodule Dstar.LivePageTest do
  use ExUnit.Case, async: true

  defmodule CounterLive do
    use Dstar.LivePage

    @impl true
    def mount(conn, _params), do: assign(conn, count: 0)

    @impl true
    def render(assigns) do
      ~H"""
      <div data-signals:count={@count}>
        <button data-on:click={event("increment")}>+1</button>
      </div>
      """
    end

    @impl true
    def stream_key(_conn), do: :live_counter

    @impl true
    def handle_connect(conn, _params), do: assign(conn, count: 0)

    @impl true
    def handle_event(conn, "increment", _signals, store) do
      Dstar.LiveStore.update(store, :count, &((&1 || 0) + 1))
      conn
    end

    @impl true
    def handle_info({:store_updated, _keys}, conn) do
      patch_signals(conn, Map.take(conn.assigns, [:count]))
    end
  end

  defmodule TunedLive do
    use Dstar.LivePage, idle_check: 50

    @impl true
    def stream_key(_conn), do: :tuned

    @impl true
    def render(assigns),
      do: ~H"""
      <div id="t">tuned</div>
      """
  end

  defp render_to_string(rendered) do
    rendered |> Phoenix.HTML.Safe.to_iodata() |> IO.iodata_to_binary()
  end

  test "render/1 produces HEEx with the page-local event helper" do
    html = render_to_string(CounterLive.render(%{count: 0}))
    assert html =~ "data-signals:count"
    assert html =~ "_event/increment"
  end

  test "the assign shim works on conns inside mount" do
    conn = Plug.Test.conn(:get, "/") |> CounterLive.mount(%{})
    assert conn.assigns.count == 0
  end

  test "handle_event/4 forwards to the store and returns the conn" do
    store = %Dstar.LiveStore{pid: self(), key: :live_counter}

    conn =
      Plug.Test.conn(:post, "/")
      |> CounterLive.handle_event("increment", %{}, store)

    assert conn.state == :unset
    assert_received {Dstar.LiveStore, {:update, :count, fun}} when is_function(fun, 1)
  end

  test "__dstar__(:idle_check) defaults to 30_000" do
    assert CounterLive.__dstar__(:idle_check) == 30_000
  end

  test "__dstar__(:idle_check) is overridable via use options" do
    assert TunedLive.__dstar__(:idle_check) == 50
  end
end
