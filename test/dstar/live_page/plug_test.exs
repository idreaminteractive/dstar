defmodule Dstar.LivePage.PlugTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Dstar.Test

  alias Dstar.LivePage.Plug, as: LivePlug

  defmodule CounterLive do
    use Dstar.LivePage, idle_check: 50

    @impl true
    def mount(conn, _params), do: assign(conn, count: 0)

    @impl true
    def render(assigns) do
      ~H"""
      <div data-signals:count={@count}>live</div>
      """
    end

    @impl true
    def stream_key(_conn), do: :live_plug_scope

    @impl true
    def handle_connect(conn, _params) do
      send(:dstar_live_plug_test, {:connected, self()})
      assign(conn, count: 0)
    end

    @impl true
    def handle_event(conn, "increment", _signals, store) do
      Dstar.LiveStore.update(store, :count, &((&1 || 0) + 1))
      conn
    end

    @impl true
    def handle_info({:store_updated, _keys}, conn) do
      patch_signals(conn, %{count: conn.assigns.count})
    end

    @impl true
    def handle_info(:halt_now, conn), do: {:halt, conn}
  end

  defmodule EphemeralLive do
    use Dstar.LivePage, idle_check: 50

    @impl true
    def render(assigns),
      do: ~H"""
      <div id="e">ephemeral</div>
      """

    @impl true
    def stream_key(_conn), do: :live_ephemeral_scope

    @impl true
    def handle_connect(conn, _params) do
      send(:dstar_live_plug_test, {:connected, self()})
      conn
    end

    @impl true
    def handle_event(conn, "validate", _signals, _store) do
      conn
      |> Dstar.SSE.start()
      |> Dstar.Signals.patch(%{error: "bad"})
    end

    @impl true
    def handle_info(:halt_now, conn), do: {:halt, conn}
  end

  defmodule NoKeyLive do
    use Dstar.LivePage

    @impl true
    def render(assigns),
      do: ~H"""
      <div id="n">no key</div>
      """

    @impl true
    def handle_connect(conn, _params), do: conn
  end

  defmodule BareLive do
    use Dstar.LivePage

    @impl true
    def render(assigns),
      do: ~H"""
      <div id="b">bare</div>
      """

    @impl true
    def stream_key(_conn), do: :live_bare_scope
  end

  setup do
    Process.register(self(), :dstar_live_plug_test)

    on_exit(fn ->
      try do
        Process.unregister(:dstar_live_plug_test)
      rescue
        _ -> :ok
      end
    end)

    :ok
  end

  defp stream_conn(tab_id) do
    conn(:post, "/live")
    |> Plug.Conn.put_req_header("content-type", "application/json")
    |> Map.put(:body_params, %{"tabId" => tab_id})
  end

  defp event_conn(event, signals) do
    conn(:post, "/live/_event/#{event}")
    |> Map.put(:path_params, %{"event" => event})
    |> Plug.Conn.put_req_header("content-type", "application/json")
    |> Map.put(:body_params, signals)
  end

  defp run_stream(page, tab_id) do
    parent = self()
    conn = stream_conn(tab_id)

    pid =
      spawn(fn ->
        returned = LivePlug.call(conn, LivePlug.init({:live_stream, page}))
        send(parent, {:loop_returned, returned})
        Process.sleep(:infinity)
      end)

    assert_receive {:connected, ^pid}, 1_000
    pid
  end

  describe "page action (GET)" do
    test "mounts and renders HTML 200" do
      conn = LivePlug.call(conn(:get, "/live"), LivePlug.init({:live_page, CounterLive}))

      assert conn.status == 200
      assert conn.state == :sent
      assert conn.resp_body =~ "data-signals:count"
    end
  end

  describe "event action without a loop" do
    test "returns 410 when no stream owns the key" do
      conn = event_conn("increment", %{"tabId" => "tab-live-missing"})
      conn = LivePlug.call(conn, LivePlug.init({:live_event, CounterLive}))

      assert conn.status == 410
      assert conn.state == :sent
      assert conn.halted
    end

    test "returns 410 when tabId is missing" do
      conn = event_conn("increment", %{})
      conn = LivePlug.call(conn, LivePlug.init({:live_event, CounterLive}))

      assert conn.status == 410
    end

    test "returns 400 for malformed signals before routing" do
      conn =
        conn(:post, "/live/_event/increment", "[]")
        |> Map.put(:path_params, %{"event" => "increment"})
        |> LivePlug.call(LivePlug.init({:live_event, CounterLive}))

      assert conn.status == 400
    end

    test "returns 500 when stream_key/1 is missing" do
      conn = event_conn("anything", %{"tabId" => "tab-x"})
      conn = LivePlug.call(conn, LivePlug.init({:live_event, NoKeyLive}))

      assert conn.status == 500
    end
  end

  describe "event action with a loop" do
    test "returns 204 and the loop re-renders from the store update" do
      pid = run_stream(CounterLive, "tab-live-1")

      conn = event_conn("increment", %{"tabId" => "tab-live-1"})
      conn = LivePlug.call(conn, LivePlug.init({:live_event, CounterLive}))

      assert conn.status == 204
      assert conn.state == :sent
      assert conn.halted

      send(pid, :halt_now)
      assert_receive {:loop_returned, stream_conn}, 2_000
      assert_patched_signals(stream_conn, %{count: 1})

      Process.exit(pid, :kill)
    end

    test "a handler-started SSE response is returned as-is for ephemeral patches" do
      pid = run_stream(EphemeralLive, "tab-live-eph")

      conn = event_conn("validate", %{"tabId" => "tab-live-eph"})
      conn = LivePlug.call(conn, LivePlug.init({:live_event, EphemeralLive}))

      assert conn.state == :chunked
      assert_patched_signals(conn, %{error: "bad"})

      send(pid, :halt_now)
      assert_receive {:loop_returned, _}, 2_000

      Process.exit(pid, :kill)
    end
  end

  describe "stream action (POST)" do
    test "500s when stream_key/1 is missing" do
      conn =
        LivePlug.call(stream_conn("tab-live-nokey"), LivePlug.init({:live_stream, NoKeyLive}))

      assert conn.status == 500
    end

    test "404s when the page has no handle_connect" do
      conn = LivePlug.call(stream_conn("tab-live-bare"), LivePlug.init({:live_stream, BareLive}))
      assert conn.status == 404
    end

    test "applies direct store messages and dispatches store_updated" do
      pid = run_stream(CounterLive, "tab-live-2")

      {:ok, stream_pid, _} =
        Dstar.Utility.StreamRegistry.owner({:live_plug_scope, "tab-live-2"})

      assert stream_pid == pid

      :ok =
        Dstar.Utility.StreamRegistry.forward(
          {:live_plug_scope, "tab-live-2"},
          {Dstar.LiveStore, {:assign, %{count: 9}}}
        )

      send(pid, :halt_now)
      assert_receive {:loop_returned, stream_conn}, 2_000
      assert_patched_signals(stream_conn, %{count: 9})

      Process.exit(pid, :kill)
    end
  end
end
