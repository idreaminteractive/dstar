if Code.ensure_loaded?(Phoenix.Controller) do
  defmodule Dstar.LivePage.Plug do
    @moduledoc """
    The plug behind `Dstar.Router.dlive/2`. Drives `Dstar.LivePage` callbacks:

    - `{:live_page, Module}` — GET: `mount/2` then `render/1`, same as
      `Dstar.Page`.
    - `{:live_event, Module}` — POST `_event/:event`: reads signals, optional
      `authorize/2`, resolves the stream loop for `{stream_key, tabId}`,
      calls `handle_event/4`. An untouched conn becomes 204; a conn the
      handler started SSE on is returned as-is for ephemeral patches.
      No loop answers halted 410.
    - `{:live_stream, Module}` — POST: optional `authorize/2`, claims
      `stream_key/1` (required), starts SSE, calls `handle_connect/2`,
      then runs the receive loop on `Dstar.Stream`. `Dstar.LiveStore`
      messages apply to `conn.assigns` and dispatch
      `handle_info({:store_updated, keys}, conn)`.

    All control flow lives here as plain functions — live pages contain
    only callbacks.
    """

    @behaviour Plug

    require Logger
    import Plug.Conn

    alias Dstar.LiveStore
    alias Dstar.Utility.StreamRegistry

    @impl Plug
    def init({action, page})
        when action in [:live_page, :live_event, :live_stream] and is_atom(page) do
      {action, page}
    end

    @impl Plug
    def call(conn, {:live_page, page}), do: page(conn, page)
    def call(conn, {:live_event, page}), do: live_event(conn, page)
    def call(conn, {:live_stream, page}), do: live_stream(conn, page)

    # ── GET: mount + render ─────────────────────────────────────────────

    defp page(conn, page) do
      conn = conn |> fetch_query_params() |> ensure_html_format()

      conn =
        if exported?(page, :mount, 2) do
          page.mount(conn, conn.params)
        else
          conn
        end

      if response_committed?(conn) do
        conn
      else
        conn
        |> Phoenix.Controller.put_view(html: page)
        |> Phoenix.Controller.render(:render)
      end
    end

    defp ensure_html_format(conn) do
      if Phoenix.Controller.get_format(conn) do
        conn
      else
        Phoenix.Controller.put_format(conn, "html")
      end
    end

    # ── POST _event/:event: authorize, route to loop, 204 ───────────────

    defp live_event(conn, page) do
      event =
        conn.path_params["event"] ||
          raise(
            ArgumentError,
            "missing :event path param — route the event POST with an `:event` segment"
          )

      conn = fetch_query_params(conn)

      case Dstar.Signals.fetch(conn, max_bytes: page.__dstar__(:max_signal_bytes)) do
        {:ok, signals, conn} ->
          conn = maybe_authorize(conn, page, {:event, event})

          if response_committed?(conn) do
            conn
          else
            forward_event(conn, page, event, signals)
          end

        {:error, reason, conn} ->
          Dstar.Signals.send_error(conn, reason)
      end
    end

    defp forward_event(conn, page, event, signals) do
      with {:key, {:ok, scope}} <- {:key, stream_scope(conn, page)},
           {:tab, tab_id} when is_binary(tab_id) <- {:tab, StreamRegistry.tab_id(signals)},
           key = {scope, tab_id},
           {:owner, {:ok, pid, _claim}} <- {:owner, StreamRegistry.owner(key)},
           true <- exported?(page, :handle_event, 4) do
        store = %LiveStore{pid: pid, key: key, tab_id: tab_id}

        conn =
          guard(page, :handle_event, conn, fn ->
            page.handle_event(conn, event, signals, store)
          end)

        if response_committed?(conn) do
          conn
        else
          conn |> send_resp(204, "") |> halt()
        end
      else
        {:key, :error} -> server_error(conn, page)
        {:tab, _} -> gone(conn)
        {:owner, :error} -> gone(conn)
        false -> gone(conn)
      end
    end

    defp stream_scope(conn, page) do
      if exported?(page, :stream_key, 1) do
        {:ok, page.stream_key(conn)}
      else
        :error
      end
    end

    defp gone(conn) do
      conn
      |> put_resp_content_type("text/plain")
      |> send_resp(410, "Stream not connected")
      |> halt()
    end

    defp server_error(conn, page) do
      Logger.error("Dstar.LivePage.Plug: #{inspect(page)} missing required stream_key/1")

      conn
      |> put_resp_content_type("text/plain")
      |> send_resp(500, "stream_key/1 is required")
      |> halt()
    end

    # ── POST stream: connect, then library-owned receive loop ───────────

    defp live_stream(conn, page) do
      cond do
        not exported?(page, :stream_key, 1) ->
          server_error(conn, page)

        not exported?(page, :handle_connect, 2) ->
          conn
          |> put_resp_content_type("text/plain")
          |> send_resp(404, "Not found")

        true ->
          before_sse(conn, page, fn conn, _signals -> open_stream(conn, page) end)
      end
    end

    defp before_sse(conn, page, start) do
      conn = fetch_query_params(conn)

      case Dstar.Signals.fetch(conn, max_bytes: page.__dstar__(:max_signal_bytes)) do
        {:ok, signals, conn} ->
          conn = maybe_authorize(conn, page, {:stream, conn.params})

          if response_committed?(conn) do
            conn
          else
            start.(conn, signals)
          end

        {:error, reason, conn} ->
          Dstar.Signals.send_error(conn, reason)
      end
    end

    defp open_stream(conn, page) do
      opts = [max_bytes: page.__dstar__(:max_signal_bytes), key: page.stream_key(conn)]

      case Dstar.Stream.open(conn, opts) do
        {:ok, conn} ->
          Dstar.Stream.run(conn,
            connect: &connect(&1, page),
            info: &live_info(&1, &2, page),
            replaced: &offer_replaced(&2, &1, page),
            disconnect: &disconnect(&1, page),
            idle_check: page.__dstar__(:idle_check)
          )

        {:error, conn} ->
          conn
      end
    end

    defp connect(conn, page) do
      guard(page, :handle_connect, conn, fn -> page.handle_connect(conn, conn.params) end)
    end

    defp live_info(msg, conn, page) do
      case LiveStore.message(msg) do
        {:ok, update} ->
          {conn, keys} = LiveStore.apply_message(conn, update)
          dispatch_info(page, {:store_updated, keys}, conn)

        :error ->
          dispatch_info(page, msg, conn)
      end
    end

    defp offer_replaced(conn, msg, page) do
      if exported?(page, :handle_info, 2) do
        dispatch_info(page, msg, conn, warn_unhandled: false)
      else
        conn
      end
    end

    defp disconnect(conn, page) do
      if exported?(page, :handle_disconnect, 1) do
        try do
          page.handle_disconnect(conn)
        rescue
          exception -> log_crash(page, :handle_disconnect, exception, __STACKTRACE__)
        end
      end
    end

    defp maybe_authorize(conn, page, action) do
      if exported?(page, :authorize, 2) do
        page.authorize(conn, action)
      else
        conn
      end
    end

    defp response_committed?(%Plug.Conn{halted: true}), do: true
    defp response_committed?(%Plug.Conn{state: state}), do: state != :unset

    defp exported?(module, fun, arity) do
      Code.ensure_loaded?(module) and function_exported?(module, fun, arity)
    end

    defp dispatch_info(page, msg, conn, opts \\ []) do
      page.handle_info(msg, conn)
    rescue
      exception in FunctionClauseError ->
        if exception.module == page and exception.function == :handle_info and
             exception.arity == 2 do
          if Keyword.get(opts, :warn_unhandled, true) do
            Logger.warning("#{inspect(page)} received unhandled message: #{inspect(msg)}")
          end

          conn
        else
          crash(page, :handle_info, conn, exception, __STACKTRACE__)
        end

      exception ->
        crash(page, :handle_info, conn, exception, __STACKTRACE__)
    end

    defp guard(page, callback, conn, fun) do
      fun.()
    rescue
      exception -> crash(page, callback, conn, exception, __STACKTRACE__)
    end

    defp crash(page, callback, conn, exception, stacktrace) do
      log_crash(page, callback, exception, stacktrace)

      if Application.get_env(:dstar, :debug_errors, false) do
        try do
          Dstar.console_log(conn, Exception.format(:error, exception, stacktrace), level: :error)
        rescue
          _ -> :ok
        end
      end

      reraise exception, stacktrace
    end

    defp log_crash(page, callback, exception, stacktrace) do
      Logger.error(
        "Dstar.LivePage.Plug: #{inspect(page)}.#{callback} raised:\n" <>
          Exception.format(:error, exception, stacktrace)
      )
    end
  end
end
