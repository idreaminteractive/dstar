defmodule Dstar.LivePage do
  @moduledoc """
  Stateful CQS pages: `mount` renders once, a stream loop owns assigns,
  events forward to the loop and answer 204.

      defmodule MyAppWeb.CounterLive do
        use Dstar.LivePage

        def mount(conn, _params), do: assign(conn, count: 0)

        def render(assigns) do
          ~H\"\"\"
          <div data-signals:count={@count}>
            <button data-on:click={event("increment")}>+1</button>
          </div>
          <div data-init={connect()}></div>
          \"\"\"
        end

        def stream_key(conn), do: {:counter, conn.assigns.current_user.id}

        def handle_connect(conn, _params), do: assign(conn, count: 0)

        def handle_event(conn, "increment", _signals, store) do
          Dstar.LiveStore.update(store, :count, &((&1 || 0) + 1))
          conn
        end

        def handle_info({:store_updated, _keys}, conn) do
          conn
          |> patch_signals(Map.take(conn.assigns, [:count]))
          |> patch(&history/1, value: conn.assigns.count)
        end

        defp history(assigns) do
          ~H"<span id=\\"history\\">Last: {@value}</span>"
        end
      end

  Route it with `Dstar.Router.dlive/2`:

      import Dstar.Router
      dlive "/counter", MyAppWeb.CounterLive

  Requests are driven by `Dstar.LivePage.Plug`:

    - GET — `mount/2` then `render/1`, same as `Dstar.Page`.
    - POST stream — `authorize/2`, claim `stream_key/1`, `handle_connect/2`,
      then the receive loop. Store messages apply to `conn.assigns` and
      dispatch `handle_info({:store_updated, keys}, conn)`.
    - POST `_event/:event` — `authorize/2`, resolve the loop for
      `{stream_key, tabId}`, call `handle_event/4`. An untouched conn
      becomes 204; `Dstar.SSE.start/1` plus patches answers a short SSE
      response for ephemeral UI (validation errors). No loop answers
      410 so the client reconnects the stream.

  `stream_key/1` is required. Events cannot route without it.
  """

  @doc "GET: load data and assign what `render/1` needs. Optional."
  @callback mount(Plug.Conn.t(), params :: map()) :: Plug.Conn.t()

  @doc "The full-page HEEx template. Required."
  @callback render(assigns :: map()) :: Phoenix.LiveView.Rendered.t()

  @doc """
  Pre-SSE gate for event and stream POSTs. Optional.

  Same contract as `Dstar.Page.authorize/2`: runs after signals are read
  and before any claim, SSE start, or handler. Halt or send a response to
  reject; return the conn to continue.
  """
  @callback authorize(
              Plug.Conn.t(),
              {:event, event :: String.t()} | {:stream, params :: map()}
            ) :: Plug.Conn.t()

  @doc """
  Stream open: subscribe to topics, assign loop state. Optional.

  The returned conn's assigns seed the `Dstar.LiveStore` state.
  """
  @callback handle_connect(Plug.Conn.t(), params :: map()) :: Plug.Conn.t()

  @doc """
  Handles a Datastar event POST. SSE is NOT started.

  Forward state changes with `store`, then return the conn untouched for
  204 — or start SSE and patch for ephemeral UI that must not touch the
  store (validation errors). Optional; missing handler answers 410.
  """
  @callback handle_event(
              Plug.Conn.t(),
              event :: String.t(),
              signals :: map(),
              store :: Dstar.LiveStore.t()
            ) :: Plug.Conn.t()

  @doc "Handles one message from the library-owned receive loop. Optional."
  @callback handle_info(msg :: term(), Plug.Conn.t()) :: Plug.Conn.t() | {:halt, Plug.Conn.t()}

  @doc """
  Required. Scopes the stream claim and routes events to the loop.

  The claim key is `{result, tabId}` through
  `Dstar.Utility.StreamRegistry`, same as `Dstar.Page.stream_key/1`.
  """
  @callback stream_key(Plug.Conn.t()) :: term()

  @doc """
  Stream close: release what `handle_connect/2` acquired. Optional.
  Same contract as `Dstar.Page.handle_disconnect/1`.
  """
  @callback handle_disconnect(Plug.Conn.t()) :: any()

  @optional_callbacks mount: 2,
                      authorize: 2,
                      handle_connect: 2,
                      handle_event: 4,
                      handle_info: 2,
                      handle_disconnect: 1

  @default_idle_check 30_000

  defmacro __using__(opts) do
    unless Code.ensure_loaded?(Phoenix.Component) do
      raise ArgumentError, """
      `use Dstar.LivePage` requires the optional dependencies. Add to your deps:

          {:phoenix, "~> 1.7"},
          {:phoenix_live_view, "~> 1.0"}
      """
    end

    idle_check = Keyword.get(opts, :idle_check, @default_idle_check)
    max_signal_bytes = Keyword.get(opts, :max_signal_bytes, Dstar.Signals.default_max_bytes())

    quote do
      @behaviour Dstar.LivePage

      use Phoenix.Component

      import Phoenix.Component,
        except: [assign: 2, assign: 3, assign_new: 3, update: 3]

      import Dstar.Page.Assigns

      import Dstar,
        only: [
          start: 1,
          start_stream: 2,
          check_connection: 1,
          read_signals: 1,
          patch_signals: 2,
          patch_signals: 3,
          remove_signals: 2,
          remove_signals: 3,
          patch_elements: 3,
          remove_elements: 2,
          remove_elements: 3,
          append_elements: 3,
          append_elements: 4,
          upsert_elements: 2,
          upsert_elements: 3,
          nudge: 2,
          nudge: 3,
          execute_script: 2,
          execute_script: 3,
          redirect: 2,
          redirect: 3,
          console_log: 2,
          console_log: 3
        ]

      import Dstar.Page.Helpers

      @doc false
      def __dstar__(:idle_check), do: unquote(idle_check)
      def __dstar__(:max_signal_bytes), do: unquote(max_signal_bytes)
    end
  end
end
