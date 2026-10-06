defmodule Dstar.LiveStore do
  @moduledoc """
  CQS store ref for `Dstar.LivePage` event handlers.

  The store has no process of its own. State lives in the stream loop's
  `conn.assigns`; this struct only routes updates to the loop pid that
  `Dstar.Utility.StreamRegistry` tracks for `{stream_key, tabId}`.

  Event handlers call `update/3` or `assign/2`, then return 204. The loop
  applies the change through `apply_message/2` and invokes the page's
  `handle_info/2` with `{:store_updated, keys}` so one clause re-renders
  every affected bit.

      def handle_event(conn, "increment", _signals, store) do
        Dstar.LiveStore.update(store, :count, &(&1 + 1))
        conn
      end

  Future ETS/DB adapters sit behind these same functions; the message
  shape below is the seam.
  """

  @enforce_keys [:pid, :key]
  defstruct [:pid, :key, tab_id: nil]

  @type t :: %__MODULE__{pid: pid(), key: term(), tab_id: String.t() | nil}
  @type update :: {:assign, map()} | {:update, atom(), (term() -> term())}

  @message __MODULE__

  @doc """
  Builds a store ref for the loop process owning `key`.
  Returns `{:ok, store}` or `{:error, :no_owner}`.
  """
  @spec new(term(), String.t() | nil) :: {:ok, t()} | {:error, :no_owner}
  def new(key, tab_id \\ nil) do
    case Dstar.Utility.StreamRegistry.owner(key) do
      {:ok, pid, _claim} -> {:ok, %__MODULE__{pid: pid, key: key, tab_id: tab_id}}
      :error -> {:error, :no_owner}
    end
  end

  @doc "Sends `{:assign, map}` to the loop. Fire-and-forget."
  @spec assign(t(), map() | keyword()) :: :ok
  def assign(%__MODULE__{pid: pid}, kv) do
    send(pid, {@message, {:assign, Map.new(kv)}})
    :ok
  end

  @doc "Sends `{:update, key, fun}` to the loop. Fire-and-forget."
  @spec update(t(), atom(), (term() -> term())) :: :ok
  def update(%__MODULE__{pid: pid}, key, fun)
      when is_atom(key) and is_function(fun, 1) do
    send(pid, {@message, {:update, key, fun}})
    :ok
  end

  @doc false
  @spec message(term()) :: {:ok, update()} | :error
  def message({@message, update}), do: {:ok, update}
  def message(_msg), do: :error

  @doc false
  @spec apply_message(Plug.Conn.t(), update()) :: {Plug.Conn.t(), [atom()]}
  def apply_message(conn, {:assign, kv}) do
    conn = Enum.reduce(kv, conn, fn {key, value}, acc -> Plug.Conn.assign(acc, key, value) end)
    {conn, Map.keys(kv)}
  end

  def apply_message(%Plug.Conn{assigns: assigns} = conn, {:update, key, fun}) do
    {Plug.Conn.assign(conn, key, fun.(Map.get(assigns, key))), [key]}
  end
end
