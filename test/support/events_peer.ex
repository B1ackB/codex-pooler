defmodule CodexPooler.EventsPeer do
  @moduledoc false

  use GenServer

  alias CodexPooler.Events
  alias CodexPooler.Events.PostgresBridge
  alias CodexPooler.Repo

  @spec start(keyword()) :: GenServer.on_start()
  def start(connection_options) do
    {:ok, _} = Application.ensure_all_started(:phoenix_pubsub)
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:postgrex)
    GenServer.start(__MODULE__, connection_options, name: __MODULE__)
  end

  @spec subscribe(Ecto.UUID.t()) :: boolean()
  def subscribe(pool_id), do: GenServer.call(__MODULE__, {:subscribe, pool_id})
  @spec events() :: [Events.Event.t()]
  def events, do: GenServer.call(__MODULE__, :events)

  @impl true
  def init(connection_options) do
    {:ok, _} =
      Supervisor.start_link(
        [
          {Phoenix.PubSub, name: CodexPooler.PubSub, pool_size: 1},
          {Repo, Keyword.put(connection_options, :pool, DBConnection.ConnectionPool)},
          {Postgrex.Notifications, Keyword.put(connection_options, :name, CodexPooler.Events.PostgresNotifications)},
          PostgresBridge
        ],
        strategy: :one_for_one
      )

    {:ok, []}
  end

  @impl true
  def handle_call({:subscribe, pool_id}, _from, events) do
    :ok = Events.subscribe_pool(pool_id, "request_logs")
    # GenServer startup completes only after the bridge registered its LISTEN.
    state = :sys.get_state(PostgresBridge)
    {:reply, is_reference(state.listen_ref), events}
  end

  def handle_call(:events, _from, events), do: {:reply, Enum.reverse(events), events}

  @impl true
  def handle_info({Events, event}, events), do: {:noreply, [event | events]}
end
