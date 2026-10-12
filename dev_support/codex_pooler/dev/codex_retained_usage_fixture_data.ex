defmodule CodexPooler.Dev.CodexRetainedUsageFixtureData do
  @moduledoc false
  import Ecto.Query
  alias CodexPooler.{Access, Accounts, Repo}
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Dev.CodexCompactionSmokeFixture.{Journal, Provisioner}
  alias CodexPooler.Dev.{CodexRetainedUsageFixture, CodexRetainedUsageFixtureUI, LocalTarget, QaBackgroundWorkers}
  alias CodexPooler.Gateway.Payloads.WebsocketTurnIdentity
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Websocket

  @spec verify_target!() :: :ok
  def verify_target! do
    name = System.fetch_env!("CODEX_POOLER_DEV_POSTGRES_DB")
    CodexRetainedUsageFixture.validate_database!(name)
    config = Repo.config()
    true = config[:database] == name and config[:hostname] in ["localhost", "127.0.0.1"]
    false = Keyword.has_key?(config, :url)
    [[^name]] = Repo.query!("SELECT current_database()").rows
    QaBackgroundWorkers.verify_disabled!()
  end

  @spec provision!() :: :ok
  def provision! do
    verify_target!()
    root = System.fetch_env!("RETAINED_PRIVATE_ROOT")
    %{type: :directory, mode: mode} = File.lstat!(root)
    true = Bitwise.band(mode, 0o777) == 0o700
    run = System.fetch_env!("RETAINED_RUN_ID")
    upstream_url = System.fetch_env!("RETAINED_UPSTREAM_URL")
    {:ok, origin} = LocalTarget.upstream_base_url(upstream_url, upstream_url)
    password = Base.url_encode64(:crypto.strong_rand_bytes(32))
    email = "retained-operator@example.com"
    {:ok, _} = Accounts.bootstrap_owner(%{email: email, display_name: "Synthetic operator", password: password, password_confirmation: password})
    provisioned = Provisioner.provision!(run, origin, Journal.prepared(run), & &1, serving_mode: :full)
    operator = CodexRetainedUsageFixtureUI.create_operator!(provisioned.pool.id)
    write_private!(Path.join(root, "fixture.json"), Map.merge(provisioned.journal, Map.merge(operator, %{apiKey: provisioned.raw_key, model: Provisioner.model(), email: email, password: password})))
  end

  @spec create_session!(String.t(), String.t(), node()) :: struct()
  def create_session!(window, mode, owner_node) do
    fixture = fixture!()
    Provisioner.set_serving_override!(fixture, mode)
    {:ok, principal} = Access.authenticate_authorization_header("Bearer " <> fixture["apiKey"])
    {:ok, session} = Websocket.start_codex_session(principal, %{session_header: window, session_header_source: "x-codex-window-id", owner_instance_id: Atom.to_string(owner_node)})
    session
  end

  @spec rows(String.t(), map()) :: [map()]
  def rows(session_id, bindings \\ %{}) do
    from(t in CodexTurn, join: session in CodexSession, on: session.id == t.codex_session_id, join: r in Request, on: r.id == t.request_id, left_join: a in Attempt, on: a.request_id == r.id, left_join: entry in LedgerEntry, on: entry.request_id == r.id and entry.entry_kind == "settlement" and entry.amount_status == "recorded", where: t.codex_session_id == ^session_id, order_by: [asc: t.turn_sequence, asc: a.attempt_number, asc: entry.created_at, asc: entry.id], select: {session, t, r, a, entry})
    |> Repo.all()
    |> Enum.chunk_by(fn {_session, turn, _request, attempt, _entry} -> {turn.id, attempt && attempt.id} end)
    |> Enum.map(fn [{session, turn, request, attempt, _entry} | _] = records ->
      entries = records |> Enum.map(&elem(&1, 4)) |> Enum.reject(&is_nil/1)
      Map.merge(row(turn, request, attempt, entries), verified_binding(session, turn, Map.get(bindings, request.id) || Map.get(bindings, :selector)))
    end)
  end

  @spec observe_client_binding(pid(), String.t(), term()) :: :ok
  def observe_client_binding(coordinator, thread, %{payload: payload, request_id: request_id}) do
    body = CodexPooler.JSON.decode!(payload)
    metadata = get_in(body, ["client_metadata", "x-codex-turn-metadata"])
    canonical = if is_binary(metadata), do: CodexPooler.JSON.decode!(metadata), else: metadata || %{}
    turn = canonical["turn_id"] || get_in(body, ["client_metadata", "turn_id"])
    client_thread = canonical["thread_id"] || thread

    if is_binary(turn) do
      send(coordinator, {:client_binding, thread, request_id, %{clientThreadId: client_thread, clientTurnId: turn}})
    end

    :ok
  end

  def observe_client_binding(_, _, _), do: :ok

  defp verified_binding(_session, _turn, nil), do: %{}

  defp verified_binding(session, turn, binding) do
    scope = WebsocketTurnIdentity.claim_scope(session, binding.clientThreadId)
    {:ok, identity} = WebsocketTurnIdentity.resolve(%{"client_metadata" => %{"turn_id" => binding.clientTurnId}}, scope)
    if identity.semantic_turn_key == turn.semantic_turn_digest, do: binding, else: %{clientBindingMismatch: true}
  end

  @spec lock_request!(String.t()) :: {pid(), pos_integer()}
  def lock_request!(request_id) do
    caller = self()

    pid =
      spawn(fn ->
        monitor = Process.monitor(caller)
        {:ok, conn} = Postgrex.start_link(Keyword.take(Repo.config(), [:hostname, :port, :username, :password, :database]))

        try do
          Postgrex.transaction(
            conn,
            fn transaction ->
              Postgrex.query!(transaction, "SELECT id FROM requests WHERE id=$1 FOR UPDATE", [Ecto.UUID.dump!(request_id)])
              [[backend]] = Postgrex.query!(transaction, "SELECT pg_backend_pid()", []).rows
              send(caller, {:request_locked, self(), backend})

              receive do
                :unlock -> :ok
                {:DOWN, ^monitor, :process, ^caller, _} -> :ok
              after
                60_000 -> :ok
              end
            end,
            timeout: 70_000
          )
        after
          GenServer.stop(conn)
        end
      end)

    receive do
      {:request_locked, ^pid, backend} -> {pid, backend}
    after
      10_000 -> raise "retained_fixture_lock_timeout"
    end
  end

  @spec sql_waits() :: [map()]
  def sql_waits do
    Repo.query!("SELECT pid,state,wait_event_type,wait_event,pg_blocking_pids(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()").rows
    |> Enum.map(fn [pid, state, type, event, blockers] -> %{pid: pid, state: state, waitType: type, waitEvent: event, blockers: blockers} end)
  end

  @spec fixture!() :: map()
  def fixture!, do: System.fetch_env!("RETAINED_PRIVATE_ROOT") |> Path.join("fixture.json") |> File.read!() |> CodexPooler.JSON.decode!()

  @spec write_private!(String.t(), map()) :: :ok
  def write_private!(path, value) do
    {:ok, file} = :file.open(String.to_charlist(path), [:write, :binary, :exclusive])

    try do
      :ok = File.chmod(path, 0o600)
      :ok = :file.write(file, CodexPooler.JSON.encode!(value))
    after
      :file.close(file)
    end
  end

  defp row(turn, request, attempt, entries) do
    settlement = List.first(entries) || %{}
    attempt = attempt || %{}
    %{requestId: request.id, attemptId: Map.get(attempt, :id), turnId: turn.id, replayGeneration: Map.get(attempt, :replay_generation), ownerInstance: get_in(request.request_metadata, ["websocket_owner_forwarding", "owner_instance_id"]), proxyInstance: get_in(request.request_metadata, ["websocket_owner_forwarding", "proxy_instance_id"]), executionInstance: Map.get(attempt, :owner_instance_id), executionBootId: Map.get(attempt, :owner_instance_boot_id), executionId: Map.get(attempt, :owner_execution_id), requestStatus: request.status, attemptStatus: Map.get(attempt, :status), turnStatus: turn.status, errorCode: request.last_error_code, usageSource: Map.get(settlement, :usage_status, "usage_unknown"), inputTokens: measured(settlement, :input_tokens), outputTokens: measured(settlement, :output_tokens), totalTokens: measured(settlement, :total_tokens), servingMode: get_in(request.request_metadata, ["routing", "model_serving_mode"]), effectiveSettlementCount: length(entries), settlementId: Map.get(settlement, :id)}
  end

  defp measured(%{usage_status: "usage_known"} = settlement, field), do: Map.get(settlement, field)
  defp measured(_, _), do: nil
end
