defmodule CodexPooler.Dev.CodexRetainedUsageFixtureUI do
  @moduledoc false
  import Ecto.Query
  alias CodexPooler.{Access, Accounting, Accounts, Repo}
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Accounts.{Scope, User}
  alias CodexPooler.Catalog.Model
  alias CodexPooler.Dev.CodexRetainedUsageFixtureData, as: Data
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment

  @spec create_operator!(String.t()) :: map()
  def create_operator!(pool_id) do
    owner = Enum.find(Accounts.list_operators(), &("instance_owner" in Accounts.roles_for_user(&1)))
    password = Base.url_encode64(:crypto.strong_rand_bytes(32))
    email = "fixture-admin@example.com"
    {:ok, %{user: user}} = Accounts.create_operator(Scope.for_user(owner), %{"email" => email, "display_name" => "Synthetic admin", "role" => "instance_admin", "pool_ids" => [pool_id], "temporary_password" => password, "password_change_required" => false, "send_email" => false})
    %{operatorId: user.id, operatorEmail: email, operatorPassword: password}
  end

  @spec mutate(map()) :: map()
  def mutate(%{"action" => "create", "status" => status}) when status in ["accepted", "in_progress", "unknown", "known", "succeeded"] do
    fixture = Data.fixture!()
    {:ok, auth} = Access.authenticate_authorization_header("Bearer " <> fixture["apiKey"])
    model = Repo.get!(Model, fixture["model_id"])
    assignment = Repo.get!(PoolUpstreamAssignment, fixture["assignment_id"])
    {:ok, session} = Websocket.start_codex_session(auth, %{accepted_turn_state: Ecto.UUID.generate()})
    opts = %{codex_session: session, endpoint: "/backend-api/codex/responses", requested_model: model.exposed_model_id, correlation_id: Ecto.UUID.generate(), transport: "websocket"}
    {:ok, %{request: claim}} = Accounting.claim_websocket_turn(auth, model, opts)

    if status == "accepted" do
      public_row(claim, nil)
    else
      {:ok, %{request: request}} = Accounting.reserve(auth, model, %{}, Map.put(opts, :turn_claim, claim))
      {:ok, attempt} = Accounting.create_attempt(request, assignment, %{transport: "websocket"})
      if status != "in_progress", do: finalize(request, attempt, status)
      public_row(Repo.get!(Request, request.id), attempt)
    end
  end

  def mutate(%{"action" => "transition", "requestId" => id, "status" => status}) when status in ["unknown", "known", "succeeded"] do
    fixture = Data.fixture!()
    request = Repo.get_by!(Request, id: id, pool_id: fixture["pool_id"])
    attempt = Repo.one!(from a in Attempt, where: a.request_id == ^request.id, order_by: [desc: a.attempt_number], limit: 1)
    finalize(request, attempt, status)
    public_row(Repo.get!(Request, id), attempt)
  end

  def mutate(%{"action" => action}) when action in ["revoke_visibility", "restore_visibility"] do
    fixture = Data.fixture!()
    owner = Enum.find(Accounts.list_operators(), &("instance_owner" in Accounts.roles_for_user(&1)))
    operator = Repo.get!(User, fixture["operatorId"])
    pools = if action == "restore_visibility", do: [fixture["pool_id"]], else: []
    {:ok, _} = Accounts.update_operator(Scope.for_user(owner), operator, %{"pool_ids" => pools})
    %{visibility: action}
  end

  @spec arm_query_barrier(pid(), String.t()) :: term()
  def arm_query_barrier(coordinator, request_id) do
    token = {__MODULE__, make_ref()}
    dumped = Ecto.UUID.dump!(request_id)
    once = :atomics.new(1, [])
    :ok = :telemetry.attach(token, [:codex_pooler, :repo, :query], &__MODULE__.hold_query/4, %{coordinator: coordinator, token: token, dumped: dumped, once: once})
    token
  end

  @spec hold_query([atom()], map(), map(), map()) :: :ok
  def hold_query(_event, _measurements, metadata, config) do
    if metadata[:source] == "requests" and config.dumped in (metadata[:params] || []) and live_view_caller?() and :atomics.compare_exchange(config.once, 1, 0, 1) == :ok do
      :telemetry.detach(config.token)
      monitor = Process.monitor(config.coordinator)
      send(config.coordinator, {:ui_query_held, self(), config.token})

      receive do
        {token, :release} when token == config.token -> :ok
        {:DOWN, ^monitor, :process, _, _} -> exit(:shutdown)
      after
        60_000 -> exit(:fixture_query_barrier_timeout)
      end

      Process.demonitor(monitor, [:flush])
    end

    :ok
  end

  defp live_view_caller? do
    Enum.any?(Process.get(:"$callers", []), &live_view_process?/1)
  end

  defp live_view_process?(caller) do
    case Process.info(caller, :initial_call) do
      {:initial_call, {:proc_lib, :init_p, _}} ->
        match?(%{socket: %Phoenix.LiveView.Socket{view: CodexPoolerWeb.Admin.RequestLogsLive}}, :sys.get_state(caller, 1_000))

      _ ->
        false
    end
  catch
    :exit, _ -> false
  end

  defp finalize(request, attempt, "unknown"), do: Accounting.finalize_failure(request, attempt, %{last_error_code: "client_disconnected", response_status_code: 499}) |> successful!()

  defp finalize(request, attempt, "known") do
    finalize(request, attempt, "unknown")
    Accounting.finalize_success_with_disposition(request, attempt, usage()) |> successful!()
  end

  defp finalize(request, attempt, "succeeded"), do: Accounting.finalize_success(request, attempt, usage()) |> successful!()
  defp successful!({:ok, result}), do: result
  defp usage, do: %{status: "usage_known", input_tokens: 100, output_tokens: 20, total_tokens: 120}
  defp public_row(request, attempt), do: %{requestId: request.id, attemptId: attempt && attempt.id, poolId: request.pool_id, status: request.status, admittedAt: DateTime.to_iso8601(request.admitted_at)}
end
