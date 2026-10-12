defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketUsageAttributionTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, DailyRollup, LedgerEntry, Request, RequestLogFact}
  alias CodexPooler.Catalog.PricingSnapshot
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPooler.UnboxedFixture
  alias Ecto.Adapters.SQL.Sandbox

  @detection_timeout 5_000
  @marker "synthetic-attribution-marker"
  @usage %{
    "input_tokens" => 123,
    "input_tokens_details" => %{"cached_tokens" => 17},
    "output_tokens" => 45,
    "output_tokens_details" => %{"reasoning_tokens" => 6},
    "total_tokens" => 168
  }

  for envelope <- [:response, :root] do
    test "native #{envelope} terminal preserves aggregate usage beyond retained body" do
      terminal = terminal_frame(unquote(envelope), @usage)
      {setup, upstream, result, frames} = execute_frames([terminal])
      assert :ok = result
      assert Enum.map(frames, &digest/1) == [digest(terminal)]
      assert length(FakeUpstream.requests(upstream)) == 1
      assert_settlement(setup, "usage_known")
    end
  end

  test "terminal usage overrides earlier progress" do
    progress =
      CodexPooler.JSON.encode!(%{
        "type" => "response.in_progress",
        "response" => %{"usage" => @usage}
      })

    terminal = terminal_frame(:response, nil)
    {setup, upstream, result, _frames} = execute_frames([progress, terminal])
    assert :ok = result
    assert length(FakeUpstream.requests(upstream)) == 1
    assert_settlement(setup, "usage_unknown")
  end

  for usage <- [
        nil,
        [],
        %{"input_tokens" => -1, "output_tokens" => 4},
        %{"input_tokens" => 1, "output_tokens" => 2, "total_tokens" => 9}
      ] do
    test "malformed terminal usage stays unknown: #{inspect(usage)}" do
      {setup, _upstream, result, _frames} =
        execute_frames([terminal_frame(:response, unquote(Macro.escape(usage)))])

      assert :ok = result
      assert_settlement(setup, "usage_unknown")
    end
  end

  test "unrelated nested usage cannot become terminal aggregate usage" do
    response = %{
      "id" => "resp_synthetic_nested_usage",
      "status" => "completed",
      "output" => [%{"usage" => @usage}]
    }

    terminal = CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => response})
    {setup, _upstream, result, _frames} = execute_frames([terminal])
    assert :ok = result
    assert_settlement(setup, "usage_unknown")
  end

  for terminal_type <- ["response.failed", "response.incomplete"] do
    test "#{terminal_type} preserves measured aggregates despite attribution" do
      terminal = terminal_frame(:response, @usage, unquote(terminal_type))
      {setup, upstream, _result, _frames} = execute_frames(model_progress() ++ [terminal])
      assert length(FakeUpstream.requests(upstream)) == 1
      assert_settlement(setup, "usage_known")
      assert [attempt] = Repo.all(from a in Attempt, join: r in Request, on: a.request_id == r.id, where: r.pool_id == ^setup.pool.id)
      assert attempt.served_model == "model-a"
      assert attempt.model_observation["conflict"] == true
      assert attempt.model_observation["first_conflicting_model"] == "model-b"
      assert attempt.model_observation["terminal_status"] == String.replace_prefix(unquote(terminal_type), "response.", "")
      assert attempt.model_observation["terminal_model"] == nil
    end
  end

  test "postvisible websocket interruption persists model disagreements before retained-body finalization" do
    frames = model_progress() ++ [CodexPooler.JSON.encode!(%{"type" => "response.output_text.delta", "delta" => "synthetic output"})]
    {setup, upstream, _result, _delivered} = execute_frames(frames, :interrupted)
    assert FakeUpstream.count(upstream) == 1
    assert [attempt] = Repo.all(from a in Attempt, join: r in Request, on: a.request_id == r.id, where: r.pool_id == ^setup.pool.id)
    assert attempt.served_model == "model-a"
    assert attempt.model_observation["conflict"] == true
    assert attempt.model_observation["first_conflicting_model"] == "model-b"
    assert attempt.model_observation["terminal_status"] == nil
    assert attempt.model_observation["terminal_model"] == nil
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^attempt.request_id and l.entry_kind == "settlement"), :count) == 1
  end

  for mode <- ["full", "lite"], usage_case <- [:measured, :zero, :missing] do
    test "committed #{mode} failed terminal keeps #{usage_case} accounting and failed projections" do
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)

      usage =
        case unquote(usage_case) do
          :measured -> @usage
          :zero -> %{"input_tokens" => 0, "output_tokens" => 0, "total_tokens" => 0}
          :missing -> nil
        end

      terminal = terminal_frame(:response, usage, "response.failed")
      {setup, upstream, _result, delivered} = execute_frames(model_progress() ++ [terminal], :terminal, committed: true, mode: unquote(mode))
      assert FakeUpstream.count(upstream) == 1
      assert digest(List.last(delivered)) == digest(terminal)
      assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
      assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
      assert [settlement] = Repo.all(from l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement")
      turn = Repo.get_by!(CodexTurn, request_id: request.id)
      fact = Repo.get!(RequestLogFact, request.id)
      assert request.status == "failed"
      assert attempt.status == "failed"
      assert turn.status == "failed"
      assert fact.latest_attempt_status == "failed"
      assert fact.latest_settlement_entry_id == settlement.id
      assert fact.latest_settlement_usage_status == settlement.usage_status

      if unquote(usage_case) == :missing do
        assert settlement.usage_status == "usage_unknown"
        assert fact.latest_total_tokens == nil
      else
        assert settlement.usage_status == "usage_known"
        assert (settlement.total_tokens || 0) == usage["total_tokens"]
        assert (fact.latest_total_tokens || 0) == usage["total_tokens"]
      end

      rollups = Repo.all(from r in DailyRollup, where: r.pool_id == ^setup.pool.id and r.dimension_kind == "pool")
      assert [rollup] = rollups
      assert rollup.success_count == 0
      assert rollup.failure_count == 1
      if unquote(usage_case) != :missing, do: assert(rollup.total_tokens == usage["total_tokens"])

      # A distinct PostgreSQL backend sees committed rows, not the caller's sandbox.
      observer = start_supervised!({Postgrex, Repo.config() |> Keyword.take([:username, :password, :hostname, :port, :database])})
      assert %{rows: [["failed", usage_status, tokens]]} = Postgrex.query!(observer, "SELECT r.status, l.usage_status, l.total_tokens FROM requests r JOIN ledger_entries l ON l.request_id = r.id WHERE r.id = $1 AND l.entry_kind = 'settlement'", [Ecto.UUID.dump!(request.id)])
      assert usage_status == settlement.usage_status
      assert tokens == settlement.total_tokens
      assert %{rows: [[writer_backend]]} = Repo.query!("SELECT pg_backend_pid()")
      assert %{rows: [[observer_backend]]} = Postgrex.query!(observer, "SELECT pg_backend_pid()", [])
      refute writer_backend == observer_backend
      IO.puts(CodexPooler.JSON.encode!(%{scenario: "committed_failed_terminal", mode: unquote(mode), usage_case: unquote(usage_case), status: request.status, usage_status: usage_status, total_tokens: tokens, settlements: 1, success_count: rollup.success_count, failure_count: rollup.failure_count, writer_backend: writer_backend, observer_backend: observer_backend}))
    end
  end

  defp model_progress do
    for model <- ["model-a", "model-b"], do: CodexPooler.JSON.encode!(%{"type" => "response.in_progress", "response" => %{"id" => "resp_synthetic_usage_123456", "model" => model}})
  end

  defp terminal_frame(envelope, usage, type \\ "response.completed") do
    # Put attribution after the usage marker so suffix retention cannot see it.
    usage_json = CodexPooler.JSON.encode!(usage)
    attribution = CodexPooler.JSON.encode!(String.duplicate(@marker, 5_000))

    usage_json =
      if is_map(usage),
        do: String.trim_trailing(usage_json, "}") <> ",\"attribution\":" <> attribution <> "}",
        else: usage_json

    response =
      ~s({"id":"resp_synthetic_usage_123456","status":) <>
        CodexPooler.JSON.encode!(String.replace_prefix(type, "response.", "")) <>
        ",\"usage\":" <> usage_json <> "}"

    case envelope do
      :response ->
        "{\"type\":" <> CodexPooler.JSON.encode!(type) <> ",\"response\":" <> response <> "}"

      :root ->
        response
    end
  end

  defp execute_frames(frames, outcome \\ :terminal, options \\ []) do
    mode = if outcome == :interrupted, do: FakeUpstream.websocket_text_frames_then_abrupt_close(frames), else: FakeUpstream.websocket_text_frames(frames)
    upstream = start_upstream(mode)
    setup = if options[:committed], do: committed_gateway_setup(upstream), else: gateway_setup(upstream)
    if mode = options[:mode], do: set_model_serving_mode!(model_serving_scope(), setup, mode)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    {:ok, session} =
      Websocket.start_codex_session(auth, %{accepted_turn_state: Ecto.UUID.generate()})

    observer = self()

    payload =
      CodexPooler.JSON.encode!(%{
        "type" => "response.create",
        "model" => setup.model.exposed_model_id,
        "input" => [],
        "stream" => true
      })

    opts =
      RequestOptions.for_websocket(%{request_id: Ecto.UUID.generate(), codex_session: session})

    result =
      Gateway.execute_websocket_response(auth, payload, opts, fn frame ->
        if CodexPooler.JSON.decode!(frame)["type"] != "codex.response.metadata",
          do: send(observer, {:usage_frame, frame})
      end)

    delivered =
      Enum.map(frames, fn _ ->
        assert_receive {:usage_frame, frame}, @detection_timeout
        frame
      end)

    refute_received {:usage_frame, _}
    {setup, upstream, result, delivered}
  end

  defp committed_gateway_setup(upstream) do
    slug = "usage-attribution-#{Ecto.UUID.generate()}"
    model = "synthetic-#{Ecto.UUID.generate()}"
    missing_pricing_id = Ecto.UUID.generate()

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      if pool = Repo.get_by(Pool, slug: slug) do
        pricing = Repo.get_by(PricingSnapshot, model_identifier: model) || %{id: missing_pricing_id}
        cleanup_unboxed_pool!(%{pool: pool, pricing: pricing})
      end
    end)

    gateway_setup(upstream, pool_slug: slug, upstream_model_id: model)
  end

  defp assert_settlement(setup, status) do
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))

    assert [settlement] =
             Repo.all(
               from(e in LedgerEntry,
                 where: e.request_id == ^request.id and e.entry_kind == "settlement"
               )
             )

    assert request.usage_status == status
    assert attempt.usage_status == status
    assert settlement.usage_status == status
    assert request.transport == "websocket"

    if status == "usage_known" do
      assert Map.take(settlement, [
               :input_tokens,
               :cached_input_tokens,
               :output_tokens,
               :reasoning_tokens,
               :total_tokens
             ]) ==
               %{
                 input_tokens: 123,
                 cached_input_tokens: 17,
                 output_tokens: 45,
                 reasoning_tokens: 6,
                 total_tokens: 168
               }
    end

    metadata = inspect({request.request_metadata, attempt.response_metadata, settlement.details})
    refute metadata =~ @marker
    refute metadata =~ setup.raw_key
  end

  defp digest(frame), do: :crypto.hash(:sha256, frame)
end
