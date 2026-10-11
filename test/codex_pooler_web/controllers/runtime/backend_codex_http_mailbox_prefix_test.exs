defmodule CodexPoolerWeb.Runtime.BackendCodexHttpMailboxPrefixTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, register_unboxed_pool_cleanup!: 1, native_text_input: 1, start_public_endpoint!: 0, start_upstream: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]
  alias CodexPooler.Accounting.{Attempt, ClientRetry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.{NativeHttpTurnIdentity, RequestOptions}
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Runtime.Finalization.{Metadata, Streaming}
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionRegistry, ExecutionTerminalProof, ExecutionTerminalProofs}
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox
  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @budget 15_000

  @tag mailbox_observation_contract: true
  test "a transport resolved before the peer reset can reject the next send with einval" do
    parent = self()
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, {_, port}} = :inet.sockname(listener)

    writer =
      Task.Supervisor.async_nolink(start_supervised!(Task.Supervisor), fn ->
        {:ok, socket} = :gen_tcp.accept(listener, @budget)
        {:ok, transport} = :inet_db.lookup_socket(socket)
        :ok = :gen_tcp.send(socket, "synthetic")
        :ok = :inet.setopts(socket, active: :once)
        send(parent, :reset_writer_ready)

        receive do
          {:tcp_closed, ^socket} -> :ok
          {:tcp_error, ^socket, reason} -> assert reason in [:closed, :econnreset]
        after
          @budget -> flunk("the owned writer did not observe the peer reset")
        end

        monitor = :erlang.monitor(:port, socket)
        assert_receive {:DOWN, ^monitor, :port, ^socket, _}, @budget
        # Exercise the real transport selected by gen_tcp before its port closes.
        assert {:error, reason} = transport.send(socket, "synthetic")
        reason
      end)

    writer_monitor = Process.monitor(writer.pid)
    {:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], @budget)
    on_exit(fn -> :gen_tcp.close(client) end)
    assert {:ok, "synthetic"} = :gen_tcp.recv(client, 9, @budget)
    assert_receive :reset_writer_ready, @budget
    :ok = :inet.setopts(client, linger: {true, 0})
    :ok = :gen_tcp.close(client)
    reason = Task.await(writer, @budget)
    assert_receive {:DOWN, ^writer_monitor, :process, _, :normal}, @budget
    assert reason == :einval
    assert_failed_mailbox_write!(reason)
  end

  test "downstream socket closure codes are interruptions while other write errors remain failures" do
    for reason <- [:closed, :econnreset, :enotconn, :epipe, :einval] do
      assert_failed_mailbox_write!(reason)
    end

    for reason <- [:timeout, {:timeout, :send}, :emsgsize, :visible_output_unavailable, :unexpected_write_error] do
      assert Streaming.error_code({:chunk, reason}) == "downstream_stream_error"
      refute Metadata.safe_reason({:chunk, reason}) == "client disconnected while writing downstream stream"
    end

    refute Streaming.error_code(:einval) == "client_disconnected"
    refute Streaming.error_code({:upstream_stream_interrupted, :einval}) == "client_disconnected"
  end

  # A comprehension expands and compiles a test's body once per generated test, so a loop that generates more than a few tests keeps
  # the scenario in a private function below it and each generated test is one call.
  for mode <- ["full", "lite"], {count, partial_tool?} <- [{1, false}, {2, false}, {1, true}], role <- [:opening, :local_summary, :remote_resume], delivery <- [:cut, :delivered] do
    @tag mode: mode, count: count, role: role, delivery: delivery, partial_tool?: partial_tool?
    test "#{mode} #{role} #{delivery} partial tool #{partial_tool?} retains first of #{count} coalesced items", context do
      assert_mailbox_prefix_retains_first_coalesced_item!(context)
    end
  end

  for mode <- ["full", "lite"] do
    @tag mode: mode, count: 2, role: :remote_resume, delivery: :cut, partial_tool?: false, cut_after_item: 2
    test "#{mode} retains the first coalesced item when the client cuts after both writes", context do
      assert_mailbox_prefix_retains_first_coalesced_item!(context)
    end

    @tag mode: mode, count: 2, role: :remote_resume, delivery: :cut, partial_tool?: false, cut_after_item: 0
    test "#{mode} records no mailbox prefix when the first output write fails", context do
      assert_mailbox_prefix_retains_first_coalesced_item!(context)
    end
  end

  # Reason: the body of a generated test; its branches select the matrix case.
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp assert_mailbox_prefix_retains_first_coalesced_item!(context) do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    outputs = Enum.map(1..context.count, fn n -> %{"type" => "reasoning", "id" => "rs_synthetic_#{n}", "summary" => [], "encrypted_content" => "synthetic_#{n}"} end)
    tool = %{"type" => "function_call", "id" => "fc_synthetic", "call_id" => "synthetic-call", "name" => "synthetic_tool", "arguments" => "{}"}
    chunk = Enum.map_join(outputs, fn item -> event(%{"type" => "response.output_item.done", "item" => item}) end)
    chunk = if context.partial_tool?, do: chunk <> event(%{"type" => "response.output_item.added", "output_index" => context.count, "item" => tool}), else: chunk
    gate = make_ref()
    tail = event(%{"type" => "response.reasoning_text.delta", "delta" => String.duplicate("synthetic", 10_000)})
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
    # provenance: synthetic_adversarial; coalesced output followed by a held cancellation-detection tail
    terminal = FakeUpstream.sse_stream([{"response.completed", completed}])
    predecessor_response = if context.delivery == :cut, do: {:gated_terminal_sse, [chunk], [tail], self(), gate}, else: FakeUpstream.raw_response(chunk <> event(completed), headers: [{"content-type", "text/event-stream"}])
    prelude = if context.role == :local_summary, do: [terminal], else: []
    upstream = start_upstream(FakeUpstream.strict_sequence(prelude ++ [predecessor_response, terminal]))
    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    set_model_serving_mode!(model_serving_scope(), setup, context.mode)
    setup = Map.put(setup, :serving_mode, context.mode)
    port = start_public_endpoint!()
    thread = Ecto.UUID.generate()
    base_input = native_text_input("synthetic")

    # provenance: local-summary/window shape from backend_codex_http_window_advance_test.exs;
    # the served window-0 opener forces the real window-1 steered claim path.
    input =
      case context.role do
        :opening -> base_input
        :local_summary -> base_input ++ native_text_input("synthetic local summary")
        :remote_resume -> base_input ++ [%{"type" => "compaction", "encrypted_content" => "synthetic_compaction"}]
      end

    payload = payload(setup, thread, input, if(context.role == :opening, do: 0, else: 1))

    prelude_id =
      if context.role == :local_summary do
        assert {200, _body} = post(port, setup, payload(setup, thread, base_input, 0), thread)
        prior = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
        assert prior.status == "succeeded"
        prior.id
      end

    retained =
      if context.delivery == :cut do
        hold = %{ref: make_ref(), after_item: Map.get(context, :cut_after_item, 1)}
        {conn, ref} = start_request(port, setup, payload, thread, true, hold)
        {conn, retained} = if hold.after_item == 0, do: until_headers(conn, ref), else: until_item(conn, ref, "")
        assert_receive {:mailbox_prefix_write_held, hold_ref, writer}, @budget
        assert hold_ref == hold.ref
        monitor = Process.monitor(writer)
        assert_receive {:fake_upstream_gate, :before_terminal, handler, ^gate}, @budget
        # Reset the real connection so the held upstream tail observes cancellation.
        :ok = :inet.setopts(Mint.HTTP.get_socket(conn), linger: {true, 0})
        Mint.HTTP.close(conn)
        assert_receive {:mailbox_prefix_client_closed, ^hold_ref, ^writer, reason}, @budget
        assert reason in [:closed, :econnreset]
        send(handler, {:fake_upstream_release_gate, gate})
        assert_receive {:DOWN, ^monitor, :process, ^writer, _reason}, @budget
        assert_mailbox_write_schedule!(hold, context)
        retained
      else
        assert {200, _body} = post(port, setup, payload, thread)
        hd(outputs)
      end

    retained = if retained, do: Map.put(retained, "content", nil)
    first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)

    if prelude_id do
      prior = Repo.get!(Request, prelude_id)
      assert first.id != prelude_id
      assert first.request_metadata["native_http_claim_arm"] == "steered_continuation"
      assert first.request_metadata["codex_session_id"] == prior.request_metadata["codex_session_id"]
      refute first.correlation_id == prior.correlation_id
    end

    attempt = Repo.get_by!(Attempt, request_id: first.id)
    turn = Repo.get_by!(CodexTurn, request_id: first.id)
    recorded = get_in(attempt.response_metadata, ["native_http_resume_progress", "output_item_done_count"])
    prefix = attempt.response_metadata["native_http_mailbox_prefix"]

    if Map.get(context, :cut_after_item) == 0 do
      assert recorded == 0
      assert prefix == %{}
      assert get_in(attempt.response_metadata, ["downstream_delivery", "frames_after_visible"]) == 0
      assert {first.status, first.last_error_code, turn.status} == {"failed", "client_disconnected", "interrupted"}
      assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^first.id), :count) == 1
      assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^first.id and l.entry_kind == "settlement"), :count) == 1
      assert FakeUpstream.count(upstream) == 1
    else
      assert prefix["output_item_done_count"] == context.count
      assert length(prefix["item_digests"]) == context.count
      if context.delivery == :cut, do: assert(get_in(attempt.response_metadata, ["downstream_delivery", "frames_after_visible"]) == Map.get(context, :cut_after_item, 1))

      if context.delivery == :cut do
        assert {first.status, first.last_error_code, turn.status} == {"failed", "client_disconnected", "interrupted"}
      else
        assert {first.status, turn.status} == {"succeeded", "succeeded"}
      end

      assert recorded == context.count
      mailbox = %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
      successor = Map.put(payload, "input", input ++ [retained, mailbox])
      bad = put_in(successor, ["input", Access.at(-2), "encrypted_content"], "changed_synthetic")
      {bad_status, _} = post(port, setup, bad, thread)
      assert bad_status == 409
      {call_without_output_status, _} = post(port, setup, Map.put(payload, "input", input ++ [retained, tool, mailbox]), thread)
      assert call_without_output_status == 409

      if context.count == 2 do
        {nonprefix_status, _} = post(port, setup, Map.put(payload, "input", input ++ [List.last(outputs), mailbox]), thread)
        assert nonprefix_status == 409
        {reordered_status, _} = post(port, setup, Map.put(payload, "input", input ++ Enum.reverse(outputs) ++ [mailbox]), thread)
        assert reordered_status == 409
      end

      {status, _} = post(port, setup, successor, thread)
      assert status == 200
      requests = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at])
      assert length(requests) == if(prelude_id, do: 3, else: 2)
      [predecessor, admitted] = Enum.take(requests, -2)
      assert predecessor.id == first.id
      assert admitted.request_metadata["client_resend"]["predecessor_request_id"] == first.id
      assert Repo.aggregate(from(l in CodexPooler.Accounting.RequestClientRetryLink, where: l.predecessor_request_id == ^first.id and l.successor_request_id == ^admitted.id), :count) == 1

      for request <- requests do
        assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
        assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      end

      assert FakeUpstream.count(upstream) == if(prelude_id, do: 3, else: 2)
    end
  end

  for mode <- ["full", "lite"] do
    @tag mode: mode, upstream_error_http_mailbox: true
    test "#{mode} completed HTTP item survives an upstream stream error for an addressed mailbox continuation", context do
      assert_completed_http_item_survives_upstream_stream_error!(context)
    end
  end

  # Reason: the body of a generated test; its branches select the matrix case.
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp assert_completed_http_item_survives_upstream_stream_error!(context) do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    output = %{"type" => "reasoning", "id" => "rs_synthetic_interrupted", "summary" => [], "encrypted_content" => "synthetic_interrupted"}
    done = %{"type" => "response.output_item.done", "item" => output}
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_successor", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
    successor_gate = make_ref()
    successor_response = FakeUpstream.gated_terminal_sse_stream([], {"response.completed", completed}, notify: self(), release_ref: successor_gate)
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.abrupt_close_mid_stream([event(done)]), successor_response]))
    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    set_model_serving_mode!(model_serving_scope(), setup, context.mode)
    setup = Map.put(setup, :serving_mode, context.mode)
    port = start_public_endpoint!()
    thread = Ecto.UUID.generate()
    input = native_text_input("synthetic interrupted stream")
    original = payload(setup, thread, input, 0)
    # The client reads the entire public stream before closing; only the
    # upstream socket is killed by the normal FakeUpstream failure mode.
    assert {200, wire} = post(port, setup, original, thread)
    [first_event | _] = String.split(wire, "\n\n", trim: true)
    [json] = for "data: " <> value <- String.split(first_event, "\n"), do: value
    assert %{"type" => "response.output_item.done", "item" => retained} = CodexPooler.JSON.decode!(json)
    first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
    attempt = Repo.get_by!(Attempt, request_id: first.id)
    turn = Repo.get_by!(CodexTurn, request_id: first.id)
    assert {first.status, first.last_error_code} == {"failed", "upstream_stream_error"}
    assert {attempt.status, attempt.network_error_code, attempt.replay_generation} == {"failed", "upstream_stream_error", 0}
    assert turn.request_id == first.id
    assert turn.final_attempt_id == attempt.id
    assert turn.status == "failed"
    assert first.completed_at && attempt.completed_at && turn.completed_at
    assert ClientRetry.original_witness_eligible?(first)
    assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^first.id and l.entry_kind == "settlement"), :count) == 1
    assert get_in(attempt.response_metadata, ["native_http_resume_progress", "output_item_done_count"]) == 1
    prefix = attempt.response_metadata["native_http_mailbox_prefix"]
    assert prefix["output_item_done_count"] == 1
    assert length(prefix["item_digests"]) == 1
    proof = await_scoped_execution_proof!(attempt, System.monotonic_time(:millisecond) + @budget)
    mailbox = %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
    successor = Map.put(original, "input", input ++ [Map.put(retained, "content", nil), mailbox])
    assert_http_stream_mailbox_fences!(turn, first, attempt, successor)

    for invalid <- [put_in(successor, ["input", Access.at(-2), "encrypted_content"], "foreign_synthetic"), Map.put(successor, "input", input ++ [mailbox]), Map.put(successor, "previous_response_id", "resp_synthetic_unavailable")] do
      {invalid_status, _body} = post(port, setup, invalid, thread)
      refute invalid_status == 200
      assert FakeUpstream.count(upstream) == 1
    end

    %{rows: [[db_now]]} = Repo.query!("SELECT clock_timestamp()")
    expired_at = DateTime.add(db_now, -31, :second)

    try do
      Repo.update_all(from(r in Request, where: r.id == ^first.id), set: [completed_at: expired_at])
      Repo.update_all(from(a in Attempt, where: a.id == ^attempt.id), set: [completed_at: expired_at])
      Repo.update_all(from(t in CodexTurn, where: t.id == ^turn.id), set: [completed_at: expired_at])
      {expired_status, _body} = post(port, setup, successor, thread)
      assert expired_status == 409
      assert FakeUpstream.count(upstream) == 1
    after
      Repo.update_all(from(r in Request, where: r.id == ^first.id), set: [completed_at: first.completed_at])
      Repo.update_all(from(a in Attempt, where: a.id == ^attempt.id), set: [completed_at: attempt.completed_at])
      Repo.update_all(from(t in CodexTurn, where: t.id == ^turn.id), set: [completed_at: turn.completed_at])
    end

    pending = Task.async(fn -> ExUnit.CaptureLog.with_log(fn -> post(port, setup, successor, thread, false) end) end)
    pending_monitor = Process.monitor(pending.pid)
    assert_receive {:fake_upstream_gate, :before_terminal, successor_handler, ^successor_gate}, @budget
    on_exit(fn -> send(successor_handler, {:fake_upstream_release_gate, successor_gate}) end)

    {{status, _body}, log} =
      try do
        active = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [desc: r.admitted_at], limit: 1)
        assert active.id != first.id
        assert active.status == "in_progress"
        assert is_nil(active.completed_at)
        assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 2
        assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^active.id), :count) == 1
        assert Repo.aggregate(from(l in CodexPooler.Accounting.RequestClientRetryLink, where: l.predecessor_request_id == ^first.id and l.successor_request_id == ^active.id), :count) == 1
        {duplicate_status, _body} = post(port, setup, successor, thread)
        assert duplicate_status == 409
        assert FakeUpstream.count(upstream) == 2
        assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 2
        assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^active.id), :count) == 1
        assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^active.id and l.entry_kind == "settlement"), :count) == 0
        if System.get_env("HTTP_MAILBOX_REGRESSION_DIAGNOSTICS") == "1", do: IO.puts(CodexPooler.JSON.encode!(%{scenario: "active_successor_duplicate", active_request_id: active.id, active_status: active.status, duplicate_status: duplicate_status, dispatches: FakeUpstream.count(upstream), request_count: Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count)}))
        send(successor_handler, {:fake_upstream_release_gate, successor_gate})
        result = Task.await(pending, @budget)
        assert_receive {:DOWN, ^pending_monitor, :process, _pid, :normal}, @budget
        result
      after
        send(successor_handler, {:fake_upstream_release_gate, successor_gate})
        if Process.alive?(pending.pid), do: Task.shutdown(pending, :brutal_kill)
      end

    if System.get_env("HTTP_MAILBOX_REGRESSION_DIAGNOSTICS") == "1" do
      IO.puts(CodexPooler.JSON.encode!(%{mode: context.mode, request_id: first.id, attempt_id: attempt.id, turn_id: turn.id, request_status: first.status, request_error: first.last_error_code, attempt_status: attempt.status, attempt_error: attempt.network_error_code, generation: attempt.replay_generation, turn_status: turn.status, first_visible_output: not is_nil(turn.first_visible_output_at), completed_items: prefix["output_item_done_count"], prefix_digest_count: length(prefix["item_digests"]), receipt_outcome: get_in(attempt.response_metadata, ["downstream_delivery", "outcome"]), receipt_terminal_class: get_in(attempt.response_metadata, ["downstream_delivery", "terminal_class"]), execution_terminal: ExecutionTerminalProofs.terminal?(attempt), proof_kind: proof.end_kind, proof_interruption: proof.interruption_code, successor_status: status, rejected_at_settlement: String.contains?(log, "mailbox_check=settlement"), dispatches: FakeUpstream.count(upstream)}))
    end

    assert status == 200
    requests = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at])
    assert length(requests) == 2
    assert List.last(requests).request_metadata["client_resend"]["predecessor_request_id"] == first.id
    assert FakeUpstream.count(upstream) == 2

    for request <- requests do
      assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
      assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
    end
  end

  defp assert_http_stream_mailbox_fences!(turn, request, attempt, successor) do
    session = Repo.get!(CodexSession, turn.codex_session_id)
    options = RequestOptions.build(%{transport: "http_sse", codex_session: session, api_key_runtime_epoch: request.native_client_retry_auth_epoch}, @path, %{})
    assert {:ok, claim} = NativeHttpTurnIdentity.request_claim(options, successor)
    witness = claim.native_client_retry_witness
    assert [_candidate] = witness.mailbox
    assert %{stage: :verified} = ClientRetry.mailbox_check(turn, request, attempt, witness, nil, :same_session)

    absent = %{attempt | owner_execution_id: Ecto.UUID.generate()}
    foreign = %{attempt | owner_instance_boot_id: "foreign-synthetic-boot"}

    for invalid <- [absent, foreign, %{attempt | replay_generation: 1}, %{attempt | status: "in_progress"}, %{attempt | completed_at: nil}] do
      assert %{stage: :settlement} = ClientRetry.mailbox_check(turn, request, invalid, witness, nil, :same_session)
    end

    assert %{stage: :settlement} = ClientRetry.mailbox_check(%{turn | final_attempt_id: Ecto.UUID.generate()}, request, attempt, witness, nil, :same_session)
    assert %{stage: :authorization} = ClientRetry.mailbox_check(turn, request, attempt, %{witness | auth_epoch: witness.auth_epoch + 1}, nil, :same_session)
    assert %{stage: :session} = ClientRetry.mailbox_check_for_session(turn, request, attempt, witness, nil, Ecto.UUID.generate(), %{pool_id: request.pool_id, api_key_id: request.api_key_id})

    for metadata <- [Map.drop(attempt.response_metadata, ["native_http_resume_progress", "native_http_mailbox_prefix"]), Map.put(Map.delete(attempt.response_metadata, "native_http_resume_progress"), "native_http_mailbox_prefix", %{"version" => 1, "output_item_done_count" => 0, "item_digests" => []}), Map.put(Map.delete(attempt.response_metadata, "native_http_resume_progress"), "native_http_mailbox_prefix", %{"version" => 1, "output_item_done_count" => 1, "item_digests" => ["000000000000"]})] do
      assert %{stage: :output_prefix} = ClientRetry.mailbox_check(turn, request, %{attempt | response_metadata: metadata}, witness, nil, :same_session)
    end

    ended = %{witness | mailbox: Enum.map(witness.mailbox, &Map.put(&1, :current?, false))}
    assert %{stage: :ending} = ClientRetry.mailbox_check(turn, request, attempt, ended, nil, :same_session)
    assert {:ok, foreign_model} = NativeHttpTurnIdentity.request_claim(options, Map.put(successor, "model", "foreign-synthetic-model"))
    assert [_candidate] = foreign_model.native_client_retry_witness.mailbox
    refute ClientRetry.mailbox_check(turn, request, attempt, foreign_model.native_client_retry_witness, nil, :same_session).stage == :verified
    if System.get_env("HTTP_MAILBOX_REGRESSION_DIAGNOSTICS") == "1", do: IO.puts(CodexPooler.JSON.encode!(%{scenario: "recognized_http_upstream_error_mailbox_fences", candidate_verified: true, checks: ["absent_proof", "foreign_proof_boot", "generation_one", "active_attempt", "unfinished_attempt", "foreign_final_attempt", "auth_epoch", "foreign_session", "missing_prefix", "zero_prefix", "foreign_prefix", "ending", "foreign_model"]}))
  end

  defp await_scoped_execution_proof!(attempt, deadline) do
    registry_node = Enum.find([node() | Node.list(:connected)], &(Atom.to_string(&1) == attempt.owner_instance_id))
    assert registry_node, "the actual HTTP executor's registry node is unavailable"
    registry = {ExecutionRegistry, registry_node}

    case ExecutionRegistry.pending_proofs([attempt.owner_execution_id], registry) do
      [proof] ->
        fields = [:owner_execution_id, :owner_instance_id, :owner_instance_boot_id, :owner_process_id]
        assert Map.take(proof, fields) == Map.take(attempt, fields)
        assert proof.end_kind in ["completed", "process_down"]
        assert ExecutionIdentity.status(attempt) == :dead

        CodexPooler.UnboxedFixture.register_unboxed_cleanup!(fn ->
          Repo.delete_all(from p in ExecutionTerminalProof, where: p.execution_id == ^attempt.owner_execution_id and p.owner_instance_id == ^attempt.owner_instance_id and p.owner_instance_boot_id == ^attempt.owner_instance_boot_id and p.owner_process_id == ^attempt.owner_process_id)
          :ok = ExecutionRegistry.acknowledge([attempt.owner_execution_id], registry)
        end)

        assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
        assert ExecutionTerminalProofs.terminal?(attempt)
        assert :ok = ExecutionRegistry.acknowledge([attempt.owner_execution_id], registry)
        scoped_execution_proof!(attempt)

      [] ->
        if ExecutionTerminalProofs.terminal?(attempt) do
          scoped_execution_proof!(attempt)
        else
          remaining = deadline - System.monotonic_time(:millisecond)
          assert remaining > 0, "the actual HTTP executor's normal terminal proof did not become available"

          receive do
          after
            min(5, remaining) -> await_scoped_execution_proof!(attempt, deadline)
          end
        end

      :unknown ->
        flunk("the actual HTTP executor's registry is unavailable")
    end
  end

  defp scoped_execution_proof!(attempt) do
    Repo.one!(from p in ExecutionTerminalProof, where: p.execution_id == ^attempt.owner_execution_id and p.owner_instance_id == ^attempt.owner_instance_id and p.owner_instance_boot_id == ^attempt.owner_instance_boot_id and p.owner_process_id == ^attempt.owner_process_id)
  end

  defp payload(setup, thread, input, window) do
    %{"model" => setup.model.exposed_model_id, "input" => input, "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:#{window}", "window_number" => window})}}
  end

  defp event(data), do: "event: #{data["type"]}\ndata: " <> CodexPooler.JSON.encode!(data) <> "\n\n"

  defp start_request(port, setup, payload, thread, register_exit?, hold) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    if register_exit?, do: on_exit(fn -> Mint.HTTP.close(conn) end)
    metadata = payload["client_metadata"]["x-codex-turn-metadata"]
    window = CodexPooler.JSON.decode!(metadata)["window_number"]
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:#{window}"}, {"x-codex-turn-metadata", metadata}, {"originator", "codex_cli_rs"}]
    headers = if setup.serving_mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    if hold, do: install_mailbox_write_hold!(conn, port, hold)
    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, CodexPooler.JSON.encode!(payload))
    {conn, ref}
  end

  defp until_item(conn, ref, acc) do
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    bytes =
      Enum.reduce(responses, acc, fn
        {:data, ^ref, data}, a -> a <> data
        _, a -> a
      end)

    if String.contains?(bytes, "\n\n") do
      [block | _ignored_after_preemption] = String.split(bytes, "\n\n")
      [data] = for "data: " <> json <- String.split(block, "\n"), do: json
      assert %{"type" => "response.output_item.done", "item" => item} = CodexPooler.JSON.decode!(data)
      {conn, item}
    else
      until_item(conn, ref, bytes)
    end
  end

  defp post(port, setup, payload, thread, register_exit? \\ true) do
    {conn, ref} = start_request(port, setup, payload, thread, register_exit?, nil)

    try do
      all(conn, ref, nil, "")
    after
      Mint.HTTP.close(conn)
    end
  end

  defp all(conn, ref, status, body) do
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    {status, body, done} =
      Enum.reduce(responses, {status, body, false}, fn
        {:status, ^ref, s}, {_, b, d} -> {s, b, d}
        {:data, ^ref, data}, {s, b, d} -> {s, b <> data, d}
        {:done, ^ref}, {s, b, _} -> {s, b, true}
        _, a -> a
      end)

    if done, do: {status, body}, else: all(conn, ref, status, body)
  end

  defp await_latest_settled(setup, deadline) do
    row = Repo.one(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [desc: r.admitted_at], limit: 1)

    cond do
      row && row.completed_at ->
        row

      System.monotonic_time(:millisecond) > deadline ->
        flunk("request never finalized")

      true ->
        Process.sleep(10)
        await_latest_settled(setup, deadline)
    end
  end

  defp until_headers(conn, ref) do
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)
    refute Enum.any?(responses, &match?({:data, ^ref, data} when data != "", &1))

    if Enum.any?(responses, &match?({:status, ^ref, 200}, &1)),
      do: {conn, nil},
      else: until_headers(conn, ref)
  end

  defp install_mailbox_write_hold!(conn, port, hold) do
    {:ok, {_address, client_port}} = :inet.sockname(Mint.HTTP.get_socket(conn))
    id = {__MODULE__, System.unique_integer([:positive])}
    config = Map.merge(hold, %{test: self(), public_port: port, client_port: client_port})
    on_exit(fn -> :telemetry.detach(id) end)
    :ok = :telemetry.attach_many(id, [[:thousand_island, :connection, :send], [:thousand_island, :connection, :send_error]], &__MODULE__.observe_mailbox_write/4, config)
    watch = {CodexPoolerWeb.WebsocketDownstreamWriteWatch, :send}
    assert [^watch, ^id] = for(%{id: handler} <- :telemetry.list_handlers([:thousand_island, :connection, :send]), handler in [watch, id], do: handler)
  end

  # The hold runs after the real socket send succeeded. Receiving the client's
  # RST on this exact port makes the next part's failure causal, without a sleep.
  @doc false
  def observe_mailbox_write(event, %{data: data} = measurements, _metadata, config) do
    socket = mailbox_writer_socket(config.public_port)
    owner_key = {__MODULE__, config.ref}
    owned? = Process.get(owner_key, false) or (is_port(socket) and match?({:ok, {_, port}} when port == config.client_port, :inet.peername(socket)))

    if owned? do
      Process.put(owner_key, true)
      bytes = IO.iodata_to_binary(data)
      item = mailbox_written_item(bytes)
      kind = List.last(event)
      if item, do: send(config.test, {:mailbox_prefix_write, config.ref, kind, item, measurements[:error]})

      if kind == :send and item == config.after_item do
        monitor = Process.monitor(config.test)
        :ok = :inet.setopts(socket, active: :once)
        send(config.test, {:mailbox_prefix_write_held, config.ref, self()})

        receive do
          {:tcp_error, ^socket, reason} = closed ->
            send(config.test, {:mailbox_prefix_client_closed, config.ref, self(), reason})
            send(self(), closed)

          {:tcp_closed, ^socket} = closed ->
            send(config.test, {:mailbox_prefix_client_closed, config.ref, self(), :closed})
            send(self(), closed)

          {:DOWN, ^monitor, :process, _test, _reason} ->
            :ok
        after
          @budget -> send(config.test, {:mailbox_prefix_client_closed, config.ref, self(), :not_observed})
        end

        Process.demonitor(monitor, [:flush])
      end
    end

    :ok
  end

  defp mailbox_writer_socket(public_port) do
    {:links, links} = Process.info(self(), :links)

    Enum.find(links, fn link ->
      is_port(link) and Port.info(link, :name) == {:name, ~c"tcp_inet"} and match?({:ok, {_, port}} when port == public_port, :inet.sockname(link))
    end)
  end

  defp mailbox_written_item(bytes) do
    cond do
      String.starts_with?(bytes, "HTTP/1.1 200") -> 0
      String.contains?(bytes, "rs_synthetic_1") -> 1
      String.contains?(bytes, "rs_synthetic_2") -> 2
      String.contains?(bytes, "fc_synthetic") -> :partial_tool
      true -> nil
    end
  end

  defp assert_mailbox_write_schedule!(%{ref: ref, after_item: after_item}, context) do
    assert_receive {:mailbox_prefix_write, ^ref, :send, 0, nil}, @budget

    for item <- 1..after_item//1 do
      assert_receive {:mailbox_prefix_write, ^ref, :send, ^item, nil}, @budget
    end

    failed_item =
      cond do
        after_item == 0 -> 1
        after_item == 1 and context.count == 2 -> 2
        after_item == 1 and context.partial_tool? -> :partial_tool
        true -> nil
      end

    if failed_item do
      assert_receive {:mailbox_prefix_write, ^ref, :send_error, ^failed_item, reason}, @budget
      assert_failed_mailbox_write!(reason)
    end

    refute_received {:mailbox_prefix_write, ^ref, :send, _item, _error}
  end

  defp assert_failed_mailbox_write!(reason) do
    assert reason in [:closed, :econnreset, :enotconn, :epipe, :einval]
    assert Streaming.error_code({:chunk, reason}) == "client_disconnected", "misclassified downstream socket error: #{inspect(reason)}"
    assert Metadata.safe_reason({:chunk, reason}) == "client disconnected while writing downstream stream"
  end
end
