defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketLocalCompactionTest do
  # A Codex client whose provider is not named `OpenAI` compacts locally
  # (findings#282): when the turn's context passes the auto-compaction limit
  # mid-turn, the released client (rust-v0.158.0, `core/src/compact.rs`) opens
  # a new websocket and sends its own summarization request, declared
  # `request_kind: "compaction"` with `implementation: "responses"` and the
  # turn's own `turn_id` (on the frame and in the upgrade's turn metadata),
  # then resumes the turn on its first websocket with the summary as full
  # history in the next context window. The Pooler used to read the
  # summarization request as a resend of the turn's opening request and refused
  # it `409 duplicate_turn`, six times, and the turn failed.
  #
  # From a thread's second local compaction on, in the same turn or in a later
  # one, the resume itself was refused the same way (findings#270 row 270-286):
  # a local compaction leaves no compaction item, so the resume stood no
  # further along its turn than the previous resume or the next turn's opener.
  # Its context window now stands in for the missing compaction point.
  #
  # Frames: the shape `codex exec` 0.158.0 sends with provider `name = "Codex
  # Pooler"` and `model_auto_compact_token_limit = 200` against a local fake
  # provider (field set, turn metadata, upgrade headers, input item kinds and
  # order, the Lite shape under a Lite catalog), with synthetic text and ids.
  #
  # Topology: the real public listener, native websocket, one node with owner
  # forwarding off (each socket's own upstream session) and on (the session's
  # owner on this node), the Pool's serving mode forced to Full and to Lite; a
  # second VM owning the session (forwarding on, the Pool's default mode, which
  # serves this fixture's model Full). FakeUpstream.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [enter_peer_owner_topology!: 0, start_peer_window_owner!: 2]

  alias CodexPooler.Accounting.{Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence
  alias CodexPoolerWeb.WebsocketConnectionLogger

  @moduletag capture_log: true

  # Detection budget for a frame, a barrier, a settlement or a row the test
  # only observes.
  @detection_timeout_ms 15_000

  @turn_path "/backend-api/codex/responses"
  @native_terminal_types ["response.completed", "response.failed", "response.incomplete", "error"]
  @installation_id "00000000-0000-4000-8000-000000000282"
  @call_id "call_local_compaction_sample"
  @compaction_prompt "You are performing a CONTEXT CHECKPOINT COMPACTION. Create a handoff summary (synthetic)."
  @summary_text "Another language model started to solve this problem and produced a summary (synthetic)."
  @peer_thread "019a0000-0000-7000-8000-00000000f282"
  @next_task "local compaction sample: the next task"

  for forwarding <- [:off, :on], mode <- ["full", "lite"] do
    @tag forwarding: forwarding, serving_mode: mode
    test "a #{mode} local compaction with owner forwarding #{forwarding} is served and the turn resumes", ctx do
      put_owner_forwarding!(ctx.forwarding == :on)
      summary_hold = make_ref()
      upstream = start_upstream(local_compaction_upstream(summary_hold))
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, ctx.serving_mode)
      assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
      {_server, port} = start_public_endpoint_with_server!()

      assert_local_compaction_served!(upstream, setup, port, new_turn(setup, ctx.serving_mode), summary_hold, owner_reader(ctx.forwarding))
    end
  end

  # The released client 0.159.0 against a provider that answers the first
  # resume with one more tool round (`same_turn`), or that ends the turn and
  # compacts again in the thread's next turn (`next_turn`): each summary on a
  # websocket of its own, each resume on the turn's first websocket, on the
  # next window. Before, the second resume was refused `duplicate_turn` six
  # times, then the client fell back to HTTPS and failed the turn.
  for forwarding <- [:off, :on], mode <- ["full", "lite"], shape <- [:same_turn, :next_turn] do
    @tag forwarding: forwarding, serving_mode: mode, shape: shape
    test "a #{mode} thread's resume after its second local compaction (#{shape}) with owner forwarding #{forwarding} is served", ctx do
      put_owner_forwarding!(ctx.forwarding == :on)
      upstream = start_upstream(second_compaction_upstream(ctx.shape))
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, ctx.serving_mode)
      assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
      {_server, port} = start_public_endpoint_with_server!()
      turn = new_turn(setup, ctx.serving_mode)

      first = connect!(port, setup, turn, "prewarm")
      first = prewarm!(first, turn)
      {first, resume} = serve_second_compaction!(ctx.shape, first, port, setup, turn)

      # One row per request, each served once under a claim of its own: the
      # openers under their turn's claim, the summaries under compaction
      # claims, each resume under the claim of a later request of its turn.
      rows = pool_requests(setup)
      assert Enum.map(rows, & &1.status) == List.duplicate("succeeded", length(rows))
      assert Enum.map(rows, &(&1.correlation_id |> String.split(":") |> hd())) == expected_claim_classes(ctx.shape)
      assert Enum.uniq_by(rows, & &1.correlation_id) == rows

      # An identical client resend is served as the completed request's successor.
      {first, resent} = send_turn!(first, resume)
      assert %{"type" => "response.completed"} = List.last(resent)
      assert_completed_successor!(setup, List.last(rows), length(rows) + 1)
      assert :ok = FakeUpstream.verify!(upstream)

      drop!(first)
    end
  end

  # The session's owner and the turn's upstream connection live on a second
  # VM; the summarization request is served on this node beside them, and the
  # turn resumes through the owner.
  @tag slow: "boots a second VM that owns the session and shares the committed database"
  test "with the session's owner on another node, the summarization request is served beside it and the turn resumes through it" do
    put_owner_forwarding!(true)
    enter_peer_owner_topology!()
    summary_hold = make_ref()
    upstream = start_upstream(local_compaction_upstream(summary_hold))
    setup = gateway_setup(upstream)
    assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
    peer_owner = start_peer_window_owner!(setup, "#{@peer_thread}:0")
    {_server, port} = start_public_endpoint_with_server!()
    turn = %{new_turn(setup, "full") | thread: @peer_thread}

    assert_local_compaction_served!(upstream, setup, port, turn, summary_hold, {:remote, peer_owner.owner_pid})
  end

  # The handshake grants nothing but the non-owner path, which is what every
  # socket gets with owner forwarding off. A socket that declared a local
  # compaction and then sends ordinary turn frames anchored on the turn's
  # opening response gets exactly that: user input anchored there takes the
  # claim the turn's opener holds and is refused `duplicate_turn` with no row,
  # and a tool result anchored there meets the continuation guard on the
  # socket's own fresh upstream connection, which carries nothing (a failed row
  # with no usage, as the guard answers on any fresh connection). The client's
  # whole resend is then served through the owner. Nothing anchored reaches
  # the provider.
  test "a socket that declared a local compaction answers anchored turn frames as a socket without owner forwarding" do
    put_owner_forwarding!(true)
    tool_result = %{"type" => "function_call_output", "call_id" => @call_id, "output" => "sample"}

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (anchored ordinary turn frames on the socket a local compaction opened, which the released client never sends; the resend entry also admits the guard's connection, which carries nothing)
        FakeUpstream.strict_sequence([
          turn_request(1, FakeUpstream.websocket_text_frames(tool_call_events())),
          turn_request(1, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_resend", "synthetic final answer")))
        ])
      )

    setup = gateway_setup(upstream)
    assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
    {_server, port} = start_public_endpoint_with_server!()
    turn = new_turn(setup, "full")

    first = connect!(port, setup, turn, "prewarm")
    first = prewarm!(first, turn)
    {first, opener} = send_turn!(first, opener_frame(turn))
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_local_compaction_opener"}} = List.last(opener)
    assert_request_finalized!(first, "succeeded")

    second = connect!(port, setup, turn, "compaction")
    steered = frame(turn, "turn", 0, [user_message("synthetic steered input")], %{"previous_response_id" => "resp_local_compaction_opener"})
    {second, steered_events} = send_turn!(second, steered)
    assert [%{"type" => "error", "status" => 409, "error" => %{"code" => "duplicate_turn"}}] = steered_events

    continuation = frame(turn, "turn", 0, [tool_result], %{"previous_response_id" => "resp_local_compaction_opener"})
    {second, continuation_events} = send_turn!(second, continuation)
    assert [native_previous_response_retry_event()] == continuation_events
    assert_request_finalized!(second, "failed")
    drop!(second)

    resend = frame(turn, "turn", 0, history(turn) ++ [tool_call_item(), tool_result], %{})
    {first, resent} = send_turn!(first, resend)
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_local_compaction_resend"}} = List.last(resent)
    assert_request_finalized!(first, "succeeded")

    assert [_opener, _resend] = FakeUpstream.requests(upstream)
    assert FakeUpstream.websocket_connection_count(upstream) == 2
    assert [opener_row, guarded_row, resend_row] = pool_requests(setup)
    assert {opener_row.status, guarded_row.status, guarded_row.last_error_code, resend_row.status} == {"succeeded", "failed", "stream_incomplete", "succeeded"}
    assert CodexPooler.AccountingTestSupport.key_usage_events(guarded_row.id) == %{known: 0, provisional: 0, admissions: 1}
    assert :ok = FakeUpstream.verify!(upstream)

    drop!(first)
  end

  defp assert_local_compaction_served!(upstream, setup, port, turn, summary_hold, owner_reader) do
    first = connect!(port, setup, turn, "prewarm")
    first = prewarm!(first, turn)
    {first, opener} = send_turn!(first, opener_frame(turn))
    assert %{"type" => "response.completed"} = List.last(opener)
    assert_request_finalized!(first, "succeeded")
    binding = owner_binding(first, owner_reader)

    # The client's summarization request, on a new websocket. While the
    # provider holds its answer, and after the client dropped that websocket,
    # the session's owner still serves the turn's socket under the same lease.
    second = connect!(port, setup, turn, "compaction")
    second = send_frame!(second, summary_frame(turn))
    assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^summary_hold}, @detection_timeout_ms
    assert owner_binding(first, owner_reader) == binding
    :ok = FakeUpstream.release_remaining_frames(upstream, summary_hold)
    {second, summary} = receive_turn!(second)
    assert_receive {:fake_upstream_frame_barrier, 3, _handler, ^summary_hold}, @detection_timeout_ms
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_local_compaction_summary"}} = List.last(summary)
    assert_request_finalized!(second, "succeeded")

    # The client drops the compaction's session, and its websocket with it:
    # nothing of that reaches the turn's socket.
    {:ok, drop_log} = with_info_log(fn -> drop!(second) end)
    assert_turn_socket_untouched!(first, drop_log)
    assert owner_binding(first, owner_reader) == binding

    # The turn resumes on its first websocket, in the next context window.
    {first, resumed} = send_turn!(first, resumed_frame(turn))
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_local_compaction_resumed"}} = List.last(resumed)
    assert_request_finalized!(first, "succeeded")

    # One row per request, each served once in the Pool's mode: the opener
    # under the turn's claim, the summarization request under a compaction
    # claim, the resume under the claim of a later request of the turn.
    assert [opener_row, summary_row, resumed_row] = pool_requests(setup)
    assert Enum.map([opener_row, summary_row, resumed_row], & &1.status) == ["succeeded", "succeeded", "succeeded"]
    assert Enum.map([opener_row, summary_row, resumed_row], & &1.request_metadata["routing"]["model_serving_mode"]) == List.duplicate(turn.mode, 3)
    assert "codex-turn:" <> _turn_claim = opener_row.correlation_id
    assert "codex-request:" <> _compaction_claim = summary_row.correlation_id
    assert "codex-resume:" <> _later_request_claim = resumed_row.correlation_id
    assert [_opener, _summary, _resumed] = FakeUpstream.requests(upstream)

    # An identical summarization resend is served and linked to that request.
    third = connect!(port, setup, turn, "compaction")
    {third, resend} = send_turn!(third, summary_frame(turn))
    assert %{"type" => "response.completed"} = List.last(resend)
    assert_completed_successor!(setup, summary_row, 4)
    assert :ok = FakeUpstream.verify!(upstream)

    drop!(third)
    drop!(first)
  end

  # The summarization request goes out whole on an upstream connection of its
  # own; the resume goes out on the turn's connection, with owner forwarding
  # off (the turn socket's session) and on (the session's owner) alike.
  defp local_compaction_upstream(summary_hold) do
    # provenance: observed codex rust-v0.158.0 `codex exec` local compaction against a local fake provider (request order: opener, summarization request, resumed turn; the provider's answers as the fake gave them); ids and text synthetic
    FakeUpstream.strict_sequence([
      turn_request(1, FakeUpstream.websocket_text_frames(tool_call_events())),
      turn_request(2, FakeUpstream.barrier_websocket_frames(message_events("resp_local_compaction_summary", @summary_text), notify: self(), release_ref: summary_hold)),
      turn_request(1, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_resumed", "synthetic final answer"))),
      turn_request(3, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_summary_retry", @summary_text)))
    ])
  end

  defp turn_request(connection_ordinal, respond) do
    FakeUpstream.expect_request(method: "WEBSOCKET", path: @turn_path, websocket_connection_ordinal: connection_ordinal, json: [valid: true, equals: %{"type" => "response.create"}], respond: respond)
  end

  defp tool_call_events(response_id \\ "resp_local_compaction_opener", call_id \\ @call_id) do
    item = %{"type" => "function_call", "id" => "fc_" <> String.replace_prefix(call_id, "call_", ""), "status" => "completed", "call_id" => call_id, "name" => "exec_command", "arguments" => ~s({"cmd":"printf sample"})}

    Enum.map(
      [
        %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress", "output" => []}},
        %{"type" => "response.output_item.done", "output_index" => 0, "item" => item},
        %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => %{"input_tokens" => 499, "output_tokens" => 1, "total_tokens" => 500}}}
      ],
      &CodexPooler.JSON.encode!/1
    )
  end

  defp message_events(response_id, text) do
    item = %{"type" => "message", "id" => "msg_#{response_id}", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => text}]}

    Enum.map(
      [
        %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress", "output" => []}},
        %{"type" => "response.output_item.done", "output_index" => 0, "item" => item},
        %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => %{"input_tokens" => 20, "output_tokens" => 5, "total_tokens" => 25}}}
      ],
      &CodexPooler.JSON.encode!/1
    )
  end

  defp serve_second_compaction!(:same_turn, first, port, setup, turn) do
    first = serve_turn!(first, opener_frame(turn), "resp_local_compaction_opener")
    :ok = serve_summary!(port, setup, turn, 0, history(turn), "one")
    first = serve_turn!(first, frame(turn, "turn", 1, history(turn) ++ [summary_message("one")], %{}), "resp_local_compaction_resumed_one")
    :ok = serve_summary!(port, setup, turn, 1, history(turn) ++ [summary_message("one")], "two")
    resume = frame(turn, "turn", 2, history(turn) ++ [summary_message("two")], %{})
    {serve_turn!(first, resume, "resp_local_compaction_resumed_two"), resume}
  end

  # The next turn opens on the same websocket, anchored on the first turn's
  # last response, and already carries the first summary.
  defp serve_second_compaction!(:next_turn, first, port, setup, turn) do
    first = serve_turn!(first, opener_frame(turn), "resp_local_compaction_opener")
    :ok = serve_summary!(port, setup, turn, 0, history(turn), "one")
    first = serve_turn!(first, frame(turn, "turn", 1, history(turn) ++ [summary_message("one")], %{}), "resp_local_compaction_resumed_one")

    next = %{turn | turn_id: Ecto.UUID.generate(), started_at: System.system_time(:millisecond)}
    first = serve_turn!(first, frame(next, "turn", 1, [user_message(@next_task)], %{"previous_response_id" => "resp_local_compaction_resumed_one"}), "resp_local_compaction_next_opener")
    before_summary = history(turn) ++ [summary_message("one"), assistant_message(answer_text("resp_local_compaction_resumed_one")), user_message(@next_task)]
    :ok = serve_summary!(port, setup, next, 1, before_summary, "two")
    resume = frame(next, "turn", 2, history(turn) ++ [user_message(@next_task), summary_message("two")], %{})
    {serve_turn!(first, resume, "resp_local_compaction_resumed_two"), resume}
  end

  defp expected_claim_classes(:same_turn), do: ["codex-turn", "codex-request", "codex-resume", "codex-request", "codex-resume"]
  defp expected_claim_classes(:next_turn), do: ["codex-turn", "codex-request", "codex-resume", "codex-turn", "codex-request", "codex-resume"]

  defp assert_request_finalized!(client, status) do
    request_id = Repo.one!(from request in Request, where: request.pool_id == ^client.pool_id and request.id not in ^client.prior_request_ids, select: request.id)
    assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"request_id" => ^request_id, "status" => ^status}}}, @detection_timeout_ms
  end

  defp serve_turn!(client, frame, response_id) do
    {client, events} = send_turn!(client, frame)
    assert %{"type" => "response.completed", "response" => %{"id" => ^response_id}} = List.last(events)
    assert_request_finalized!(client, "succeeded")
    client
  end

  # The summarization request on a websocket of its own, which the client
  # drops once it has the summary.
  defp serve_summary!(port, setup, turn, window, before, label) do
    input = before ++ tool_round(label) ++ [user_message(@compaction_prompt)]
    client = connect!(port, setup, turn, "compaction", window)
    client = serve_turn!(client, frame(turn, "compaction", window, input, %{"parallel_tool_calls" => false}, tools: []), "resp_local_compaction_summary_#{label}")
    drop!(client)
  end

  # Opener, summary, resume on the turn's connection, and so on: every summary
  # on an upstream connection of its own.
  defp second_compaction_upstream(:same_turn) do
    # provenance: observed codex 0.159.0 `codex exec` local compactions against a local fake provider (smoke arm compact-local-twice: opener, summary, resume answered with one more tool round, summary, resume); ids and text synthetic
    FakeUpstream.strict_sequence([
      turn_request(1, FakeUpstream.websocket_text_frames(tool_call_events())),
      turn_request(2, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_summary_one", @summary_text))),
      turn_request(1, FakeUpstream.websocket_text_frames(tool_call_events("resp_local_compaction_resumed_one", "call_local_compaction_two"))),
      turn_request(3, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_summary_two", @summary_text))),
      turn_request(1, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_resumed_two", answer_text("resp_local_compaction_resumed_two")))),
      turn_request(1, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_resend", "synthetic resent answer")))
    ])
  end

  defp second_compaction_upstream(:next_turn) do
    # provenance: observed codex 0.159.0 `codex exec` local compactions against a local fake provider (smoke arm compact-local-next: the first turn compacts and ends, the next turn compacts again); ids and text synthetic
    FakeUpstream.strict_sequence([
      turn_request(1, FakeUpstream.websocket_text_frames(tool_call_events())),
      turn_request(2, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_summary_one", @summary_text))),
      turn_request(1, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_resumed_one", answer_text("resp_local_compaction_resumed_one")))),
      turn_request(1, FakeUpstream.websocket_text_frames(tool_call_events("resp_local_compaction_next_opener", "call_local_compaction_two"))),
      turn_request(3, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_summary_two", @summary_text))),
      turn_request(1, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_resumed_two", answer_text("resp_local_compaction_resumed_two")))),
      turn_request(1, FakeUpstream.websocket_text_frames(message_events("resp_local_compaction_resend", "synthetic resent answer")))
    ])
  end

  defp assert_completed_successor!(setup, predecessor, count) do
    rows = pool_requests(setup)
    assert length(rows) == count
    successor = await_request_succeeded!(List.last(rows).id, System.monotonic_time(:millisecond) + @detection_timeout_ms)
    assert Repo.exists?(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^predecessor.id and link.successor_request_id == ^successor.id))
  end

  defp await_request_succeeded!(request_id, deadline) do
    case Repo.get!(Request, request_id) do
      %Request{status: "succeeded"} = request ->
        request

      %Request{status: status} when status in ["accepted", "in_progress"] ->
        assert System.monotonic_time(:millisecond) < deadline, "successor did not settle"

        receive do
        after
          5 -> await_request_succeeded!(request_id, deadline)
        end

      request ->
        flunk("successor ended with #{request.status}")
    end
  end

  defp answer_text(response_id), do: "synthetic final answer of #{response_id}"

  # One tool round the client ran before it compacted, as its history carries it.
  defp tool_round(label) do
    call_id = "call_local_compaction_#{label}"

    [
      %{"type" => "function_call", "id" => "fc_local_compaction_#{label}", "name" => "exec_command", "arguments" => ~s({"cmd":"printf sample"}), "call_id" => call_id},
      %{"type" => "function_call_output", "id" => "fco_local_compaction_#{label}", "call_id" => call_id, "output" => "sample"}
    ]
  end

  defp summary_message(label), do: user_message("#{@summary_text}\nsynthetic summary #{label}")

  # What serves the turn's socket: the session's owner lease on the row, and
  # the owner's lease and downstream binding. Nil without owner forwarding.
  defp owner_reader(:off), do: nil
  defp owner_reader(:on), do: :local

  defp owner_binding(_client, nil), do: nil

  defp owner_binding(client, :local) do
    session_id = socket_connection_state!(client.socket).codex_session.id
    assert {:ok, owner} = WebsocketOwnerSession.lookup(session_id)
    owner_binding(client, {:owner, owner, :sys.get_state(owner)})
  end

  defp owner_binding(client, {:remote, owner}) do
    assert node(owner) != node()
    owner_binding(client, {:owner, owner, :erpc.call(node(owner), :sys, :get_state, [owner], @detection_timeout_ms)})
  end

  defp owner_binding(client, {:owner, owner, owner_state}) do
    socket_state = socket_connection_state!(client.socket)
    session = Repo.get!(CodexSession, socket_state.codex_session.id)
    downstream = owner_state.downstream && Map.take(owner_state.downstream, [:pid, :epoch, :correlation_id])
    socket = client.socket

    # The turn's socket is the owner's downstream, under the session's lease.
    assert %{pid: ^socket} = downstream
    assert Map.take(socket_state.websocket_owner_downstream, [:epoch, :correlation_id]) == Map.take(downstream, [:epoch, :correlation_id])
    assert owner_state.owner_lease_token == session.owner_lease_token

    %{owner: owner, owner_instance_id: session.owner_instance_id, lease: session.owner_lease_token, downstream: downstream}
  end

  # No upstream-close latch (findings#270) and no owner-exit close
  # (findings#276) on the turn's socket: neither taken (the socket's close and
  # its line) nor latched or skipped.
  defp assert_turn_socket_untouched!(client, log) do
    for line <- [
          WebsocketConnectionLogger.downstream_closed_after_upstream_close_message(),
          WebsocketConnectionLogger.downstream_kept_open_after_upstream_close_message(),
          WebsocketConnectionLogger.downstream_closed_after_owner_exit_message(),
          WebsocketConnectionLogger.downstream_kept_open_after_owner_exit_message()
        ] do
      refute log =~ line
    end

    assert Process.alive?(client.socket)
    state = socket_connection_state!(client.socket)
    refute Map.has_key?(state, :upstream_close_pending)
    refute Map.has_key?(state, :owner_exit_close_pending)
    refute Map.get(state, :websocket_owner_lost?, false)
  end

  # The released client's identity of one turn: its thread, the turn id, its
  # context windows (before and after each compaction), and the serving
  # mode the Pool's catalog told it (Lite moves the tools and instructions into
  # the input and marks every frame).
  defp new_turn(setup, mode) do
    %{
      model: setup.model.exposed_model_id,
      thread: Ecto.UUID.generate(),
      turn_id: Ecto.UUID.generate(),
      contexts: Enum.map(0..2, fn _window -> Ecto.UUID.generate() end),
      mode: mode,
      started_at: System.system_time(:millisecond)
    }
  end

  # The handshake carries the turn metadata of the request the client opens
  # the connection for: the prewarm on the turn's socket, the summarization
  # request on the compaction's socket, in the window it summarizes.
  defp connect!(port, setup, turn, request_kind, window \\ 0) do
    before = WebsocketCleanupFence.listener_sockets()
    turn_id = if request_kind == "prewarm", do: "", else: turn.turn_id

    headers = [
      {"session-id", turn.thread},
      {"thread-id", turn.thread},
      {"x-client-request-id", turn.thread},
      {"x-codex-window-id", "#{turn.thread}:#{window}"},
      {"x-codex-turn-metadata", CodexPooler.JSON.encode!(turn_metadata(turn, request_kind, window, turn_id))},
      {"x-codex-beta-features", "remote_compaction_v2"},
      {"openai-beta", "responses_websockets=2026-02-06"},
      {"originator", "codex_exec"}
    ]

    {conn, websocket, ref, _response_headers} = public_websocket_connect_with_request_headers!(port, setup, turn.thread, @turn_path, headers)
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    %{conn: conn, websocket: websocket, ref: ref, socket: socket, pool_id: setup.pool.id}
  end

  defp drop!(client) do
    Mint.HTTP.close(client.conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
  end

  defp prewarm!(client, turn) do
    frame = frame(turn, "prewarm", 0, [developer_message("synthetic developer instructions"), user_message("synthetic environment context")], %{"generate" => false})
    {client, events} = send_turn!(client, frame)
    assert %{"type" => "response.completed", "response" => %{"id" => ""}} = List.last(events)
    client
  end

  # The Pooler answers the prewarm itself, with an empty response id
  # (`WebsocketCodec.warmup_result/0`), and the released client sends its next
  # request whole when the last response id is empty (`client.rs`
  # `prepare_websocket_request` at rust-v0.158.0 and rust-v0.159.0): the
  # turn's opener carries the full history and no anchor.
  defp opener_frame(turn), do: frame(turn, "turn", 0, history(turn), %{})

  # The summarization request: the whole history, the tool round included,
  # then the client's compaction prompt; no tools and no parallel tool calls.
  defp summary_frame(turn) do
    input =
      history(turn) ++
        [
          tool_call_item(),
          %{"type" => "function_call_output", "id" => "fco_local_compaction_sample", "call_id" => @call_id, "output" => "sample"},
          user_message(@compaction_prompt)
        ]

    frame(turn, "compaction", 0, input, %{"parallel_tool_calls" => false}, tools: [])
  end

  defp tool_call_item, do: %{"type" => "function_call", "id" => "fc_local_compaction_sample", "name" => "exec_command", "arguments" => ~s({"cmd":"printf sample"}), "call_id" => @call_id}

  # The turn resumed on the summary, in the next context window.
  defp resumed_frame(turn), do: frame(turn, "turn", 1, history(turn) ++ [user_message(@summary_text)], %{})

  defp history(turn) do
    [
      developer_message("synthetic developer instructions"),
      user_message("synthetic environment context"),
      user_message("local compaction sample: run the tool, then answer")
    ]
    |> then(&if(turn.mode == "lite", do: [additional_tools() | &1], else: &1))
  end

  defp frame(turn, request_kind, window_number, input, extra, opts \\ []) do
    tools = Keyword.get(opts, :tools, [tool()])

    base = %{
      "type" => "response.create",
      "model" => turn.model,
      "tool_choice" => "auto",
      "parallel_tool_calls" => true,
      "reasoning" => %{"effort" => "medium"},
      "store" => false,
      "stream" => true,
      "include" => ["reasoning.encrypted_content"],
      "text" => %{"verbosity" => "low"},
      "prompt_cache_key" => turn.thread,
      "input" => input,
      "client_metadata" => client_metadata(turn, request_kind, window_number)
    }

    base =
      case turn.mode do
        "full" -> Map.merge(base, %{"instructions" => "synthetic base instructions", "tools" => tools})
        "lite" -> base |> Map.put("parallel_tool_calls", false) |> put_in(["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"], "true")
      end

    base |> Map.merge(Map.drop(extra, if(turn.mode == "lite", do: ["parallel_tool_calls"], else: []))) |> CodexPooler.JSON.encode!()
  end

  defp client_metadata(turn, request_kind, window_number) do
    turn_id = if request_kind == "prewarm", do: "", else: turn.turn_id

    metadata = %{
      "session_id" => turn.thread,
      "thread_id" => turn.thread,
      "turn_id" => turn_id,
      "x-codex-installation-id" => @installation_id,
      "x-codex-window-id" => "#{turn.thread}:#{window_number}",
      "x-codex-ws-stream-request-start-ms" => Integer.to_string(System.system_time(:millisecond)),
      "x-codex-turn-metadata" => CodexPooler.JSON.encode!(turn_metadata(turn, request_kind, window_number, turn_id))
    }

    if request_kind == "prewarm", do: metadata, else: Map.put(metadata, "root_turn_id", turn.turn_id)
  end

  defp turn_metadata(turn, request_kind, window_number, turn_id) do
    context_window_id = Enum.at(turn.contexts, window_number)

    base = %{
      "installation_id" => @installation_id,
      "session_id" => turn.thread,
      "thread_id" => turn.thread,
      "agent_name" => "/root",
      "turn_id" => turn_id,
      "window_id" => "#{turn.thread}:#{window_number}",
      "window_number" => window_number,
      "context_window_id" => context_window_id,
      "request_kind" => request_kind,
      "thread_source" => "user",
      "sandbox" => "seatbelt",
      "sandbox_mode" => "read-only",
      "auto_review_enabled" => false,
      "node_repl_auto_review_required" => false,
      "node_repl_disabled" => false,
      "analytics_enabled" => false
    }

    case request_kind do
      "prewarm" ->
        Map.merge(base, %{"model" => turn.model, "reasoning_effort" => "medium"})

      "turn" ->
        Map.merge(base, %{"root_turn_id" => turn.turn_id, "turn_trigger" => "exec", "workspaces" => %{}, "turn_started_at_unix_ms" => turn.started_at, "model" => turn.model, "reasoning_effort" => "medium"})

      "compaction" ->
        Map.merge(base, %{
          "root_turn_id" => turn.turn_id,
          "turn_trigger" => "exec",
          "workspaces" => %{},
          "turn_started_at_unix_ms" => turn.started_at,
          "compaction" => %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses", "phase" => "mid_turn", "strategy" => "memento"}
        })
    end
  end

  defp tool, do: %{"type" => "function", "name" => "exec_command", "description" => "synthetic tool", "strict" => false, "parameters" => %{"type" => "object", "properties" => %{"cmd" => %{"type" => "string"}}, "required" => ["cmd"]}}

  defp additional_tools, do: %{"type" => "additional_tools", "id" => "at_local_compaction_sample", "role" => "developer", "tools" => []}

  defp developer_message(text), do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => text}]}
  defp user_message(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}
  defp assistant_message(text), do: %{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => text}]}

  defp send_turn!(client, frame), do: client |> send_frame!(frame) |> receive_turn!()

  defp send_frame!(client, frame) do
    prior_request_ids = Repo.all(from request in Request, where: request.pool_id == ^client.pool_id, select: request.id)
    client = Map.put(client, :prior_request_ids, prior_request_ids)
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    %{client | conn: conn, websocket: websocket}
  end

  defp receive_turn!(client) do
    {conn, websocket, events} = receive_events_until_terminal!(client.conn, client.websocket, client.ref, [])
    {%{client | conn: conn, websocket: websocket}, events}
  end

  # Every JSON event the Pooler sends up to the native terminal, controls excepted.
  defp receive_events_until_terminal!(conn, websocket, ref, events) do
    message = receive_mint_socket_message!(conn, @detection_timeout_ms, "timed out waiting for the native terminal")

    case Mint.WebSocket.stream(conn, message) do
      {:ok, conn, responses} ->
        {websocket, events} =
          Enum.reduce(responses, {websocket, events}, fn
            {:data, ^ref, data}, {websocket, events} ->
              assert {:ok, websocket, frames} = Mint.WebSocket.decode(websocket, data)
              {websocket, events ++ for({:text, text} <- frames, event = CodexPooler.JSON.decode!(text), not String.starts_with?(event["type"] || "", "codex."), do: event)}

            _response, acc ->
              acc
          end)

        if Enum.any?(events, &(&1["type"] in @native_terminal_types)),
          do: {conn, websocket, events},
          else: receive_events_until_terminal!(conn, websocket, ref, events)

      {:error, _conn, reason, _responses} ->
        flunk("websocket receive failed: #{inspect(reason)}")

      :unknown ->
        receive_events_until_terminal!(conn, websocket, ref, events)
    end
  end

  defp put_owner_forwarding!(enabled?) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)
  end

  defp pool_requests(setup),
    do: Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: [asc: request.admitted_at]))
end
