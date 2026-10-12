defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketPreTurnCompactionCutPeerTest do
  use CodexPoolerWeb.ConnCase, async: false

  @moduletag capture_log: true

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [start_shared_bridge_peer!: 0]

  alias CodexPoolerWeb.Runtime.PreTurnCompactionCutScenario, as: Scenario

  # The admitted native compaction cut of
  # `backend_codex_websocket_pre_turn_compaction_cut_test.exs` (findings#206
  # rows 206-310, 206-330, 206-436, 206-455), Full pre-turn and mid-turn and
  # Lite pre-turn, with owner forwarding on and the session's owner and its
  # provider connection on a second VM sharing the committed database, the
  # socket on this node, as when a production turn lands on the other web pod
  # (findings#206 row 206-334). The Lite arms commit the Pool's serving mode
  # through `model_serving_scope/0`, which removes the owner it commits
  # (findings#270 row 270-330). The module boots that VM once: booting it and
  # warming its first turn took one to three seconds of every test, and on a
  # busy machine pushed the queued arms past the six-second budget
  # (findings#206 row 206-539).
  # Each test starts its own session's owner on it and stops it when it ends.
  setup_all do
    %{peer_node: start_shared_bridge_peer!()}
  end

  @tag compaction_drain_authority: true, real_generation: true
  test "a real reconnected peer generation rejects an old issued capability while its compact drains", %{peer_node: peer_node} do
    assert Scenario.run_drain_completion(:peer, peer_node: peer_node, reconnect_generation: true, hold_completion: true) == :ok
  end

  for shape <- [:pre_turn, :standalone_turn] do
    @tag compaction_drain_completion: true, control_shape: shape
    test "full #{shape} peer control completes after drain before provider output", %{control_shape: shape, peer_node: peer_node} do
      assert Scenario.run_drain_completion(:peer, peer_node: peer_node, shape: shape, before_output: true, hold_completion: true) == :ok
    end
  end

  for fault <- [:before_admission, :generation, :capability, :stale_lease, :upstream_closed, :key_revoked, :cancel_queued, :result_before_cancel] do
    @tag compaction_drain_authority: true, compaction_drain_cancel: fault in [:cancel_queued, :result_before_cancel], fault: fault
    test "real peer compact drain authority boundary #{fault}", %{fault: fault, peer_node: peer_node} do
      assert Scenario.run_drain_authority(:peer, fault, peer_node: peer_node) == :ok
    end
  end

  for order <- [:before_publication, :after_publication] do
    @tag compaction_owned_cleanup: true, compaction_cut_order: true, cleanup_order: order
    test "exact compact proof cleanup preserves committed foreign proof and same-model fixture #{order}", %{peer_node: peer_node, cleanup_order: order} do
      assert Scenario.run_scenario("full", :pre_turn, :peer, :drain_no_resend, :on_arrival, peer_node: peer_node, proof_cleanup_probe: order, execution_order: :stopped) == Scenario.expected(:drain_no_resend, :peer)
    end
  end

  for {mode, form} <- [{"full", :first}, {"lite", :first}, {"lite", :incremental}] do
    @tag compaction_drain_completion: true, mode: mode, form: form
    test "#{mode} #{form} compact reaches the client while its actual peer owner drains", %{mode: mode, form: form, peer_node: peer_node} do
      assert Scenario.run_drain_completion(:peer, peer_node: peer_node, mode: mode, form: form, hold_completion: true) == :ok
    end
  end

  @tag compaction_drain_completion: true
  test "full incremental mid-turn compact completes after its actual peer owner begins draining", %{peer_node: peer_node} do
    log = ExUnit.CaptureLog.capture_log(fn -> assert Scenario.run_drain_completion(:peer, peer_node: peer_node) == :ok end)
    assert log =~ "reason=owner_drained confirmation=not_applied"
  end

  @tag compaction_drain_delivery: true
  test "full compact peer owner drain waits for tracked completion handoff after accounting settled", %{peer_node: peer_node} do
    assert Scenario.run_drain_completion(:peer, peer_node: peer_node, hold_completion: true) == :ok
  end

  @tag compaction_drain_deadline: true
  test "full compact peer drain keeps its original deadline when the proxy receipt write fails", %{peer_node: peer_node} do
    log = ExUnit.CaptureLog.capture_log(fn -> assert Scenario.run_drain_completion(:peer, peer_node: peer_node, hold_completion: true, receipt_failure: true) == :ok end)
    assert log =~ "downstream delivery receipt not persisted"
    assert log =~ "longer than 100ms"
  end

  for {mode, shape, cut} <- [{"full", :pre_turn, :observed_cut}, {"full", :pre_turn, :observed_cut_exited}, {"full", :mid_turn, :observed_cut}, {"lite", :pre_turn, :observed_cut}, {"lite", :pre_turn, :observed_cut_exited}] do
    @tag mode: mode, shape: shape, topology: :peer, cut: cut
    test "#{mode} #{shape} peer admitted compaction #{cut} with the closed socket's cleanup held: the released client's first websocket retry is served",
         %{mode: mode, shape: shape, topology: topology, cut: cut, peer_node: peer_node} do
      assert Scenario.run_scenario(mode, shape, topology, cut, :on_arrival, peer_node: peer_node) == Scenario.expected(cut, topology)
    end
  end

  # The first retry's take-over cannot see the cut compaction settle within
  # its two-second wait (the settlement is held): refused once, then served
  # (findings#206 row 206-580, seen on this mid-turn arm under load).
  for {mode, shape} <- [{"full", :pre_turn}, {"full", :mid_turn}, {"lite", :pre_turn}] do
    @tag slow: "holds settlement through the real two-second takeover budget before serving the retry"
    @tag mode: mode, shape: shape, topology: :peer, cut: :observed_cut_settlement_held
    test "#{mode} #{shape} peer admitted compaction whose settlement outlasts the take-over wait: the first retry is refused and the next is served",
         %{mode: mode, shape: shape, topology: topology, cut: cut, peer_node: peer_node} do
      assert Scenario.run_scenario(mode, shape, topology, cut, :on_arrival, peer_node: peer_node) == Scenario.expected(cut, topology)
    end
  end

  for {mode, shape} <- [{"full", :pre_turn}, {"full", :mid_turn}, {"lite", :pre_turn}], cut <- [:unobserved_cut, :observed_cut] do
    @tag mode: mode, shape: shape, topology: :peer, cut: cut, dispatch: :queued
    test "#{mode} #{shape} peer compaction queued behind the settling turn, #{cut}: the owner keys it and no resend is a second generation while it runs",
         %{mode: mode, shape: shape, topology: topology, cut: cut, peer_node: peer_node} do
      assert Scenario.run_scenario(mode, shape, topology, cut, :queued, peer_node: peer_node) == Scenario.expected(cut, topology)
    end
  end

  # The production shape of findings#270 row 270-352: the rollout drain of the
  # socket's node cuts the compaction while its owner, on the other node, is
  # not draining. Hold the original proxy before it can settle the owner's
  # cancellation answer: the real watcher stops it after receipt COMMIT and
  # the first resend recovers its proven-dead request as its successor.
  # The explicit finalized controls below exercise the other ordering.
  # It used to meet the dead request as a
  # live turn the owner no longer held and was refused `409 duplicate_turn`
  # twice, after which the HTTPS fallback bought the compaction again.
  # And the provider's stream dying after the Pooler collected the compaction
  # item, with the owner and its provider connection on the peer (row
  # 270-365).
  for cut <- [:drain_before_output, :drain_after_output, :provider_cut_after_output] do
    @tag mode: "full", shape: :pre_turn, topology: :peer, cut: cut, compaction_cut_order: true
    test "full pre_turn peer admitted compaction #{cut}: the released client's first websocket retry is its successor",
         %{mode: mode, shape: shape, topology: topology, cut: cut, peer_node: peer_node} do
      order = if cut in [:drain_before_output, :drain_after_output], do: :stopped
      assert Scenario.run_scenario(mode, shape, topology, cut, :on_arrival, peer_node: peer_node, execution_order: order) == Scenario.expected(cut, topology)
    end
  end

  @tag compaction_cut_order: true
  test "never resent: the dead-execution scan settles it once the attempt is eligible", %{peer_node: peer_node} do
    log = ExUnit.CaptureLog.capture_log(fn -> assert Scenario.run_scenario("full", :pre_turn, :peer, :drain_no_resend, :on_arrival, peer_node: peer_node, execution_order: :stopped) == Scenario.expected(:drain_no_resend, :peer) end)
    refute log =~ "websocket delivery receipt handoff failed"
  end

  for cut <- [:drain_before_output, :drain_after_output, :drain_no_resend] do
    @tag compaction_executor_finalized: true, compaction_cut_order: true, cut: cut
    test "full pre_turn peer drain preserves the original executor's committed failure for #{cut}", %{peer_node: peer_node, cut: cut} do
      log = ExUnit.CaptureLog.capture_log(fn -> assert Scenario.run_scenario("full", :pre_turn, :peer, cut, :on_arrival, peer_node: peer_node, execution_order: :finalized) == Scenario.expected(cut, :peer, :finalized) end)
      refute log =~ "websocket delivery receipt handoff failed"
    end
  end

  for mode <- ["full", "lite"], cut <- [:no_cut, :before_output, :after_output, :after_completion, :unobserved_cut] do
    @tag mode: mode, shape: :pre_turn, topology: :peer, cut: cut
    test "#{mode} pre_turn peer admitted compaction #{cut}: the released client's retries buy the compaction once per request and the turn completes",
         %{mode: mode, shape: shape, topology: topology, cut: cut, peer_node: peer_node} do
      assert Scenario.run_scenario(mode, shape, topology, cut, :on_arrival, peer_node: peer_node) == Scenario.expected(cut, topology)
    end
  end
end
