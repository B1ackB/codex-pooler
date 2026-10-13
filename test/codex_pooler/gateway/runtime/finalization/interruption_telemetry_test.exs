defmodule CodexPooler.Gateway.Runtime.Finalization.InterruptionTelemetryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPooler.UnboxedFixture
  import Ecto.Query

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Gateway.Payloads.RequestOptions

  alias CodexPooler.Gateway.Persistence.{
    BridgeOwnerLease,
    CodexSession,
    CodexTurn,
    RuntimeCleanup,
    SessionContinuity
  }

  alias CodexPooler.Gateway.Runtime.Dispatch.SelectedCandidateContext
  alias CodexPooler.Gateway.Runtime.Finalization
  alias CodexPooler.Gateway.Runtime.Finalization.Interruption
  alias CodexPooler.Gateway.Runtime.Finalization.InterruptionOutcome
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Jobs.RuntimeStateCleanupWorker
  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias Ecto.Adapters.SQL.Sandbox

  # Failure-detection budget for the ordered-operations tasks, not a behaviour timer.
  @task_timeout 15_000

  test "interrupted stream outcomes are emitted only through the interruption owner" do
    capture_outcomes(fn ->
      assert InterruptionOutcome.emit("http_sse", "websocket") == :ok

      assert_receive {:stream_outcome,
                      %{
                        outcome: "interrupted",
                        downstream_transport: "http_sse",
                        upstream_transport: "websocket"
                      }}
    end)
  end

  test "cleanup worker emits each expired-owner interruption after commit exactly once" do
    fixtures =
      for mode <- [:active_attempt, :legacy_attempt, :without_attempt] do
        fixture = committed_interruption_fixture!(mode)
        expire_owner!(fixture)
        fixture
      end

    capture_outcomes(fn ->
      assert run_unboxed(fn -> perform_job(RuntimeStateCleanupWorker, %{}) end) == :ok

      for fixture <- fixtures do
        upstream_transport = if fixture.attempt, do: "websocket", else: "unknown"

        assert_receive {:stream_outcome,
                        %{
                          outcome: "interrupted",
                          downstream_transport: "websocket",
                          upstream_transport: ^upstream_transport
                        }}

        assert_receive {:stream_outcome_transaction, false}
        assert committed_interruption_state(fixture).turn_status == "interrupted"
      end

      [dead, legacy, _unattempted] = fixtures

      assert run_unboxed(fn -> Repo.get!(Request, dead.request.id).last_error_code end) ==
               "dead_execution_recovered"

      assert run_unboxed(fn -> Repo.get!(Request, legacy.request.id).last_error_code end) ==
               "owner_unavailable"

      refute_received {:stream_outcome, _}
      assert run_unboxed(fn -> perform_job(RuntimeStateCleanupWorker, %{}) end) == :ok
      refute_received {:stream_outcome, _}
    end)
  end

  test "cleanup worker recovery inside a rolled-back caller transaction emits nothing" do
    fixture = committed_interruption_fixture!(:active_attempt)
    expire_owner!(fixture)

    capture_outcomes(fn ->
      assert run_unboxed(fn ->
               Repo.transaction(fn ->
                 assert perform_job(RuntimeStateCleanupWorker, %{}) == :ok
                 Repo.rollback(:caller_rollback)
               end)
             end) == {:error, :caller_rollback}

      refute_received {:stream_outcome, _}
      assert committed_interruption_state(fixture).turn_status == "in_progress"
    end)
  end

  test "recovery inside a caller transaction hands its outcomes back instead of dropping them" do
    # The emitter above is silent for the right reason — nothing has committed
    # yet — but silence is also what losing the markers looks like. A caller
    # that owns the transaction is the only thing that knows when the write
    # becomes durable, so the outcomes are its to emit and it is told so.
    #
    # `RuntimeStateCleanup.run/1` runs every step bare, so this arm is
    # unreachable in production today. That invariant is what the after-commit
    # property rests on, which is exactly why breaking it must be audible.
    fixture = committed_interruption_fixture!(:active_attempt)
    expire_owner!(fixture)

    log =
      capture_outcomes(fn ->
        ExUnit.CaptureLog.capture_log(fn ->
          assert run_unboxed(fn ->
                   Repo.transaction(fn ->
                     assert perform_job(RuntimeStateCleanupWorker, %{}) == :ok
                     Repo.rollback(:caller_rollback)
                   end)
                 end) == {:error, :caller_rollback}
        end)
      end)

    refute_received {:stream_outcome, _}
    assert log =~ "expired-owner recovery outcomes dropped inside a caller transaction"
    assert log =~ "outcomes=1"
    assert committed_interruption_state(fixture).turn_status == "in_progress"
  end

  test "the after-commit rule belongs to the gate, not to each caller that reaches it" do
    # findings#195 row 195-05 asks that every caller that can drop these markers
    # be audited. An enumeration of call sites answers that only until the next
    # site is written, so the rule now lives in the one function every one of
    # them reaches — `Interruption.emit_outcomes_after_commit/1`, the single
    # path to `Streaming.emit_stream_outcome/3` for an interrupted outcome —
    # and it reads the transaction it is standing in rather than a flag the
    # caller computed and passed down. What that buys is that a fourth caller
    # gets the same answer as these three without anyone having to find it.
    #
    # So this drives the gate itself, on the same markers, with nothing
    # different between the two halves except whether a transaction is open.
    markers = [
      %{
        kind: :stream_outcome,
        outcome: "interrupted",
        downstream_transport: "websocket",
        upstream_transport: "unknown"
      }
    ]

    capture_outcomes(fn ->
      assert run_unboxed(fn ->
               Repo.transaction(fn ->
                 Interruption.emit_committed_recovery_outcomes(%{interrupted_outcomes: markers})
               end)
             end) == {:ok, {:deferred, markers}}

      refute_received {:stream_outcome, _}

      assert run_unboxed(fn ->
               Interruption.emit_committed_recovery_outcomes(%{interrupted_outcomes: markers})
             end) == :ok

      assert_receive {:stream_outcome, %{outcome: "interrupted"}}
      assert_receive {:stream_outcome_transaction, false}
    end)
  end

  test "every successful expired-owner result reaches the outcome emitter" do
    marker = %{
      kind: :stream_outcome,
      outcome: "interrupted",
      downstream_transport: "websocket",
      upstream_transport: "unknown"
    }

    capture_outcomes(fn ->
      assert RuntimeCleanup.complete_expired_owner_recovery(
               {:ok, {0, %{interrupted_outcomes: [marker]}}},
               7
             ) == {:cont, {:ok, 7}}

      assert_receive {:stream_outcome, %{outcome: "interrupted"}}
      assert_receive {:stream_outcome_transaction, false}
    end)
  end

  for {name, finalizer, reason, phase, committed_turn_status} <- [
        {"direct interrupt", :direct, "owner_drained", "turn_interrupted", "interrupted"},
        {"task exception", :task_exception, "owner_task_exception", "task_exception", "failed"}
      ] do
    test "caller-owned #{name} publishes only after commit and stays silent on rollback" do
      for outer_result <- [:commit, :rollback] do
        fixture = committed_interruption_fixture!(:without_attempt)
        receipt = pre_attempt_receipt(fixture)
        samples = capture_pre_attempt_releases()

        result =
          run_unboxed(fn ->
            Repo.transaction(fn ->
              finalization =
                case unquote(finalizer) do
                  :direct ->
                    Interruption.interrupt_direct_request(receipt, unquote(reason))

                  :task_exception ->
                    Interruption.finalize_task_exception_request(receipt, unquote(reason))
                end

              assert {:ok, %{after_commit_markers: markers}} = finalization
              assert markers != []

              case outer_result do
                :commit -> {:committed, markers}
                :rollback -> Repo.rollback(:caller_rollback)
              end
            end)
          end)

        case outer_result do
          :commit ->
            assert {:ok, {:committed, markers}} = result
            assert Interruption.emit_committed_deferred_outcomes(markers) == :ok

            assert [
                     %{
                       phase: unquote(phase),
                       release_reason: unquote(reason),
                       in_transaction?: false
                     }
                   ] = drain_samples(samples)

            assert committed_interruption_state(fixture) == %{
                     request_status: "failed",
                     attempt_status: nil,
                     turn_status: unquote(committed_turn_status),
                     settlement_count: 0
                   }

            assert committed_release_count(fixture) == 1

          :rollback ->
            assert result == {:error, :caller_rollback}
            assert drain_samples(samples) == []

            assert committed_interruption_state(fixture) == %{
                     request_status: "in_progress",
                     attempt_status: nil,
                     turn_status: "in_progress",
                     settlement_count: 0
                   }

            assert committed_release_count(fixture) == 0
        end
      end
    end
  end

  test "a multi-turn expired-owner recovery does not count a release its rollback erases" do
    # `interrupt_session_transaction/4` maps
    # `interrupt_turn!/5` over EVERY in-progress turn of a session inside one
    # transaction. The first turn's `release_unattempted_request!/6` counts a
    # `turn_interrupted` pre-attempt release; the second turn then raises, and
    # the whole transaction it shares with the first is aborted.
    #
    # The raise is precisely where `multi_turn_interruption_fixture!/0` puts it,
    # and saying so is the point of the assertion below. The second turn's
    # reservation has its `source_event_id` detached, so
    # `release_unattempted_request!/6`'s `Repo.get_by!(LedgerEntry, ...)` raises
    # `Ecto.NoResultsError` — past `rollback_interrupted_accounting/4`, which
    # never runs, and past both of that function's clauses, neither of which
    # rescues. RuntimeCleanup's per-candidate boundary now catches the raise
    # after its transaction rolls back, preserving `{:raised, Ecto.NoResultsError}`
    # rather than `{:interrupt_accounting_failed, _}`. Two
    # reviews read this comment as the accounting-rollback path because the
    # error assertion was loose enough not to tell them apart.
    #
    # The injected fault stands for the class, not for itself: any exception
    # raised after the first turn's release — a lock timeout, a serialization
    # failure, a raced terminal row — aborts the same shared transaction and
    # leaves the same counted sample behind.
    #
    # `tap_pre_attempt_release_count/3`
    # (`lib/codex_pooler/accounting/lifecycle/request_lifecycle.ex`) runs on the
    # value of `Repo.transaction/1`, which is a savepoint release rather than a
    # commit whenever a caller already holds a transaction, and unlike every
    # emitter beside it that function never asks `Repo.in_transaction?/0`. So
    # the counter keeps a sample for a release row that no longer exists, biased
    # upward on the `turn_interrupted` and `task_exception` slices. Ledger,
    # settlement, routing and durable metadata all roll back correctly; only the
    # counter lies.
    #
    # This is the production path findings#195 row 195-94 names, driven from the
    # real worker. Fixing the defect means deferring the marker to the outermost
    # commit the way interrupted outcomes already are, and it must change this
    # test — which is also the moment row 195-12's second clause becomes
    # satisfiable.
    fixture = multi_turn_interruption_fixture!()
    expire_owner!(fixture)
    samples = capture_pre_attempt_releases()

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, {:runtime_state_cleanup_steps_failed, [:gateway_runtime], summary}} =
                 run_unboxed(fn -> perform_job(RuntimeStateCleanupWorker, %{}) end)

        assert summary.expired_owner_sessions_failed == 1
        assert summary.expired_owner_sessions_recovered == 0
      end)

    # Pin the raised class, not only the failed phase. Per-candidate isolation
    # must retain the cause without including the exception's query or payload.
    assert log =~ "runtime state cleanup step gateway_runtime failed"
    assert log =~ "{:raised, Ecto.NoResultsError}"
    assert log =~ "Ecto.NoResultsError"
    refute log =~ "expected at least one result but got none"
    refute log =~ "interrupt_accounting_failed"

    # The transaction rolled back: no release row exists for either request and
    # both turns are still running.
    assert run_unboxed(fn -> release_entry_count(fixture) end) == 0
    assert run_unboxed(fn -> turn_statuses(fixture) end) == ["in_progress", "in_progress"]

    # The first turn's release marker stayed attached to the shared transaction
    # and was discarded with it.
    assert drain_samples(samples) == []
  end

  test "cleanup worker recovery raises past the accounting rollback and emits nothing" do
    # Same injection as `outermost accounting rollback emits one settlement
    # failure and preserves exact tuple` below — the reservation ledger entry is
    # deleted — and a different failure at a different stage, which is exactly
    # why the failure has to be named.
    #
    # `interrupt_codex_turn/2` reaches the missing reservation through
    # `finalize_interrupted_request!/5`, whose `rescue` turns the
    # `Ecto.NoResultsError` into `{:interrupt_accounting_failed, _}` and emits a
    # `settlement_failed` outcome. Expired-owner recovery never gets that far:
    # `recover_expired_owner_locked/2` runs dead-execution recovery over the
    # session's in-progress turns FIRST, so `recover_dead_request_execution/2`
    # raises out of `RequestLifecycle.lock_finalization_rows/2` before any turn
    # is interrupted. `recover_dead_request_execution/2` handles `{:error, _}`
    # and not a raise. RuntimeCleanup contains the exception at its candidate
    # boundary and reports its class. Nothing is emitted
    # because nothing reached the code that builds a marker.
    #
    # So this test never exercised the interruption's accounting handling at
    # all — not `release_unattempted_request!/6`, not
    # `rollback_interrupted_accounting/4` — while its name said "accounting
    # rollback" and its error tuple was loose enough to agree with anything.
    # findings#195 row 195-127, the same class as 195-103 one test over.
    fixture = committed_interruption_fixture!(:accounting_failure)
    expire_owner!(fixture)

    capture_outcomes(fn ->
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, {:runtime_state_cleanup_steps_failed, [:gateway_runtime], summary}} =
                   run_unboxed(fn -> perform_job(RuntimeStateCleanupWorker, %{}) end)

          assert summary.expired_owner_sessions_failed == 1
          assert summary.expired_owner_sessions_recovered == 0
        end)

      assert log =~ "runtime state cleanup step gateway_runtime failed"
      assert log =~ "{:raised, Ecto.NoResultsError}"
      assert log =~ "Ecto.NoResultsError"
      refute log =~ "expected at least one result but got none"
      refute log =~ "CodexPooler.Accounting.LedgerEntry"
      refute log =~ "interrupt_accounting_failed"

      refute_received {:stream_outcome, _}
      assert committed_interruption_state(fixture).turn_status == "in_progress"
    end)
  end

  test "a candidate that goes stale between selection and settlement emits nothing" do
    # The cleanup selects expired-owner candidates outside the per-session
    # transaction, so a session that acquires a new owner in between is settled
    # by nobody: `recover_expired_owner_locked/2` returns `:stale_owner` and the
    # caller counts no recovery. Nothing may be emitted on that arm — an
    # interrupted outcome for a turn that is still running would be a lie, and
    # it is the arm a real rollout produces when a pod takes over a lease while
    # the sweep is mid-pass.
    fixture = committed_interruption_fixture!(:active_attempt)
    expire_owner!(fixture)

    barrier_ref = make_ref()
    parent = self()

    Application.put_env(
      :codex_pooler,
      :runtime_cleanup_owner_candidate_test_barrier,
      {parent, barrier_ref}
    )

    on_exit(fn ->
      Application.delete_env(:codex_pooler, :runtime_cleanup_owner_candidate_test_barrier)
    end)

    capture_outcomes(fn ->
      cleanup =
        Task.async(fn -> run_unboxed(fn -> perform_job(RuntimeStateCleanupWorker, %{}) end) end)

      assert_receive {:runtime_cleanup_owner_candidates_selected, cleanup_pid, ^barrier_ref, candidates},
                     @task_timeout

      assert Enum.any?(candidates, &(&1.session_id == fixture.session.id)),
             "the fixture session was not selected, so the stale arm was never reached"

      # A successor takes the lease while the sweep holds its candidate list.
      run_unboxed(fn ->
        Repo.update_all(from(s in CodexSession, where: s.id == ^fixture.session.id),
          set: [owner_lease_token: Ecto.UUID.generate()]
        )
      end)

      send(cleanup_pid, {:release_runtime_cleanup_owner_candidates, barrier_ref})
      assert Task.await(cleanup, @task_timeout) == :ok

      refute_received {:stream_outcome, _}
      assert committed_interruption_state(fixture).turn_status == "in_progress"
    end)
  end

  defp expire_owner!(fixture) do
    run_unboxed(fn ->
      past = DateTime.add(DateTime.utc_now(), -60, :second)

      Repo.update_all(from(s in CodexSession, where: s.id == ^fixture.session.id),
        set: [owner_lease_expires_at: past]
      )

      Repo.update_all(
        from(l in BridgeOwnerLease, where: l.codex_session_id == ^fixture.session.id),
        set: [expires_at: past]
      )
    end)
  end

  test "outermost active-attempt interruption emits once after commit and repeated interruption is silent" do
    fixture = committed_interruption_fixture!(:active_attempt)

    capture_outcomes(fn ->
      result = run_unboxed(fn -> interrupt_turn(fixture) end)

      assert result == {:ok, %{interrupted_turn_count: 1, turn_authority: :selected}}

      assert_receive {:stream_outcome,
                      %{
                        outcome: "interrupted",
                        downstream_transport: "websocket",
                        upstream_transport: "websocket"
                      }}

      assert committed_interruption_state(fixture) == %{
               request_status: "failed",
               attempt_status: "failed",
               turn_status: "interrupted",
               settlement_count: 1
             }

      assert run_unboxed(fn -> interrupt_turn(fixture) end) ==
               {:ok, %{interrupted_turn_count: 0, turn_authority: :selected}}

      refute_received {:stream_outcome, _metadata}
    end)
  end

  # The websocket owner-failure finalizer reads the same vocabulary as the SSE
  # finalizer and the turn status: an owner that crashed interrupted the turn,
  # a forwarding refusal failed it (findings#228).
  for {reason, outcome} <- [owner_crashed: "interrupted", owner_busy: "failed"] do
    test "a websocket turn finalized on #{reason} settles with the #{outcome} stream outcome" do
      fixture = committed_interruption_fixture!(:active_attempt)

      capture_outcomes(fn ->
        result =
          run_unboxed(fn ->
            Finalization.finalize_failed_websocket_response(fixture.selected_context, %{
              body: "",
              headers: [],
              reason: unquote(reason),
              started: System.monotonic_time(:millisecond)
            })
          end)

        assert {:error, %{code: unquote(Atom.to_string(reason))}} = result

        assert_receive {:stream_outcome,
                        %{
                          outcome: unquote(outcome),
                          downstream_transport: "websocket",
                          upstream_transport: "websocket"
                        }}

        assert_receive {:stream_outcome_transaction, false}
        refute_received {:stream_outcome, _other}
      end)

      assert %{request_status: "failed", attempt_status: "failed"} =
               committed_interruption_state(fixture)
    end
  end

  test "outermost no-attempt interruption emits unknown upstream after commit" do
    fixture = committed_interruption_fixture!(:without_attempt)

    capture_outcomes(fn ->
      assert run_unboxed(fn -> interrupt_turn(fixture) end) ==
               {:ok, %{interrupted_turn_count: 1, turn_authority: :selected}}

      assert_receive {:stream_outcome,
                      %{
                        outcome: "interrupted",
                        downstream_transport: "websocket",
                        upstream_transport: "unknown"
                      }}

      refute_received {:stream_outcome, _metadata}
    end)
  end

  test "interruption-first and transport-finalizer-first orderings emit one total outcome each" do
    for ordering <- [:interruption_first, :transport_finalizer_first] do
      fixture = committed_interruption_fixture!(:active_attempt)

      capture_outcomes(fn ->
        results =
          case ordering do
            :interruption_first ->
              run_ordered_operations(
                fn -> interrupt_turn(fixture) end,
                fn -> transport_finalize(fixture) end
              )

            :transport_finalizer_first ->
              run_ordered_operations(
                fn -> transport_finalize(fixture) end,
                fn -> interrupt_turn(fixture) end
              )
          end

        assert Enum.count(results, &match?({:error, %{code: "client_disconnected"}}, &1)) == 1

        expected_interruption_result =
          case ordering do
            :interruption_first ->
              {:ok, %{interrupted_turn_count: 1, turn_authority: :selected}}

            :transport_finalizer_first ->
              {:ok, %{interrupted_turn_count: 0, turn_authority: :selected}}
          end

        assert Enum.find(results, &match?({:ok, %{interrupted_turn_count: _}}, &1)) ==
                 expected_interruption_result

        assert_receive {:stream_outcome,
                        %{
                          outcome: "interrupted",
                          downstream_transport: "websocket",
                          upstream_transport: "websocket"
                        }}

        refute_received {:stream_outcome, _metadata}
      end)
    end
  end

  test "outermost accounting rollback emits one settlement failure and preserves exact tuple" do
    fixture = committed_interruption_fixture!(:accounting_failure)

    capture_outcomes(fn ->
      result = run_unboxed(fn -> interrupt_turn(fixture) end)

      assert {:error, {:interrupt_accounting_failed, %Ecto.NoResultsError{}}} = result

      assert_receive {:stream_outcome,
                      %{
                        outcome: "settlement_failed",
                        downstream_transport: "websocket",
                        upstream_transport: "websocket"
                      }}

      refute_received {:stream_outcome, _metadata}
    end)

    assert committed_interruption_state(fixture) == %{
             request_status: "in_progress",
             attempt_status: "in_progress",
             turn_status: "in_progress",
             settlement_count: 0
           }

    assert run_unboxed(fn -> Repo.get!(CodexSession, fixture.session.id).status end) == "active"
  end

  test "caller-owned transaction hands markers to the commit owner and rollback discards them" do
    for outer_result <- [:commit, :rollback] do
      fixture = committed_interruption_fixture!(:active_attempt)

      capture_outcomes(fn ->
        result =
          run_unboxed(fn ->
            Repo.transaction(fn ->
              assert {:ok,
                      %{
                        interrupted_turn_count: 1,
                        turn_authority: :selected,
                        after_commit_markers: markers
                      }} = interrupt_turn(fixture)

              case outer_result do
                :commit -> {:committed, markers}
                :rollback -> Repo.rollback(:caller_rollback)
              end
            end)
          end)

        case outer_result do
          :commit ->
            assert {:ok, {:committed, [_marker] = markers}} = result
            assert Interruption.emit_committed_deferred_outcomes(markers) == :ok
            assert_receive {:stream_outcome, %{outcome: "interrupted"}}
            assert_receive {:stream_outcome_transaction, false}

          :rollback ->
            assert result == {:error, :caller_rollback}
            refute_received {:stream_outcome, _metadata}
        end
      end)

      state = committed_interruption_state(fixture)

      expected_state =
        case outer_result do
          :commit ->
            %{
              request_status: "failed",
              attempt_status: "failed",
              turn_status: "interrupted",
              settlement_count: 1
            }

          :rollback ->
            %{
              request_status: "in_progress",
              attempt_status: "in_progress",
              turn_status: "in_progress",
              settlement_count: 0
            }
        end

      assert state == expected_state
    end
  end

  test "caller-owned accounting rollback preserves the exact error and emits zero" do
    fixture = committed_interruption_fixture!(:accounting_failure)

    capture_outcomes(fn ->
      result =
        run_unboxed(fn ->
          Repo.transaction(fn ->
            result = interrupt_turn(fixture)

            assert {:error,
                    {:deferred_after_commit, {:interrupt_accounting_failed, %Ecto.NoResultsError{}},
                     [
                       %{
                         kind: :stream_outcome,
                         outcome: "settlement_failed",
                         downstream_transport: "websocket",
                         upstream_transport: "websocket"
                       }
                     ]}} = result

            :caller_callback_returned
          end)
        end)

      assert result == {:error, :rollback}
      refute_received {:stream_outcome, _metadata}
    end)

    assert committed_interruption_state(fixture) == %{
             request_status: "in_progress",
             attempt_status: "in_progress",
             turn_status: "in_progress",
             settlement_count: 0
           }
  end

  # The cleanup is registered, never scoped. An assertion that fails inside a `run_unboxed/1`
  # block raises in the linked task, whose exit signal kills the test process before any
  # enclosing `after` can run; ExUnit's own teardown runs regardless of how the test died.
  defp committed_interruption_fixture!(mode) do
    fixture = build_committed_interruption_fixture!(mode)
    register_unboxed_cleanup!(fn -> delete_committed_fixture!(fixture) end)

    if mode == :active_attempt,
      do: CodexPooler.ExecutionProofSupport.publish_committed_terminal!(fixture.attempt)

    fixture
  end

  defp build_committed_interruption_fixture!(mode) do
    run_unboxed(fn ->
      unique = System.unique_integer([:positive, :monotonic])
      setup = accounting_setup(%{account_label: "Interruption telemetry #{unique}"})

      assert {:ok, session} =
               Gateway.start_codex_session(setup.auth, %{
                 accepted_turn_state: "interruption-telemetry-#{unique}"
               })

      correlation_id = "interruption-telemetry-request-#{unique}"

      assert {:ok, reserved} =
               Accounting.reserve(
                 setup.auth,
                 setup.model,
                 %{"model" => setup.model.exposed_model_id},
                 %{
                   endpoint: "/backend-api/codex/responses",
                   transport: "websocket",
                   correlation_id: correlation_id,
                   request_metadata: %{"codex_session_id" => session.id}
                 }
               )

      attempt =
        if mode == :without_attempt do
          nil
        else
          assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

          maybe_clear_execution_identity!(mode, attempt)
          maybe_delete_reservation_ledger_entry!(mode, reserved.request)

          attempt
        end

      request_options =
        RequestOptions.for_websocket(%{
          request_id: correlation_id,
          interrupt_reason: "client_disconnected",
          reconnect_window_seconds: 300
        })

      assert {:ok, turn} =
               SessionContinuity.start_codex_turn(session, reserved.request, request_options)

      ExecutionIdentity.complete()

      Map.merge(setup, %{
        session: session,
        request: reserved.request,
        attempt: attempt,
        turn: turn,
        request_options: request_options,
        selected_context: selected_context(setup, reserved, attempt, request_options)
      })
    end)
  end

  defp maybe_delete_reservation_ledger_entry!(:accounting_failure, request) do
    Repo.delete_all(
      from entry in LedgerEntry,
        where: entry.source_event_id == ^"request:#{request.id}:reservation"
    )
  end

  defp maybe_delete_reservation_ledger_entry!(_mode, _request), do: :ok

  defp maybe_clear_execution_identity!(:legacy_attempt, attempt) do
    Repo.update_all(from(a in Attempt, where: a.id == ^attempt.id),
      set: [owner_execution_id: nil]
    )
  end

  defp maybe_clear_execution_identity!(_mode, _attempt), do: :ok

  defp selected_context(setup, reserved, attempt, request_options) do
    %SelectedCandidateContext{
      auth: setup.auth,
      endpoint: "/backend-api/codex/responses",
      payload: %{"model" => setup.model.exposed_model_id},
      model: setup.model,
      reserved: reserved,
      request_options: request_options,
      route_plan: %{affinity: %{enabled?: false}, demotions: %{}},
      assignment: setup.assignment,
      identity: setup.identity,
      index: 0,
      retry_count: 0,
      allow_retry?: false,
      routing_attempt_metadata: %{},
      route_class: "proxy_websocket",
      attempt: attempt,
      started: System.monotonic_time(:millisecond)
    }
  end

  # One session carrying two in-progress turns, neither with an attempt, whose
  # SECOND turn has no reservation ledger entry: finalizing it raises and
  # `rollback_interrupted_accounting/4` aborts the transaction both turns share.
  defp multi_turn_interruption_fixture! do
    fixture = committed_interruption_fixture!(:without_attempt)

    run_unboxed(fn ->
      second = add_failing_turn!(fixture)

      # `in_progress_turns_for_session/1` orders by started_at, so the failing
      # turn is made unambiguously second.
      Repo.update_all(from(t in CodexTurn, where: t.id == ^fixture.turn.id),
        set: [started_at: DateTime.add(DateTime.utc_now(), -30, :second)]
      )

      Repo.update_all(from(t in CodexTurn, where: t.id == ^second.turn.id),
        set: [started_at: DateTime.utc_now()]
      )

      Map.merge(fixture, %{
        requests: [fixture.request, second.request],
        turns: [fixture.turn, second.turn]
      })
    end)
  end

  defp add_failing_turn!(fixture) do
    unique = System.unique_integer([:positive, :monotonic])
    correlation_id = "interruption-telemetry-second-#{unique}"

    assert {:ok, reserved} =
             Accounting.reserve(
               fixture.auth,
               fixture.model,
               %{"model" => fixture.model.exposed_model_id},
               %{
                 endpoint: "/backend-api/codex/responses",
                 transport: "websocket",
                 correlation_id: correlation_id,
                 request_metadata: %{"codex_session_id" => fixture.session.id}
               }
             )

    opts =
      RequestOptions.for_websocket(%{
        request_id: correlation_id,
        interrupt_reason: "client_disconnected",
        reconnect_window_seconds: 300
      })

    assert {:ok, turn} =
             SessionContinuity.start_codex_turn(fixture.session, reserved.request, opts)

    # The reservation still reads as outstanding, so this turn takes the same
    # release branch as the first one, but `finalize_reserved_request_failure/2`
    # cannot find the entry by its source event id and raises — which is what
    # `rollback_interrupted_accounting/4` turns into a rollback of the
    # transaction both turns share. Deleting the entry instead would make the
    # branch quietly succeed, and giving this turn an attempt would fail the
    # dead-execution recovery that runs before any turn is interrupted.
    Repo.update_all(
      from(entry in LedgerEntry,
        where: entry.source_event_id == ^"request:#{reserved.request.id}:reservation"
      ),
      set: [source_event_id: "request:#{reserved.request.id}:reservation-detached"]
    )

    %{request: reserved.request, turn: turn}
  end

  defp release_entry_count(%{requests: requests}) do
    ids = Enum.map(requests, & &1.id)

    Repo.aggregate(
      from(entry in LedgerEntry,
        where: entry.request_id in ^ids and entry.entry_kind == "release"
      ),
      :count
    )
  end

  defp turn_statuses(%{turns: turns}) do
    ids = Enum.map(turns, & &1.id)

    Repo.all(from turn in CodexTurn, where: turn.id in ^ids, select: turn.status, order_by: :id)
  end

  defp capture_pre_attempt_releases do
    handler_id = "pre-attempt-release-#{System.unique_integer([:positive, :monotonic])}"
    parent = self()
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        [:codex_pooler, :accounting, :reservation, :pre_attempt_release],
        fn _event, _measurements, metadata, _config ->
          send(parent, {:pre_attempt_release, metadata, Repo.in_transaction?()})
        end,
        nil
      )

    handler_id
  end

  defp drain_samples(handler_id) when is_binary(handler_id) do
    :telemetry.detach(handler_id)
    collect_samples([])
  end

  defp collect_samples(acc) do
    receive do
      {:pre_attempt_release, metadata, in_transaction?} ->
        collect_samples([Map.put(metadata, :in_transaction?, in_transaction?) | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp interrupt_turn(fixture) do
    Interruption.interrupt_codex_turn(fixture.session, fixture.request_options)
  end

  defp pre_attempt_receipt(fixture) do
    %{
      session_id: fixture.session.id,
      request_id: fixture.request.id,
      correlation_id: fixture.request.correlation_id,
      api_key_id: fixture.request.api_key_id,
      owner_binding: nil
    }
  end

  defp transport_finalize(fixture) do
    Finalization.finalize_failed_websocket_response(fixture.selected_context, %{
      body: "",
      headers: [],
      reason: :client_disconnected,
      started: System.monotonic_time(:millisecond)
    })
  end

  defp committed_interruption_state(fixture) do
    run_unboxed(fn ->
      %{
        request_status: Repo.get!(Request, fixture.request.id).status,
        attempt_status: fixture.attempt && Repo.get!(Attempt, fixture.attempt.id).status,
        turn_status: Repo.get!(CodexTurn, fixture.turn.id).status,
        settlement_count:
          Repo.aggregate(
            from(entry in LedgerEntry,
              where: entry.request_id == ^fixture.request.id and entry.entry_kind == "settlement"
            ),
            :count,
            :id
          )
      }
    end)
  end

  defp committed_release_count(fixture) do
    run_unboxed(fn ->
      Repo.aggregate(
        from(entry in LedgerEntry,
          where: entry.request_id == ^fixture.request.id and entry.entry_kind == "release"
        ),
        :count,
        :id
      )
    end)
  end

  defp delete_committed_fixture!(fixture) do
    CodexPooler.PoolerFixtures.delete_committed_pools!([fixture.pool.id])

    Repo.delete_all(from identity in UpstreamIdentity, where: identity.id == ^fixture.identity.id)

    Repo.delete_all(
      from pricing in CodexPooler.Catalog.PricingSnapshot,
        where: pricing.id == ^fixture.pricing.id
    )

    :ok
  end

  defp capture_outcomes(fun) do
    handler_id = "interruption-outcome-#{System.unique_integer([:positive, :monotonic])}"
    parent = self()

    # Also on_exit: a linked crash or the ExUnit timeout kills the test before `after` runs.
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        [:codex_pooler, :gateway, :stream, :outcome],
        fn _event, _measurements, metadata, _config ->
          send(parent, {:stream_outcome, metadata})
          send(parent, {:stream_outcome_transaction, Repo.in_transaction?()})
        end,
        nil
      )

    try do
      fun.()
    after
      :telemetry.detach(handler_id)
    end
  end

  defp run_ordered_operations(first_fun, second_fun) do
    parent = self()
    barrier = make_ref()

    tasks =
      for {position, operation} <- [first: first_fun, second: second_fun] do
        Task.async(fn ->
          send(parent, {barrier, position, :ready})

          receive do
            {^barrier, ^position, :run} -> Sandbox.unboxed_run(Repo, operation)
          after
            @task_timeout -> flunk("#{position} interruption ordering task was not released")
          end
        end)
      end

    [first_task, second_task] = tasks

    try do
      assert_receive {^barrier, :first, :ready}, @task_timeout
      assert_receive {^barrier, :second, :ready}, @task_timeout

      send(first_task.pid, {barrier, :first, :run})
      first_result = Task.await(first_task, @task_timeout)

      send(second_task.pid, {barrier, :second, :run})
      second_result = Task.await(second_task, @task_timeout)

      [first_result, second_result]
    after
      Enum.each(tasks, fn task ->
        if Process.alive?(task.pid), do: Task.shutdown(task, :brutal_kill)
      end)
    end
  end
end
