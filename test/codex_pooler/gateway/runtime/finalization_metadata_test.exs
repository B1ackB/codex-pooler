defmodule CodexPooler.Gateway.Runtime.FinalizationMetadataTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Gateway.Payloads.RequestOptions

  alias CodexPooler.Gateway.Persistence.{
    BridgeDemotion,
    BridgeOwnerLease,
    CodexSession,
    CodexTurn,
    RoutingCircuitState,
    SessionContinuity
  }

  alias CodexPooler.Gateway.Persistence.SessionContinuity.OwnerWitness
  alias CodexPooler.Gateway.Routing.{BridgeRing, RoutePlanInput}
  alias CodexPooler.Gateway.Runtime.Dispatch.SelectedCandidateContext
  alias CodexPooler.Gateway.Runtime.Finalization
  alias CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement
  alias CodexPooler.Gateway.Runtime.Finalization.Metadata
  alias CodexPooler.Gateway.Transports.TransportFailureReason
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Repo

  import CodexPooler.AccountingTestSupport, only: [accounting_setup: 0, key_usage_events: 1]

  test "HTTP and websocket finalization ignore retired metadata while preserving prompt cache state" do
    options =
      RequestOptions.build(%{payload_compression: %{"attempted" => true, "status" => "compressed"}}, "/backend-api/codex/responses", %{"model" => "example-model"})
      |> RequestOptions.put_runtime_context(prompt_cache_controls_downgraded: true)

    response = %Req.Response{status: 200, headers: [{"content-type", ["application/json"]}]}

    for metadata <- [Metadata.response_metadata(response, nil, options), Metadata.websocket_response_metadata([], nil, options)] do
      assert metadata["prompt_cache_controls_downgraded"] == true
      refute Map.has_key?(metadata, "payload_compression")
    end

    assert_raise KeyError, fn -> RequestOptions.put_runtime_context(options, payload_compression: %{}) end
  end

  test "stale owner usage settles without promoting an interrupted turn or moving replacement assignment" do
    setup = accounting_setup()

    %{assignment: replacement_assignment} =
      CodexPooler.PoolerFixtures.upstream_assignment_fixture(setup.pool)

    payload = %{"model" => setup.model.exposed_model_id}

    assert {:ok, %CodexSession{} = session} =
             Gateway.start_codex_session(setup.auth, %{
               accepted_turn_state: "terminal-settlement-#{System.unique_integer([:positive])}",
               owner_instance_id: "node-a"
             })

    session = Repo.reload!(session)
    {:ok, witness} = OwnerWitness.new(session)

    request_options =
      %{transport: "http_json"}
      |> RequestOptions.build("/backend-api/codex/responses", payload)
      |> RequestOptions.put_session_owner_witness(witness)

    assert {:ok, reserved} =
             Accounting.reserve(setup.auth, setup.model, payload, %{
               endpoint: "/backend-api/codex/responses",
               transport: "http_json",
               correlation_id: "terminal-settlement-#{System.unique_integer([:positive])}",
               request_metadata: %{}
             })

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    assert {:ok, %CodexTurn{} = turn} =
             SessionContinuity.start_codex_turn(session, reserved.request, request_options)

    turn
    |> Ecto.Changeset.change(%{status: CodexTurn.interrupted_status()})
    |> Repo.update!()

    replacement_token = Ecto.UUID.generate()
    takeover_now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    takeover_deadline = DateTime.add(takeover_now, 90, :second)

    session
    |> Ecto.Changeset.change(%{
      owner_instance_id: "node-b",
      owner_lease_token: replacement_token,
      owner_lease_expires_at: takeover_deadline,
      pool_upstream_assignment_id: replacement_assignment.id,
      last_heartbeat_at: takeover_now,
      updated_at: takeover_now
    })
    |> Repo.update!()

    lease =
      Repo.one!(
        from lease in BridgeOwnerLease,
          where: lease.codex_session_id == ^session.id and lease.status == "active",
          limit: 1
      )

    lease
    |> Ecto.Changeset.change(%{
      owner_instance_id: "node-b",
      lease_token: replacement_token,
      expires_at: takeover_deadline,
      pool_upstream_assignment_id: replacement_assignment.id,
      renewed_at: takeover_now,
      updated_at: takeover_now
    })
    |> Repo.update!()

    before_session = Repo.get!(CodexSession, session.id)
    before_lease = Repo.reload!(lease)

    assert {:ok, %{finalization_disposition: :inserted}} =
             AttemptSettlement.finalize_success(
               reserved.request,
               attempt,
               %{
                 status: "usage_known",
                 input_tokens: 3,
                 output_tokens: 2,
                 total_tokens: 5
               },
               %{response_status_code: 200},
               witness
             )

    assert Repo.reload!(reserved.request).status == "succeeded"
    assert Repo.reload!(attempt).status == "succeeded"
    assert Repo.reload!(turn).status == CodexTurn.interrupted_status()
    assert Repo.reload!(reserved.request).usage_status == "usage_known"
    assert Repo.reload!(attempt).usage_status == "usage_known"
    assert is_nil(Repo.reload!(attempt).response_metadata["downstream_delivery"])
    assert [settlement] = Repo.all(from entry in LedgerEntry, where: entry.request_id == ^reserved.request.id and entry.entry_kind == "settlement" and entry.amount_status == "recorded")
    assert settlement.total_tokens == 5
    CodexPooler.TestDiagnostics.puts(Jason.encode!(%{scenario: "open_accounting_interrupted_turn", status: Repo.reload!(reserved.request).status, attempt: Repo.reload!(attempt).status, turn: Repo.reload!(turn).status, tokens: settlement.total_tokens, delivered_receipt: false}))
    assert Repo.get!(CodexSession, session.id) == before_session
    assert Repo.reload!(lease) == before_lease
  end

  test "post-serialization dispatch failure persists only prompt cache adaptation metadata" do
    setup = accounting_setup()
    payload = %{"model" => setup.model.exposed_model_id}

    assert {:ok, reserved} =
             Accounting.reserve(setup.auth, setup.model, payload, %{
               endpoint: "/backend-api/codex/responses",
               transport: "http_json",
               correlation_id: "prompt-cache-dispatch-error-#{System.unique_integer([:positive])}",
               request_metadata: %{}
             })

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    request_options =
      %{
        transport: "http_json",
        upstream_endpoint: "/backend-api/codex/responses"
      }
      |> RequestOptions.build("/backend-api/codex/responses", payload)
      |> RequestOptions.put_runtime_context(prompt_cache_controls_downgraded: true)

    context = %SelectedCandidateContext{
      auth: setup.auth,
      endpoint: "/backend-api/codex/responses",
      payload: payload,
      model: setup.model,
      reserved: reserved,
      request_options: request_options,
      route_plan:
        BridgeRing.plan_route(%{
          auth: setup.auth,
          model: setup.model,
          candidates: [{setup.assignment, setup.identity}],
          route_plan_input: RoutePlanInput.from_reserved(reserved),
          request_options: request_options
        }),
      assignment: setup.assignment,
      identity: setup.identity,
      index: 0,
      retry_count: 0,
      allow_retry?: false,
      routing_attempt_metadata: %{},
      route_class: "proxy_http",
      attempt: attempt,
      started: System.monotonic_time(:millisecond)
    }

    assert {:error, %{status: 502}} =
             Finalization.handle_dispatch_error(:synthetic_network_failure, context, 7)

    attempt = Repo.get!(Attempt, attempt.id)

    assert attempt.response_metadata["prompt_cache_controls_downgraded"] == true

    refute Map.has_key?(
             Repo.get!(Request, reserved.request.id).request_metadata,
             "prompt_cache_controls_downgraded"
           )
  end

  test "ambiguous post-submit HTTP failure does not rotate candidates" do
    setup = accounting_setup()

    %{assignment: second_assignment, identity: second_identity} =
      CodexPooler.PoolerFixtures.upstream_assignment_fixture(setup.pool)

    payload = %{"model" => setup.model.exposed_model_id}

    assert {:ok, reserved} =
             Accounting.reserve(setup.auth, setup.model, payload, %{
               endpoint: "/backend-api/codex/responses",
               transport: "http_json",
               correlation_id: "ambiguous-dispatch-#{System.unique_integer([:positive])}",
               request_metadata: %{}
             })

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    request_options = request_options()

    context = %SelectedCandidateContext{
      auth: setup.auth,
      endpoint: "/backend-api/codex/responses",
      payload: payload,
      model: setup.model,
      reserved: reserved,
      request_options: request_options,
      route_plan:
        BridgeRing.plan_route(%{
          auth: setup.auth,
          model: setup.model,
          candidates: [
            {setup.assignment, setup.identity},
            {second_assignment, second_identity}
          ],
          route_plan_input: RoutePlanInput.from_reserved(reserved),
          request_options: request_options
        }),
      assignment: setup.assignment,
      identity: setup.identity,
      index: 0,
      retry_count: 0,
      allow_retry?: true,
      routing_attempt_metadata: %{},
      route_class: "proxy_http",
      attempt: attempt,
      started: System.monotonic_time(:millisecond)
    }

    error = TransportFailureReason.upstream_transport_error(:timeout, %{phase: :request})

    assert {:error, %{status: 502}} = Finalization.handle_dispatch_error(error, context, 7)
    assert Repo.reload!(reserved.request).status == "failed"
    refute Repo.reload!(attempt).retryable
    assert Repo.aggregate(Attempt, :count) == 1
  end

  test "terminal websocket failure settles once with the sanitized local guard diagnostic" do
    setup = accounting_setup()
    payload = %{"model" => setup.model.exposed_model_id, "stream" => true}

    assert {:ok, reserved} =
             Accounting.reserve(setup.auth, setup.model, payload, %{
               endpoint: "/backend-api/codex/responses",
               transport: "websocket",
               correlation_id: "continuation-guard-finalization-#{System.unique_integer([:positive])}",
               request_metadata: %{}
             })

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    circuit =
      %RoutingCircuitState{
        pool_id: setup.auth.pool.id,
        pool_upstream_assignment_id: setup.assignment.id,
        upstream_identity_id: setup.identity.id,
        model_identifier: setup.model.exposed_model_id,
        route_class: "proxy_websocket",
        status: "half_open",
        reason_code: "upstream_5xx",
        failure_count: 3,
        success_count: 0,
        opened_at: DateTime.add(now, -120, :second),
        half_opened_at: now,
        metadata: %{"probe_in_flight_count" => 1},
        created_at: DateTime.add(now, -120, :second),
        updated_at: now
      }
      |> Repo.insert!()

    request_options =
      %{
        request_id: "continuation-guard-finalization",
        upstream_endpoint: "/backend-api/codex/responses"
      }
      |> RequestOptions.for_websocket(payload)

    context = %SelectedCandidateContext{
      auth: setup.auth,
      endpoint: "/backend-api/codex/responses",
      payload: payload,
      model: setup.model,
      reserved: reserved,
      request_options: request_options,
      assignment: setup.assignment,
      identity: setup.identity,
      index: 0,
      retry_count: 0,
      allow_retry?: true,
      routing_attempt_metadata: %{},
      route_class: "proxy_websocket",
      routing_circuit_state: circuit,
      routing_circuit_admission: :probe,
      attempt: attempt,
      started: System.monotonic_time(:millisecond)
    }

    raw_sentinel = "caller-controlled-terminal-sentinel"

    terminal_result = %{
      body: ~s(data: {"type":"error","error":{"code":"previous_response_not_found"}}\n\n),
      terminal: "error",
      status: 200,
      headers: [],
      started: System.monotonic_time(:millisecond),
      upstream_error_code: "previous_response_not_found",
      upstream_error_param: raw_sentinel,
      transport_failure: %{
        "reason" => "previous_response_generation_mismatch",
        "reason_class" => "previous_response_generation_mismatch",
        "phase" => "send_payload",
        "termination_source" => "continuation_generation_guard",
        "connection_use" => "fresh",
        "pre_visible_output" => true,
        "upstream_committed" => false,
        "terminal_seen" => false,
        "text_frame_count" => 0,
        "previous_response_id" => raw_sentinel,
        "message" => raw_sentinel,
        "raw_frame" => raw_sentinel
      }
    }

    assert {:ok, %{status: 200, websocket_messages: []}} =
             Finalization.finalize_terminal_websocket_response(context, terminal_result)

    request = Repo.get!(Request, reserved.request.id)
    attempt = Repo.get!(Attempt, attempt.id)

    assert request.status == "failed"
    assert request.last_error_code == "stream_incomplete"
    assert attempt.status == "failed"
    assert attempt.network_error_code == "stream_incomplete"
    assert attempt.response_metadata["upstream_error_code"] == "previous_response_not_found"
    assert attempt.response_metadata["upstream_error_param"] == "previous_response_id"

    assert attempt.response_metadata["transport_failure"] == %{
             "connection_use" => "fresh",
             "phase" => "send_payload",
             "pre_visible_output" => true,
             "reason" => "previous_response_generation_mismatch",
             "reason_class" => "previous_response_generation_mismatch",
             "termination_source" => "continuation_generation_guard",
             "terminal_seen" => false,
             "text_frame_count" => 0,
             "upstream_committed" => false
           }

    assert Repo.aggregate(
             from(entry in LedgerEntry,
               where: entry.request_id == ^request.id and entry.entry_kind == "settlement"
             ),
             :count
           ) == 1

    # The guard refused before anything was sent, so no usage applies: the
    # reservation's estimate is neither the settlement's tokens and cost nor
    # the key's provisional tokens, which it was as unknown usage.
    assert_undispatched_settlement!(request, attempt)

    assert Repo.all(from(demotion in BridgeDemotion)) == []

    updated_circuit = Repo.get!(RoutingCircuitState, circuit.id)
    assert updated_circuit.status == "half_open"
    assert updated_circuit.reason_code == "upstream_5xx"
    assert updated_circuit.failure_count == 3
    assert updated_circuit.success_count == 0
    assert updated_circuit.metadata["probe_in_flight_count"] == 0

    refute inspect({request.request_metadata, attempt.response_metadata}) =~ raw_sentinel
  end

  test "exact previous response miss fixes the param when guard metadata is stale" do
    setup = accounting_setup()
    payload = %{"model" => setup.model.exposed_model_id, "stream" => true}

    assert {:ok, reserved} =
             Accounting.reserve(setup.auth, setup.model, payload, %{
               endpoint: "/backend-api/codex/responses",
               transport: "websocket",
               correlation_id: "continuation-guard-stale-#{System.unique_integer([:positive])}",
               request_metadata: %{}
             })

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    request_options =
      RequestOptions.for_websocket(
        %{
          request_id: "continuation-guard-stale",
          upstream_endpoint: "/backend-api/codex/responses"
        },
        payload
      )

    context = %SelectedCandidateContext{
      auth: setup.auth,
      endpoint: "/backend-api/codex/responses",
      payload: payload,
      model: setup.model,
      reserved: reserved,
      request_options: request_options,
      assignment: setup.assignment,
      identity: setup.identity,
      index: 0,
      retry_count: 0,
      allow_retry?: true,
      routing_attempt_metadata: %{},
      route_class: "proxy_websocket",
      attempt: attempt,
      started: System.monotonic_time(:millisecond)
    }

    raw_sentinel = "caller-controlled-stale-guard-sentinel"

    assert {:ok, %{status: 200, websocket_messages: []}} =
             Finalization.finalize_terminal_websocket_response(context, %{
               body: ~s(data: {"type":"error","error":{"code":"previous_response_not_found"}}\n\n),
               terminal: "error",
               status: 200,
               headers: [],
               started: System.monotonic_time(:millisecond),
               upstream_error_code: "previous_response_not_found",
               upstream_error_param: raw_sentinel,
               transport_failure: %{
                 "connection_use" => "reused",
                 "phase" => "receive",
                 "pre_visible_output" => false,
                 "reason" => "previous_response_generation_mismatch",
                 "reason_class" => raw_sentinel,
                 "termination_source" => "continuation_generation_guard",
                 "terminal_seen" => true,
                 "text_frame_count" => 99,
                 "upstream_committed" => true,
                 "previous_response_id" => raw_sentinel,
                 "message" => raw_sentinel,
                 "raw_frame" => raw_sentinel
               }
             })

    request = Repo.get!(Request, reserved.request.id)
    attempt = Repo.get!(Attempt, attempt.id)

    assert request.status == "failed"
    assert request.last_error_code == "stream_incomplete"
    assert attempt.status == "failed"
    assert attempt.network_error_code == "stream_incomplete"
    assert attempt.response_metadata["upstream_error_code"] == "previous_response_not_found"
    assert attempt.response_metadata["upstream_error_param"] == "previous_response_id"
    refute Map.has_key?(attempt.response_metadata, "transport_failure")

    assert Repo.aggregate(
             from(entry in LedgerEntry,
               where: entry.request_id == ^request.id and entry.entry_kind == "settlement"
             ),
             :count
           ) == 1

    # Guard-shaped metadata that is not the guard's exact pre-send shape proves
    # nothing about dispatch: the miss keeps the unknown usage of a request the
    # provider may have received.
    assert_unknown_usage_settlement!(request, attempt)

    assert Repo.all(from(demotion in BridgeDemotion)) == []
    refute inspect({request.request_metadata, attempt.response_metadata}) =~ raw_sentinel
  end

  test "terminal websocket finalization rejects a valid guard map for another terminal code" do
    setup = accounting_setup()
    payload = %{"model" => setup.model.exposed_model_id, "stream" => true}

    assert {:ok, reserved} =
             Accounting.reserve(setup.auth, setup.model, payload, %{
               endpoint: "/backend-api/codex/responses",
               transport: "websocket",
               correlation_id: "continuation-guard-near-miss-#{System.unique_integer([:positive])}",
               request_metadata: %{}
             })

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    request_options =
      RequestOptions.for_websocket(
        %{
          request_id: "continuation-guard-near-miss",
          upstream_endpoint: "/backend-api/codex/responses"
        },
        payload
      )

    context = %SelectedCandidateContext{
      auth: setup.auth,
      endpoint: "/backend-api/codex/responses",
      payload: payload,
      model: setup.model,
      reserved: reserved,
      request_options: request_options,
      assignment: setup.assignment,
      identity: setup.identity,
      index: 0,
      retry_count: 0,
      allow_retry?: true,
      routing_attempt_metadata: %{},
      route_class: "proxy_websocket",
      attempt: attempt,
      started: System.monotonic_time(:millisecond)
    }

    assert {:ok, %{status: 200}} =
             Finalization.finalize_terminal_websocket_response(context, %{
               body: ~s(data: {"type":"error","error":{"code":"server_error"}}\n\n),
               terminal: "error",
               status: 200,
               headers: [],
               started: System.monotonic_time(:millisecond),
               upstream_error_code: "server_error",
               upstream_error_param: "reasoning.summary",
               transport_failure: %{
                 "connection_use" => "fresh",
                 "phase" => "send_payload",
                 "pre_visible_output" => true,
                 "reason" => "previous_response_generation_mismatch",
                 "reason_class" => "previous_response_generation_mismatch",
                 "termination_source" => "continuation_generation_guard",
                 "terminal_seen" => false,
                 "text_frame_count" => 0,
                 "upstream_committed" => false
               }
             })

    attempt = Repo.get!(Attempt, attempt.id)
    refute Map.has_key?(attempt.response_metadata, "transport_failure")
    assert attempt.response_metadata["upstream_error_param"] == "reasoning.summary"
    assert_unknown_usage_settlement!(Repo.get!(Request, attempt.request_id), attempt)
  end

  defp assert_undispatched_settlement!(request, attempt) do
    %{reservation: reservation, settlement: settlement, release: release} = ledger(request)
    assert reservation.total_tokens > 0

    assert request.usage_status == "not_applicable"
    assert attempt.usage_status == "not_applicable"
    assert settlement.attempt_id == attempt.id
    assert settlement.usage_status == "not_applicable"
    assert release.usage_status == "not_applicable"

    assert {settlement.input_tokens, settlement.output_tokens, settlement.total_tokens} ==
             {nil, nil, nil}

    assert Decimal.equal?(settlement.estimated_cost_micros, 0)
    assert Decimal.equal?(settlement.settled_cost_micros, 0)
    assert settlement.details["estimated_from_reserve"] == false
    assert settlement.details["usage_source"] == "undispatched_refusal"
    assert key_usage_events(request.id) == %{known: 0, provisional: 0, admissions: 1}
  end

  defp assert_unknown_usage_settlement!(request, attempt) do
    %{reservation: reservation, settlement: settlement} = ledger(request)
    assert reservation.total_tokens > 0

    assert request.usage_status == "usage_unknown"
    assert attempt.usage_status == "usage_unknown"
    assert settlement.usage_status == "usage_unknown"
    assert settlement.total_tokens == reservation.total_tokens
    assert settlement.details["estimated_from_reserve"] == true
    assert key_usage_events(request.id) == %{known: 0, provisional: reservation.total_tokens, admissions: 1}
  end

  defp ledger(request) do
    entries = Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request.id))
    assert Enum.all?(entries, &(&1.amount_status == "recorded"))

    assert [reservation] = Enum.filter(entries, &(&1.entry_kind == "reservation"))
    assert [settlement] = Enum.filter(entries, &(&1.entry_kind == "settlement"))
    assert [release] = Enum.filter(entries, &(&1.entry_kind == "release"))
    %{reservation: reservation, settlement: settlement, release: release}
  end

  defp request_options do
    RequestOptions.build(
      %{transport: "http_json", upstream_endpoint: "/backend-api/codex/responses"},
      "/backend-api/codex/responses",
      %{"model" => "example-model"}
    )
  end
end
