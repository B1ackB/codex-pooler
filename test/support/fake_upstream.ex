defmodule CodexPooler.FakeUpstream do
  require Logger

  @moduledoc """
  Local HTTP fake for gateway contract tests.

  The fake runs through Bandit and Plug so gateway tests exercise real local HTTP
  request/response behavior instead of mocking the client boundary.
  """

  @max_body_bytes 30 * 1024 * 1024

  defstruct [:pid, :server, :supervisor, :url]

  @type t :: %__MODULE__{pid: pid(), server: pid(), supervisor: pid(), url: String.t()}

  @typedoc """
  How an HTTP barrier-SSE reply treats the client going away mid-tail (the
  websocket transport of the same scenario writes frames and has no client
  close to observe).

    * `:fail` (default) keeps the strict complete-delivery contract: a write
      that finds the client gone raises with the scenario's owner correlator.
    * `:expected` is for cancellation scenarios that end the client path on
      purpose before releasing the held tail: the first write that finds the
      client gone stops the tail, emits no delivery acknowledgement for it, and
      records one bounded outcome readable through `sse_outcomes/1`.

  Every other write error fails in both modes.
  """
  @type barrier_sse_settings :: %{
          required(:on_client_close) => :fail | :expected,
          required(:owner) => String.t()
        }

  @type sse_outcome :: %{
          required(:scenario) => :barrier_sse,
          required(:owner) => String.t(),
          required(:outcome) => :client_closed_expected,
          required(:chunk_index) => pos_integer(),
          required(:chunk_count) => non_neg_integer(),
          required(:reason) => :closed | :enotconn | :einval | :econnaborted | :econnreset | :epipe
        }

  @type provider_validation_error :: %{
          required(String.t()) => String.t()
        }

  @type provider_validation_option ::
          {:lite, boolean()} | {:refuse_lite_async_tools, boolean()}

  @type steerable_outcome ::
          :accepted | :response_not_found | :invalid_input | :native_lane_error | :post_terminal_close

  @type websocket_steerable_settings :: %{
          response_id: String.t(),
          terminal_frames: [String.t()],
          successor_frames: [String.t()],
          notify: pid(),
          ref: reference(),
          outcome: steerable_outcome(),
          steer_id: String.t(),
          batches: :coalesced | :separate
        }

  @type websocket_steerable_option ::
          {:response_id, String.t()}
          | {:terminal_frames, [iodata()]}
          | {:successor_frames, [iodata()]}
          | {:notify, pid()}
          | {:ref, reference()}
          | {:outcome, steerable_outcome()}
          | {:steer_id, String.t()}
          | {:batches, :coalesced | :separate}

  @type websocket_steer_capture :: %{
          websocket_connection_id: pos_integer(),
          body: binary(),
          json: map()
        }

  @type mode ::
          {:json, non_neg_integer(), map()}
          | {:json_headers, non_neg_integer(), map(), [{String.t(), String.t()}]}
          | {:raw_body, non_neg_integer(), binary(), [{String.t(), String.t()}]}
          | {:chunked_body, non_neg_integer(), [binary()], [{String.t(), String.t()}]}
          | {:barrier_json, non_neg_integer(), map(), pid(), reference()}
          | {:gated_json_headers, non_neg_integer(), map(), pid(), reference()}
          | {:gated_sse_headers, [String.t()], pid(), reference()}
          | {:path_json, map()}
          | {:file_protocol, map()}
          | {:reject_json_field, String.t(), non_neg_integer(), map(), non_neg_integer(), map()}
          | {:require_json_field, String.t(), non_neg_integer(), map(), non_neg_integer(), map()}
          | {:sse, [String.t()]}
          | {:sse_headers, [String.t()], [{String.t(), String.t()}]}
          | {:delayed_sse, [String.t()], pos_integer(), pid() | nil}
          | {:delayed_terminal_sse, [String.t()], [String.t()], pid(), reference()}
          | {:gated_terminal_sse, [String.t()], [String.t()], pid(), reference()}
          | {:abrupt_close_mid_stream, [String.t()]}
          | :close_before_headers
          | {:websocket_text, [String.t()]}
          | {:websocket_text_then_abrupt_close, [String.t()]}
          | {:provider_refusal, String.t()}
          | {:websocket_sse_then_close, [String.t()], non_neg_integer(), String.t()}
          | {:websocket_terminal_then_close_barrier, String.t(), non_neg_integer(), String.t(), pid(), reference()}
          | {:websocket_connection_limit_terminal_barrier, atom(), pid(), reference()}
          | {:websocket_close_without_terminal_barrier, non_neg_integer(), String.t(), pid(), reference()}
          | {:websocket_init_barrier, mode(), pid(), reference()}
          | {:websocket_frame_barrier, [String.t()], pid(), reference()}
          | {:websocket_interruptible, [String.t()], map()}
          | {:websocket_steerable, [String.t()], websocket_steerable_settings()}
          | {:sequence, [mode()]}
          | {:strict_sequence, [mode()]}
          | {:repeat_last, [mode()]}
          | {:expect_request, keyword(), mode()}
          | {:scenario_failure, String.t()}
          | {:barrier_sse, [String.t()], non_neg_integer(), pid(), reference(), barrier_sse_settings()}
          | {:malformed_json, non_neg_integer(), String.t()}
          | {:json_error, non_neg_integer(), map()}
          | {:non_json_error, non_neg_integer(), String.t()}
          | {:timeout_before_headers, pid() | nil, reference()}
          | {:timeout_after_sse_headers, pid() | nil, reference()}
          | {:timeout_mid_stream, String.t(), pid() | nil, reference()}
          | {:websocket_upgrade_timeout, pid() | nil, reference()}
          | {:websocket_idle_timeout, pid(), reference()}
          | {:websocket_upgrade_error, non_neg_integer(), map(), [{String.t(), String.t()}], pid() | nil, reference() | nil}

  @doc "Starts a local fake upstream server for the given response mode."
  def start_link(mode, opts \\ []) do
    validate_mode!(mode)
    {supervisor_name, opts} = Keyword.pop(opts, :supervisor_name)
    supervisor_opts = if supervisor_name, do: [name: supervisor_name], else: []
    {:ok, supervisor} = CodexPooler.FakeUpstream.Supervisor.start_link(supervisor_opts)

    {:ok, pid} =
      Supervisor.start_child(
        supervisor,
        Supervisor.child_spec({Agent, fn -> initial_state(mode) end}, restart: :temporary)
      )

    bandit_options = [
      plug: {CodexPooler.FakeUpstream.Plug, pid},
      port: 0,
      ip: {127, 0, 0, 1},
      startup_log: false
    ]

    {:ok, server} =
      Supervisor.start_child(
        supervisor,
        Bandit.child_spec(Keyword.merge(bandit_options, opts))
        |> Supervisor.child_spec(restart: :temporary, significant: true)
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    {:ok,
     %__MODULE__{
       pid: pid,
       server: server,
       supervisor: supervisor,
       url: "http://127.0.0.1:#{port}"
     }}
  end

  @doc "Stops the fake upstream server."
  def stop(%__MODULE__{supervisor: supervisor}) do
    safe_stop(fn -> Supervisor.stop(supervisor) end)
    :ok
  end

  @doc "Returns the base URL for the fake upstream."
  def url(%__MODULE__{url: url}), do: url

  @doc "Sets response headers on subsequent successful websocket upgrades."
  @spec set_websocket_upgrade_headers(t(), [{String.t(), String.t()}]) :: :ok
  def set_websocket_upgrade_headers(%__MODULE__{pid: pid}, headers) when is_list(headers),
    do: Agent.update(pid, &Map.put(&1, :websocket_upgrade_headers, headers))

  @doc "Returns captured HTTP and websocket request frames in order; steering controls are separate in websocket_steers/1."
  def requests(%__MODULE__{pid: pid}) do
    Agent.get(pid, fn state -> Enum.reverse(state.requests) end)
  end

  @doc "Returns the captured request count."
  def count(fake), do: fake |> requests() |> length()

  @type physical_receipt :: %{
          ordinal: pos_integer(),
          kind: :generation | :usage | :consume | :other,
          transport: :http | :websocket,
          path: String.t(),
          connection_id: pos_integer() | nil,
          model_fingerprint: String.t() | nil,
          identity_fingerprint: String.t() | nil
        }

  @doc "Returns ordered metadata-only receipts for physical HTTP requests and websocket generation/control frames."
  @spec physical_receipts(t()) :: [physical_receipt()]
  def physical_receipts(%__MODULE__{pid: pid}) do
    Agent.get(pid, fn state -> Enum.reverse(state.physical_receipts) end)
  end

  @doc "Counts generation separately from usage, reset consumption and control-plane requests."
  @spec physical_counts(t()) :: %{required(atom()) => non_neg_integer()}
  def physical_counts(fake) do
    Enum.reduce(physical_receipts(fake), %{http_generation: 0, websocket_generation: 0, usage: 0, consume: 0, other: 0}, fn receipt, counts ->
      key = if receipt.kind == :generation, do: generation_counter(receipt.transport), else: receipt.kind
      Map.update!(counts, key, &(&1 + 1))
    end)
  end

  defp generation_counter(:http), do: :http_generation
  defp generation_counter(:websocket), do: :websocket_generation

  @doc "Builds a finite response sequence whose entries are consumed exactly once."
  @spec strict_sequence([mode()]) :: mode()
  def strict_sequence([_mode | _rest] = modes), do: {:strict_sequence, modes}

  @doc "Builds a response sequence whose final entry is intentionally repeated."
  @spec repeat_last([mode()]) :: mode()
  def repeat_last([_mode | _rest] = modes), do: {:repeat_last, modes}

  @doc "Validates selected request facts before emitting the configured response."
  @spec expect_request(keyword()) :: mode()
  def expect_request(opts) when is_list(opts) do
    respond = Keyword.fetch!(opts, :respond)
    {:expect_request, Keyword.delete(opts, :respond), respond}
  end

  @doc "Fails when a strict scenario was not consumed exactly as configured."
  @spec verify!(t()) :: :ok
  def verify!(%__MODULE__{pid: pid}) do
    state = Agent.get(pid, & &1)
    failures = Enum.reverse(state.scenario_failures)

    failures =
      if state.strict_total > state.strict_consumed do
        [
          "unused_strict_entries remaining=#{state.strict_total - state.strict_consumed} consumed=#{state.strict_consumed} total=#{state.strict_total}"
          | failures
        ]
      else
        failures
      end

    missing_acknowledgements =
      MapSet.difference(state.required_acknowledgements, state.acknowledged)

    failures =
      Enum.reduce(missing_acknowledgements, failures, fn acknowledgement, acc ->
        ["missing_required_acknowledgement acknowledgement=#{inspect(acknowledgement)}" | acc]
      end)

    case Enum.reverse(failures) do
      [] -> :ok
      failures -> raise ExUnit.AssertionError, message: Enum.join(failures, "\n")
    end
  end

  @doc "Registers a causal acknowledgement that strict scenario verification requires."
  @spec require_acknowledgement(t(), term()) :: :ok
  def require_acknowledgement(%__MODULE__{pid: pid}, acknowledgement) do
    Agent.update(pid, fn state ->
      %{
        state
        | required_acknowledgements: MapSet.put(state.required_acknowledgements, acknowledgement)
      }
    end)
  end

  @doc "Records a causal acknowledgement for strict scenario verification."
  @spec acknowledge(t(), term()) :: :ok
  def acknowledge(%__MODULE__{pid: pid}, acknowledgement) do
    Agent.update(pid, fn state ->
      %{state | acknowledged: MapSet.put(state.acknowledged, acknowledgement)}
    end)
  end

  @doc false
  @spec take_response_mode(pid(), map()) :: mode()
  def take_response_mode(pid, request) when is_pid(pid) and is_map(request) do
    Agent.get_and_update(pid, fn state ->
      {mode, state} = take_response_mode_from_state(state, request)
      {mode, record_physical_request(state, request)}
    end)
  end

  @doc "Returns the captured non-websocket request count."
  @spec http_request_count(t()) :: non_neg_integer()
  def http_request_count(fake) do
    fake
    |> requests()
    |> Enum.count(&(&1.method != "WEBSOCKET"))
  end

  def notify_websocket_controls(%__MODULE__{pid: pid}, notify) when is_pid(notify) do
    Agent.update(pid, &%{&1 | websocket_control_notify: notify})
  end

  def websocket_control_frames(%__MODULE__{pid: pid}) do
    Agent.get(pid, fn state -> Enum.reverse(state.websocket_control_frames || []) end)
  end

  def websocket_connection_count(%__MODULE__{pid: pid}) do
    Agent.get(pid, fn state -> Map.get(state, :websocket_connection_count, 0) end)
  end

  @doc """
  Waits for `count` websocket connections to have registered, and returns what
  it saw.

  The counter is written by the WebSock handler's `init/1`, which runs in its
  own process and can still be on its way there when the client already holds a
  connection. A test that goes on to read `requests/1` never notices, because a
  recorded request is itself proof that `init/1` has run — but one that asserts
  the count with no bytes exchanged has nothing else to wait on.

  Returns the count rather than `:ok` so a timeout fails the caller's own
  assertion, with the number it actually reached.
  """
  @spec await_websocket_connection_count(t(), non_neg_integer(), timeout()) ::
          non_neg_integer()
  def await_websocket_connection_count(%__MODULE__{} = upstream, count, timeout \\ 2_000) do
    poll_websocket_connection_count(
      upstream,
      count,
      System.monotonic_time(:millisecond) + timeout
    )
  end

  # Sleeping rather than spinning: the handler this waits for is a process too,
  # and on a loaded runner a busy loop competes with the thing it wants to see.
  defp poll_websocket_connection_count(upstream, count, deadline) do
    seen = websocket_connection_count(upstream)

    cond do
      seen >= count ->
        seen

      System.monotonic_time(:millisecond) >= deadline ->
        seen

      true ->
        Process.sleep(5)
        poll_websocket_connection_count(upstream, count, deadline)
    end
  end

  @spec websocket_connection_ids(t()) :: [reference()]
  def websocket_connection_ids(%__MODULE__{pid: pid}) do
    Agent.get(pid, fn state -> Enum.reverse(Map.get(state, :websocket_connection_ids, [])) end)
  end

  def close_websocket_connections(%__MODULE__{pid: pid}) do
    pids = Agent.get(pid, fn state -> Map.get(state, :websocket_pids, MapSet.new()) end)
    Enum.each(pids, &send(&1, :fake_upstream_close_websocket))
    :ok
  end

  def set_mode(%__MODULE__{pid: pid}, mode) do
    validate_mode!(mode)

    Agent.update(pid, fn state ->
      %{
        state
        | mode: mode,
          strict_total: strict_entry_count(mode),
          strict_consumed: 0,
          scenario_failures: []
      }
    end)
  end

  def json_response(payload, status \\ 200), do: {:json, status, payload}

  def json_response_with_headers(payload, headers, status \\ 200),
    do: {:json_headers, status, payload, headers}

  @spec compaction_stream(map(), [{String.t(), String.t()}]) :: mode()
  def compaction_stream(payload, headers \\ []) do
    items =
      List.wrap(Map.get(payload, "output")) ++
        Enum.map(
          List.wrap(Map.get(payload, "compaction_summary")),
          &Map.put_new(&1, "type", "compaction_summary")
        )

    events =
      Enum.map(
        items,
        &{"response.output_item.done", %{"type" => "response.output_item.done", "item" => &1}}
      )

    sse_stream(
      events ++
        [
          {"response.completed",
           %{
             "type" => "response.completed",
             "response" => Map.put_new(payload, "status", "completed")
           }}
        ],
      headers: headers
    )
  end

  def raw_response(body, opts \\ []) when is_binary(body) and is_list(opts) do
    {:raw_body, Keyword.get(opts, :status, 200), body, Keyword.get(opts, :headers, [])}
  end

  def chunked_response(chunks, opts \\ []) when is_list(chunks) and is_list(opts) do
    {:chunked_body, Keyword.get(opts, :status, 200), chunks, Keyword.get(opts, :headers, [])}
  end

  def barrier_json_response(payload, opts) when is_map(payload) and is_list(opts) do
    {:barrier_json, Keyword.get(opts, :status, 200), payload, Keyword.fetch!(opts, :notify), Keyword.fetch!(opts, :release_ref)}
  end

  def gated_json_headers(payload, opts) when is_map(payload) and is_list(opts) do
    {:gated_json_headers, Keyword.get(opts, :status, 200), payload, Keyword.fetch!(opts, :notify), Keyword.fetch!(opts, :release_ref)}
  end

  def gated_sse_headers(events, opts) when is_list(events) and is_list(opts) do
    chunks = Enum.map(events, &sse_chunk/1) ++ ["data: [DONE]\n\n"]

    {:gated_sse_headers, chunks, Keyword.fetch!(opts, :notify), Keyword.fetch!(opts, :release_ref)}
  end

  def reject_json_field(
        field,
        success_payload,
        error_payload,
        success_status \\ 200,
        error_status \\ 400
      )
      when is_binary(field) and is_map(success_payload) and is_map(error_payload) do
    {:reject_json_field, field, success_status, success_payload, error_status, error_payload}
  end

  def require_json_field(
        field,
        success_payload,
        error_payload,
        success_status \\ 200,
        error_status \\ 400
      )
      when is_binary(field) and is_map(success_payload) and is_map(error_payload) do
    {:require_json_field, field, success_status, success_payload, error_status, error_payload}
  end

  def file_protocol_success(opts \\ []), do: {:file_protocol, file_protocol_config(opts)}

  def file_protocol_unauthorized(opts \\ []) do
    {:file_protocol, opts |> file_protocol_config() |> Map.put(:mode, :unauthorized)}
  end

  def file_protocol_non_json_error(opts \\ []) do
    {:file_protocol, opts |> file_protocol_config() |> Map.put(:mode, :non_json_error)}
  end

  @doc """
  A file create that the upstream refuses with `status`.

  The file bridge relays an upstream create status verbatim, so this is the
  boundary where a throttled or unavailable upstream becomes a Codex
  Pooler-authored error envelope at that same status (findings#191).
  """
  def file_protocol_create_error(status, opts \\ []) when is_integer(status) do
    {:file_protocol,
     opts
     |> file_protocol_config()
     |> Map.merge(%{mode: :create_error, create_error_status: status})}
  end

  def file_protocol_finalize_retry(opts \\ []) do
    {:file_protocol, opts |> file_protocol_config() |> Map.put(:mode, :finalize_retry)}
  end

  def sse_stream(events, opts \\ []) do
    include_done? = Keyword.get(opts, :done, true)
    headers = Keyword.get(opts, :headers, [])

    chunks = Enum.map(events, &sse_chunk/1)
    chunks = if include_done?, do: chunks ++ ["data: [DONE]\n\n"], else: chunks

    if headers == [] do
      {:sse, chunks}
    else
      {:sse_headers, chunks, headers}
    end
  end

  @spec websocket_text_frames([iodata()]) :: mode()
  def websocket_text_frames(messages) when is_list(messages) do
    {:websocket_text, Enum.map(messages, &IO.iodata_to_binary/1)}
  end

  @doc """
  Pushes `messages` as native text frames, then drops the TCP connection
  without a websocket close frame, so the Pooler's Mint receive loop observes
  the transport close (`Mint.TransportError` with reason `:closed`) rather than
  a peer close frame or a terminal.
  """
  @spec websocket_text_frames_then_abrupt_close([iodata()]) :: mode()
  def websocket_text_frames_then_abrupt_close(messages) when is_list(messages) do
    {:websocket_text_then_abrupt_close, Enum.map(messages, &IO.iodata_to_binary/1)}
  end

  @doc """
  Native websocket analogue of `barrier_sse_stream/2`: pushes `messages` as text
  frames one at a time and holds before every push.

  Before pushing frame `n + 1`, and once more after the last frame, the fake's
  websocket handler sends `{:fake_upstream_frame_barrier, n, handler_pid,
  release_ref}` to `notify`, where `n` is the number of frames already pushed
  on this reply (`0` is the pre-visible barrier). It then waits until the test
  calls `release_frame/2` (one barrier) or `release_remaining_frames/2` (this and
  every later barrier of the reply, which still notify). The connection stays
  open throughout; while a barrier is held the handler reads nothing else, so a
  second request on the connection is consumed only after the trailing barrier.
  Every barrier is a required acknowledgement for `verify!/1`: release through
  the helpers, never by raw message, and wait for the last notification before
  verifying an auto-released tail. An empty list yields the single barrier `0`,
  which makes the consumption of a reply-less request (an ack) observable.
  """
  @spec barrier_websocket_frames([iodata()], keyword()) :: mode()
  def barrier_websocket_frames(messages, opts) when is_list(messages) and is_list(opts) do
    {:websocket_frame_barrier, Enum.map(messages, &IO.iodata_to_binary/1), Keyword.fetch!(opts, :notify), Keyword.fetch!(opts, :release_ref)}
  end

  @doc "Releases the frame barrier currently held for `release_ref` and records its acknowledgement."
  @spec release_frame(t(), reference()) :: :ok | {:error, :no_frame_barrier_waiting}
  def release_frame(%__MODULE__{pid: pid}, release_ref) when is_reference(release_ref) do
    release_frame_barrier(pid, release_ref, false)
  end

  @doc "Releases the held frame barrier for `release_ref` and every later barrier of that reply."
  @spec release_remaining_frames(t(), reference()) :: :ok | {:error, :no_frame_barrier_waiting}
  def release_remaining_frames(%__MODULE__{pid: pid}, release_ref)
      when is_reference(release_ref) do
    release_frame_barrier(pid, release_ref, true)
  end

  @doc "Retires only unreached future barriers after the exact provider handler died; reached holds still require release."
  @spec retire_frame_barriers(t(), reference(), pid()) :: :ok | {:error, :handler_alive | :handler_mismatch}
  def retire_frame_barriers(%__MODULE__{pid: pid}, release_ref, handler) when is_reference(release_ref) and is_pid(handler) do
    if Process.alive?(handler) do
      {:error, :handler_alive}
    else
      Agent.get_and_update(pid, &retire_unreached_frame_barriers(&1, release_ref, handler))
    end
  end

  defp retire_unreached_frame_barriers(state, release_ref, handler) do
    case Map.get(state.frame_barrier_progress, release_ref) do
      {^handler, reached} ->
        required =
          MapSet.reject(state.required_acknowledgements, fn
            {:frame_barrier, ^release_ref, ordinal} -> ordinal > reached
            _other -> false
          end)

        {:ok, %{state | required_acknowledgements: required}}

      _foreign ->
        {{:error, :handler_mismatch}, state}
    end
  end

  @doc """
  A native response the provider stops when its client interrupts it (Codex
  0.159.0 `response.interrupt`, findings#270 row 270-272). Pushes `opening`,
  then holds the response open until the first of:

    * a `response.interrupt` text frame naming `:response_id` on this
      connection, answered with `:interrupted` (the provider sends
      `response.interrupt.accepted`, `response.output_item.interrupted` for an
      item it had open, and the terminal `response.incomplete` with reason
      `interrupted`);
    * `release_interruptible/2`, answered with `:completion` (nobody
      interrupted it).

  Before holding, the handler sends `{:fake_upstream_interruptible_open,
  handler_pid, release_ref}` to `:notify`, and `{:fake_upstream_interrupted,
  handler_pid, release_ref}` once it answered an interrupt. Every
  `response.interrupt` frame is recorded (`websocket_interrupts/1`) and
  consumes no expectation; one that names no open response is answered, as
  the provider answers it, with a `response.interrupt.failed`
  (`response_not_in_progress`), which is not a terminal.
  """
  @spec interruptible_websocket_frames([iodata()], keyword()) :: mode()
  def interruptible_websocket_frames(opening, opts) when is_list(opening) and is_list(opts) do
    {:websocket_interruptible, Enum.map(opening, &IO.iodata_to_binary/1),
     %{
       response_id: Keyword.fetch!(opts, :response_id),
       interrupted: Enum.map(Keyword.fetch!(opts, :interrupted), &IO.iodata_to_binary/1),
       completion: Enum.map(Keyword.fetch!(opts, :completion), &IO.iodata_to_binary/1),
       notify: Keyword.fetch!(opts, :notify),
       release_ref: Keyword.fetch!(opts, :release_ref)
     }}
  end

  @doc "Completes the interruptible response held for `release_ref` as nobody interrupted it."
  @spec release_interruptible(t(), reference()) :: :ok | {:error, :no_interruptible_response}
  def release_interruptible(%__MODULE__{pid: pid}, release_ref) when is_reference(release_ref) do
    case Agent.get(pid, &get_in(&1, [Access.key(:interruptible_handlers, %{}), release_ref])) do
      handler when is_pid(handler) ->
        send(handler, {:fake_upstream_release_interruptible, release_ref})
        :ok

      nil ->
        {:error, :no_interruptible_response}
    end
  end

  @doc "Every `response.interrupt` frame the fake received, in order, with its connection."
  @spec websocket_interrupts(t()) :: [%{websocket_connection_id: pos_integer(), json: map() | nil}]
  def websocket_interrupts(%__MODULE__{pid: pid}) do
    Agent.get(pid, fn state -> state |> Map.get(:websocket_interrupts, []) |> Enum.reverse() end)
  end

  @doc """
  Holds a native response open for a measured `response.steer` outcome (findings#342).

  Required options are `:notify`, `:ref`, `:response_id`, `:terminal_frames` and
  `:successor_frames`. `:outcome` defaults to `:accepted`; the refusal arms are
  `:response_not_found`, `:invalid_input` and `:native_lane_error`. The explicit
  `:post_terminal_close` arm models the Full provider's silent Close 1000 after
  the original terminal. No arm is chosen from the request's serving shape.

  `:steer_id` defaults to `"steer_synthetic_342"`. `:batches` is `:coalesced` by
  default (acceptance, original terminal and unsolicited successor in one push),
  or `:separate`, which uses `barrier_websocket_frames/2`'s ordinal-zero and
  trailing barriers for that tail. Release those with `release_frame/2` or
  `release_remaining_frames/2`; neither opening frames nor refusal frames wait
  on a frame barrier.

  After writing `opening`, the handler sends `{:fake_upstream_steerable_open,
  handler, ref}`. A written acceptance sends `{:fake_upstream_steered, handler,
  ref}`. `release_steerable/2` writes the original terminal when no steering was
  accepted (including after a failed steer), then sends
  `{:fake_upstream_steerable_released, handler, ref}`. The released target stays
  available on this connection: a late accepted steer writes no second copy of
  the original terminal.

  Steering is captured verbatim by `websocket_steers/1`, records a physical
  control receipt, and consumes no scripted generation response. An empty
  successor list models accepted input waiting for the next explicit
  `response.create`; client-owned tools remain outside the fake.
  """
  @spec websocket_steerable([iodata()], [websocket_steerable_option()]) :: mode()
  def websocket_steerable(opening, opts) when is_list(opening) and is_list(opts) do
    settings = %{
      response_id: Keyword.fetch!(opts, :response_id),
      terminal_frames: Enum.map(Keyword.fetch!(opts, :terminal_frames), &IO.iodata_to_binary/1),
      successor_frames: Enum.map(Keyword.fetch!(opts, :successor_frames), &IO.iodata_to_binary/1),
      notify: Keyword.fetch!(opts, :notify),
      ref: Keyword.fetch!(opts, :ref),
      outcome: Keyword.get(opts, :outcome, :accepted),
      steer_id: Keyword.get(opts, :steer_id, "steer_synthetic_342"),
      batches: Keyword.get(opts, :batches, :coalesced)
    }

    validate_steerable_settings!(settings)
    {:websocket_steerable, Enum.map(opening, &IO.iodata_to_binary/1), settings}
  end

  @spec validate_steerable_settings!(websocket_steerable_settings()) :: :ok
  defp validate_steerable_settings!(settings) do
    unless valid_steerable_owner?(settings) and valid_steerable_ids?(settings) and valid_steerable_controls?(settings) do
      raise ArgumentError, "steerable websocket requires a notify PID, ref, response/steer IDs, a measured outcome and coalesced or separate batches"
    end

    :ok
  end

  defp valid_steerable_owner?(settings), do: is_pid(settings.notify) and is_reference(settings.ref)

  defp valid_steerable_ids?(settings) do
    is_binary(settings.response_id) and byte_size(settings.response_id) > 0 and
      is_binary(settings.steer_id) and String.starts_with?(settings.steer_id, "steer_") and byte_size(settings.steer_id) > 6
  end

  defp valid_steerable_controls?(settings) do
    settings.outcome in [:accepted, :response_not_found, :invalid_input, :native_lane_error, :post_terminal_close] and
      settings.batches in [:coalesced, :separate]
  end

  @doc "Writes the original terminal of the steerable response held for `ref`."
  @spec release_steerable(t(), reference()) :: :ok | {:error, :no_steerable_response}
  def release_steerable(%__MODULE__{pid: pid}, ref) when is_reference(ref) do
    case Agent.get_and_update(pid, &claim_steerable_release(&1, ref)) do
      {:ok, handler} ->
        send(handler, {:fake_upstream_release_steerable, ref})
        :ok

      error ->
        error
    end
  end

  defp claim_steerable_release(state, ref) do
    case Map.get(state.steerable_handlers, ref) do
      %{handler: handler, phase: :open} = held ->
        {{:ok, handler},
         %{
           state
           | steerable_handlers: Map.put(state.steerable_handlers, ref, %{held | phase: :releasing}),
             required_acknowledgements: MapSet.put(state.required_acknowledgements, {:steerable_release, ref})
         }}

      _not_open ->
        {{:error, :no_steerable_response}, state}
    end
  end

  @doc "Exact transient `response.steer` wire frames in receipt order, with their connection."
  @spec websocket_steers(t()) :: [websocket_steer_capture()]
  def websocket_steers(%__MODULE__{pid: pid}) do
    Agent.get(pid, fn state -> Enum.reverse(state.websocket_steers) end)
  end

  @doc false
  @spec record_websocket_steer(pid(), map()) :: :ok
  def record_websocket_steer(pid, request) when is_pid(pid) and is_map(request) do
    Agent.update(pid, fn state ->
      capture = %{websocket_connection_id: request.websocket_connection_id, body: request.body, json: request.json}
      state = record_physical_receipt(state, request)
      %{state | websocket_steers: [capture | state.websocket_steers]}
    end)
  end

  defp release_frame_barrier(pid, release_ref, remaining?) do
    case Agent.get_and_update(pid, &take_frame_barrier(&1, release_ref, remaining?)) do
      {:ok, handler} ->
        send(handler, {:fake_upstream_release_frame, release_ref})
        :ok

      error ->
        error
    end
  end

  defp take_frame_barrier(state, release_ref, remaining?) do
    case Map.pop(state.frame_barriers_waiting, release_ref) do
      {nil, _waiting} ->
        {{:error, :no_frame_barrier_waiting}, state}

      {{handler, ordinal}, waiting} ->
        auto_release =
          if remaining?,
            do: MapSet.put(state.frame_barrier_auto_release, release_ref),
            else: state.frame_barrier_auto_release

        {{:ok, handler},
         %{
           state
           | frame_barriers_waiting: waiting,
             frame_barrier_auto_release: auto_release,
             acknowledged: MapSet.put(state.acknowledged, {:frame_barrier, release_ref, ordinal})
         }}
    end
  end

  @doc false
  @spec register_frame_barriers(pid(), reference(), non_neg_integer()) :: :ok
  def register_frame_barriers(pid, release_ref, frame_count) do
    Agent.update(pid, fn state ->
      required =
        Enum.reduce(0..frame_count//1, state.required_acknowledgements, fn ordinal, acc ->
          MapSet.put(acc, {:frame_barrier, release_ref, ordinal})
        end)

      %{state | required_acknowledgements: required}
    end)
  end

  # Called by the websocket handler when it reaches barrier `ordinal`. Returns
  # whether the barrier is auto-released; otherwise it is recorded as waiting so
  # `release_frame/2` can acknowledge it synchronously from the test process.
  @doc false
  @spec reach_frame_barrier(pid(), reference(), non_neg_integer(), pid()) :: boolean()
  def reach_frame_barrier(pid, release_ref, ordinal, handler) do
    Agent.get_and_update(pid, fn state ->
      state = %{state | frame_barrier_progress: Map.put(state.frame_barrier_progress, release_ref, {handler, ordinal})}

      if MapSet.member?(state.frame_barrier_auto_release, release_ref) do
        {true,
         %{
           state
           | acknowledged: MapSet.put(state.acknowledged, {:frame_barrier, release_ref, ordinal})
         }}
      else
        {false,
         %{
           state
           | frame_barriers_waiting: Map.put(state.frame_barriers_waiting, release_ref, {handler, ordinal})
         }}
      end
    end)
  end

  def quota_exhausted_429(opts \\ []) when is_list(opts) do
    quota_type = Keyword.get(opts, :quota_type, "workspace_owner_usage_limit_reached")

    {:json_headers, 429,
     %{
       "error" => %{
         "code" => "rate_limit_exceeded",
         "message" => "synthetic account quota exhausted"
       }
     }, [{"x-codex-rate-limit-reached-type", quota_type}]}
  end

  def generic_429 do
    {:json_error, 429,
     %{
       "error" => %{
         "code" => "rate_limit_exceeded",
         "message" => "synthetic rate limit"
       }
     }}
  end

  def generic_5xx(status \\ 503) when status in 500..599 do
    {:json_error, status, %{"error" => %{"code" => "server_error", "message" => "synthetic server failure"}}}
  end

  def delayed_sse_stream(events, opts) do
    include_done? = Keyword.get(opts, :done, true)
    interval_ms = Keyword.fetch!(opts, :interval_ms)
    notify = Keyword.get(opts, :notify)

    chunks = Enum.map(events, &sse_chunk/1)
    chunks = if include_done?, do: chunks ++ ["data: [DONE]\n\n"], else: chunks

    {:delayed_sse, chunks, interval_ms, notify}
  end

  def delayed_terminal_sse_stream(events, terminal_event, opts)
      when is_list(events) and is_list(opts) do
    notify = Keyword.fetch!(opts, :notify)
    release_ref = Keyword.fetch!(opts, :release_ref)
    before_terminal = Enum.map(events, &sse_chunk/1)
    terminal = [sse_chunk(terminal_event), "data: [DONE]\n\n"]

    {:delayed_terminal_sse, before_terminal, terminal, notify, release_ref}
  end

  def gated_terminal_sse_stream(events, terminal_event, opts)
      when is_list(events) and is_list(opts) do
    before_terminal = Enum.map(events, &sse_chunk/1)
    terminal = [sse_chunk(terminal_event), "data: [DONE]\n\n"]

    {:gated_terminal_sse, before_terminal, terminal, Keyword.fetch!(opts, :notify), Keyword.fetch!(opts, :release_ref)}
  end

  def abrupt_close_mid_stream(events) when is_list(events) do
    {:abrupt_close_mid_stream, Enum.map(events, &sse_chunk/1)}
  end

  def close_before_headers, do: :close_before_headers

  @doc """
  Holds an SSE reply at chunk `barrier_after` until the owning test releases it.

  Options: `:notify` and `:release_ref` (required), `:barrier_after` (default
  1), `:done` (default true), `:on_client_close` (`:fail` by default or
  `:expected`, see `t:barrier_sse_settings/0`), and `:owner`, a bounded
  correlator (`[A-Za-z0-9_.:-]`, at most 80 bytes) that names the scenario in
  every write-failure error and recorded outcome so a CI log identifies its
  caller. It defaults to the calling test file and line (`file_test.exs:line`)
  when the fixture is built inside a test.
  """
  def barrier_sse_stream(events, opts) do
    include_done? = Keyword.get(opts, :done, true)
    barrier_after = Keyword.get(opts, :barrier_after, 1)
    notify = Keyword.fetch!(opts, :notify)
    release_ref = Keyword.fetch!(opts, :release_ref)

    chunks = Enum.map(events, &sse_chunk/1)
    chunks = if include_done?, do: chunks ++ ["data: [DONE]\n\n"], else: chunks

    {:barrier_sse, chunks, barrier_after, notify, release_ref,
     barrier_sse_settings(
       Keyword.get(opts, :on_client_close, :fail),
       Keyword.get_lazy(opts, :owner, &default_barrier_sse_owner/0)
     )}
  end

  defp barrier_sse_settings(on_client_close, owner)
       when on_client_close in [:fail, :expected] and is_binary(owner) do
    unless byte_size(owner) in 1..80 and Regex.match?(~r/^[A-Za-z0-9_.:-]+$/, owner) do
      raise ArgumentError, "barrier SSE owner must be a bounded [A-Za-z0-9_.:-] correlator"
    end

    %{on_client_close: on_client_close, owner: owner}
  end

  defp barrier_sse_settings(on_client_close, _owner) when on_client_close in [:fail, :expected],
    do: raise(ArgumentError, "barrier SSE owner must be a binary correlator")

  defp barrier_sse_settings(_on_client_close, _owner),
    do: raise(ArgumentError, "barrier SSE on_client_close must be :fail or :expected")

  # `barrier_sse_stream/2` runs in the owning test process, so the nearest
  # `*_test.exs` frame names the caller (`file.exs:line`); a scenario may still
  # pass `:owner` for a more specific label. No test frame yields the generic
  # label rather than a guess.
  #
  # Two accepted residuals (findings#226). `:current_stacktrace` is truncated to
  # the `backtrace_depth` system flag (default 8) and the test frame sits 3-4 up
  # today, so it fits; a future helper layer between the caller and this
  # function could push it past the cut, and the label would fall back to
  # `barrier_sse` rather than name the wrong caller. Pass `:owner` explicitly if
  # you add such a layer. Derived labels also carry the `.exs` extension
  # (`fake_upstream_test.exs:687`) while hand-written owners do not
  # (`fake_upstream_test:expected_close`); that is cosmetic, the charset allows
  # `.`, and the two forms stay distinguishable.
  defp default_barrier_sse_owner do
    {:current_stacktrace, frames} = Process.info(self(), :current_stacktrace)

    Enum.find_value(frames, "barrier_sse", fn
      {_module, _function, _arity, location} ->
        file = to_string(Keyword.get(location, :file, ""))
        line = Keyword.get(location, :line)

        if String.ends_with?(file, "_test.exs") and is_integer(line) do
          bounded_owner("#{Path.basename(file)}:#{line}")
        end

      _frame ->
        nil
    end)
  end

  defp bounded_owner(label) do
    label = String.replace(label, ~r/[^A-Za-z0-9_.:-]/, "-")
    if byte_size(label) in 1..80, do: label, else: "barrier_sse"
  end

  @doc """
  Bounded outcomes barrier-SSE replies recorded for this fake, oldest first.
  Today only an expected client close is recorded (`t:sse_outcome/0`).
  """
  @spec sse_outcomes(t()) :: [sse_outcome()]
  def sse_outcomes(%__MODULE__{pid: pid}) do
    Agent.get(pid, fn state -> Enum.reverse(state.sse_outcomes) end)
  end

  defmodule SseWriteError do
    @moduledoc "A barrier-SSE reply could not deliver a chunk; carries only bounded scenario metadata."
    defexception [:owner, :chunk_index, :chunk_count, :reason, :mode]

    @impl true
    def message(%__MODULE__{} = error) do
      "fake upstream barrier SSE owner=#{error.owner} mode=#{error.mode} " <>
        "chunk #{error.chunk_index}/#{error.chunk_count} write failed: #{error.reason}"
    end
  end

  def websocket_sse_then_close(events, opts \\ []) do
    code = Keyword.get(opts, :code, 1001)
    reason = Keyword.get(opts, :reason, "fake upstream closed websocket")
    chunks = Enum.map(events, &sse_chunk/1)

    {:websocket_sse_then_close, chunks, code, reason}
  end

  @spec websocket_terminal_then_close_barrier(map() | binary(), keyword()) :: mode()
  def websocket_terminal_then_close_barrier(terminal, opts)
      when (is_map(terminal) or is_binary(terminal)) and is_list(opts) do
    terminal = if is_map(terminal), do: CodexPooler.JSON.encode!(terminal), else: terminal

    {:websocket_terminal_then_close_barrier, terminal, Keyword.get(opts, :code, 1000), Keyword.get(opts, :reason, "synthetic terminal close"), Keyword.fetch!(opts, :notify), Keyword.fetch!(opts, :release_ref)}
  end

  @spec websocket_connection_limit_terminal_barrier(keyword()) :: mode()
  def websocket_connection_limit_terminal_barrier(opts) when is_list(opts) do
    shape = Keyword.fetch!(opts, :shape)

    unless shape in [:top_level, :nested] do
      raise ArgumentError, "connection-limit terminal shape must be :top_level or :nested"
    end

    {:websocket_connection_limit_terminal_barrier, shape, Keyword.fetch!(opts, :notify), Keyword.fetch!(opts, :release_ref)}
  end

  @spec websocket_close_without_terminal_barrier(keyword()) :: mode()
  def websocket_close_without_terminal_barrier(opts) when is_list(opts) do
    {:websocket_close_without_terminal_barrier, Keyword.get(opts, :code, 1000), Keyword.get(opts, :reason, "synthetic close without terminal"), Keyword.fetch!(opts, :notify), Keyword.fetch!(opts, :release_ref)}
  end

  @spec websocket_connection_alive?(t(), pos_integer()) :: boolean()
  def websocket_connection_alive?(%__MODULE__{pid: pid}, connection_id)
      when is_integer(connection_id) and connection_id > 0 do
    case Agent.get(pid, fn state ->
           state
           |> Map.get(:websocket_pids_by_connection, %{})
           |> Map.get(connection_id)
         end) do
      websocket_pid when is_pid(websocket_pid) -> Process.alive?(websocket_pid)
      _other -> false
    end
  end

  @spec close_websocket_connection(t(), pos_integer(), keyword()) :: :ok | {:error, :not_found}
  def close_websocket_connection(%__MODULE__{pid: pid}, connection_id, opts \\ [])
      when is_integer(connection_id) and connection_id > 0 and is_list(opts) do
    close_ref = Keyword.fetch!(opts, :close_ref)

    websocket_pid =
      Agent.get(pid, fn state ->
        state
        |> Map.get(:websocket_pids_by_connection, %{})
        |> Map.get(connection_id)
      end)

    if is_pid(websocket_pid) and Process.alive?(websocket_pid) do
      Agent.update(pid, fn state ->
        %{
          state
          | required_acknowledgements: MapSet.put(state.required_acknowledgements, {:peer_close, close_ref})
        }
      end)

      send(
        websocket_pid,
        {:fake_upstream_close_websocket, Keyword.get(opts, :code, 1000), Keyword.get(opts, :reason, "synthetic peer close"), Keyword.fetch!(opts, :notify), close_ref}
      )

      :ok
    else
      {:error, :not_found}
    end
  end

  def websocket_terminal_failure(code \\ "server_error") when is_binary(code) do
    websocket_text_frames([
      CodexPooler.JSON.encode!(%{
        "type" => "response.failed",
        "response" => %{
          "status" => "failed",
          "error" => %{"code" => code, "message" => "synthetic terminal failure"}
        }
      })
    ])
  end

  def websocket_close(opts \\ []) when is_list(opts) do
    websocket_sse_then_close([],
      code: Keyword.get(opts, :code, 1011),
      reason: Keyword.get(opts, :reason, "synthetic websocket close")
    )
  end

  def websocket_init_barrier(mode, opts) when is_list(opts) do
    {:websocket_init_barrier, mode, Keyword.fetch!(opts, :notify), Keyword.fetch!(opts, :release_ref)}
  end

  def malformed_json(body \\ "{not-json", status \\ 200), do: {:malformed_json, status, body}

  def http_500_json_error(payload \\ %{"error" => %{"code" => "server_error"}}) do
    {:json_error, 500, payload}
  end

  def non_json_502(body \\ "bad gateway"), do: {:non_json_error, 502, body}

  def timeout_before_headers(opts \\ []) when is_list(opts) do
    {:timeout_before_headers, Keyword.get(opts, :notify), Keyword.get(opts, :release_ref, make_ref())}
  end

  def timeout_after_sse_headers(opts \\ []) when is_list(opts) do
    {:timeout_after_sse_headers, Keyword.get(opts, :notify), Keyword.get(opts, :release_ref, make_ref())}
  end

  def timeout_mid_stream(first_chunk \\ "data: partial\n\n", opts \\ []) do
    {:timeout_mid_stream, first_chunk, Keyword.get(opts, :notify), Keyword.get(opts, :release_ref, make_ref())}
  end

  def websocket_upgrade_timeout(opts \\ []) when is_list(opts) do
    {:websocket_upgrade_timeout, Keyword.get(opts, :notify), Keyword.get(opts, :release_ref, make_ref())}
  end

  def websocket_idle_timeout(opts) when is_list(opts) do
    {:websocket_idle_timeout, Keyword.fetch!(opts, :notify), Keyword.fetch!(opts, :release_ref)}
  end

  def websocket_upgrade_error(payload, opts \\ []) when is_map(payload) and is_list(opts) do
    {:websocket_upgrade_error, Keyword.get(opts, :status, 401), payload, Keyword.get(opts, :headers, []), Keyword.get(opts, :notify), Keyword.get(opts, :release_ref)}
  end

  defp initial_state(mode) do
    %{
      mode: mode,
      requests: [],
      physical_receipts: [],
      physical_ordinal: 0,
      route_counts: %{},
      websocket_connection_count: 0,
      websocket_upgrade_headers: [],
      websocket_connection_ids: [],
      websocket_pids: MapSet.new(),
      websocket_pids_by_connection: %{},
      websocket_control_notify: nil,
      websocket_control_frames: [],
      websocket_steers: [],
      steerable_handlers: %{},
      refuse_lite_async_tools?: false,
      strict_total: strict_entry_count(mode),
      strict_consumed: 0,
      scenario_failures: [],
      sse_outcomes: [],
      required_acknowledgements: MapSet.new(),
      acknowledged: MapSet.new(),
      frame_barriers_waiting: %{},
      frame_barrier_progress: %{},
      frame_barrier_auto_release: MapSet.new()
    }
  end

  def handle(pid, conn) do
    if websocket_upgrade?(conn) do
      mode = Agent.get(pid, & &1.mode)

      handle_websocket(pid, conn, mode)
    else
      handle_http(pid, conn)
    end
  end

  defp handle_websocket(pid, conn, {:websocket_upgrade_timeout, notify, release_ref}) do
    wait_for_timeout_release(:websocket_upgrade, notify, release_ref)

    WebSockAdapter.upgrade(
      conn,
      CodexPooler.FakeUpstream.Websocket,
      %{pid: pid, mode: Agent.get(pid, & &1.mode), headers: conn.req_headers},
      []
    )
  end

  defp handle_websocket(
         pid,
         conn,
         {:websocket_upgrade_error, _status, _payload, _headers, _notify, _release_ref} = mode
       ) do
    Agent.update(pid, fn state ->
      {_mode, next_mode} = next_response_mode(state.mode)
      %{state | mode: next_mode}
    end)

    send_websocket_upgrade_error(conn, mode)
  end

  defp handle_websocket(
         pid,
         conn,
         {:sequence, [{:websocket_upgrade_error, _status, _payload, _headers, _notify, _ref} = mode | _rest]}
       ) do
    handle_websocket(pid, conn, mode)
  end

  # A strict scenario may reject the handshake itself: the leading entry (bare
  # or wrapped in `expect_request`) is consumed with the usual strict
  # accounting and its expectations are checked against the upgrade request
  # (method GET, path, headers; no JSON, no connection ordinal). The handshake
  # is not recorded as a request, matching the legacy upgrade-error path, so
  # request counts keep describing accepted connections and HTTP calls.
  defp handle_websocket(pid, conn, {kind, [head | _rest]} = mode)
       when kind in [:strict_sequence, :repeat_last] do
    if websocket_upgrade_error_head?(head) do
      reject_websocket_handshake(pid, conn)
    else
      upgrade_websocket(pid, conn, mode)
    end
  end

  # An exhausted strict scenario refuses further handshakes as extra requests
  # instead of upgrading into a connection that has nothing to serve.
  defp handle_websocket(pid, conn, {:strict_sequence, []}),
    do: reject_websocket_handshake(pid, conn)

  defp handle_websocket(pid, conn, mode), do: upgrade_websocket(pid, conn, mode)

  defp upgrade_websocket(pid, conn, mode) do
    conn =
      Enum.reduce(Agent.get(pid, & &1.websocket_upgrade_headers), conn, fn {name, value}, current ->
        Plug.Conn.put_resp_header(current, name, value)
      end)

    WebSockAdapter.upgrade(
      conn,
      CodexPooler.FakeUpstream.Websocket,
      %{pid: pid, mode: mode, headers: conn.req_headers},
      []
    )
  end

  defp reject_websocket_handshake(pid, conn) do
    handshake = %{
      method: conn.method,
      path: conn.request_path,
      query_string: conn.query_string,
      headers: conn.req_headers,
      body: "",
      json: nil
    }

    taken =
      Agent.get_and_update(pid, fn state -> take_response_mode_from_state(state, handshake) end)

    case taken do
      {:websocket_upgrade_error, _status, _payload, _headers, _notify, _ref} = error ->
        send_websocket_upgrade_error(conn, error)

      {:scenario_failure, _diagnostic} = failure ->
        respond(pid, conn, failure, handshake)
    end
  end

  defp websocket_upgrade_error_head?({:expect_request, _expectations, respond}),
    do: websocket_upgrade_error_head?(respond)

  defp websocket_upgrade_error_head?({:websocket_upgrade_error, _, _, _, _, _}), do: true
  defp websocket_upgrade_error_head?(_mode), do: false

  defp send_websocket_upgrade_error(
         conn,
         {:websocket_upgrade_error, status, payload, headers, notify, release_ref}
       ) do
    if is_pid(notify) do
      wait_for_timeout_release(:before_headers, notify, release_ref)
    end

    conn =
      Enum.reduce(headers, conn, fn {key, value}, conn ->
        Plug.Conn.put_resp_header(conn, key, value)
      end)

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, CodexPooler.JSON.encode!(payload))
  end

  defp handle_http(pid, conn) do
    {:ok, body, conn} = read_body(conn)

    request = %{
      method: conn.method,
      path: conn.request_path,
      query_string: conn.query_string,
      headers: conn.req_headers,
      body: body,
      json: decode_json(body)
    }

    cond do
      provider_rejects_http_model?(pid, request) ->
        record_rejected_request(pid, request)
        respond_http_model_rejection(conn, request.json["model"])

      provider_rejects_http_previous_response_id?(request) ->
        record_rejected_request(pid, request)
        respond_http_previous_response_id_rejection(conn)

      refusal = provider_request_refusal(request) ->
        record_rejected_request(pid, request)
        respond_provider_refusal(conn, refusal)

      error = provider_validation_rejection(pid, request) ->
        record_rejected_request(pid, request)
        respond_provider_validation_rejection(conn, error)

      true ->
        mode = take_response_mode(pid, request)

        respond(pid, conn, mode, request)
    end
  end

  # The ChatGPT Codex backend resolves `previous_response_id` only on the
  # websocket connection that produced the response. Over HTTP it refuses the
  # parameter before any lookup, whatever `store` or a forwarded `session-id`
  # (findings#232 rows 232-275 and 232-276, live probe 2026-09-23), with this
  # exact body (a 43-byte detail). The fake answers the same way so no test can
  # certify an anchored HTTP continuation the provider never serves; the
  # refused request is still captured, and no scripted response is consumed
  # because the provider generates nothing for it.
  @http_previous_response_id_rejection ~s({"detail":"Unsupported parameter: previous_response_id"})

  @doc "The body the Codex backend answers, with status 400, to `previous_response_id` over HTTP."
  @spec http_previous_response_id_rejection_body() :: String.t()
  def http_previous_response_id_rejection_body, do: @http_previous_response_id_rejection

  defp provider_rejects_http_previous_response_id?(%{method: "POST", path: path, json: %{} = json}) do
    String.ends_with?(path, "/codex/responses") and not is_nil(Map.get(json, "previous_response_id"))
  end

  defp provider_rejects_http_previous_response_id?(_request), do: false

  # The Codex backend refuses these request fields before it generates anything (direct probe 2026-10-06,
  # findings#333, `gpt-6-luna`, Full and Lite request shapes, HTTP and websocket): a top-level `metadata` (an empty
  # object included) and a top-level `tools` entry of type `programmatic_tool_calling` or `web_search_preview` (the
  # Full shape; a Lite `additional_tools` manifest accepts `programmatic_tool_calling`). Over HTTP the answer is
  # `400 {"detail": "Unsupported parameter: metadata"}` or `400 {"detail": "Unsupported tool type: <type>"}`; on the
  # websocket it is the wrapped error frame with the same text and no code (`provider_refusal_frame/1`), after which the
  # provider answers nothing more on that connection and drops it, without a Close frame, about 3 s later. The fake
  # answers both transports the same way, so no test can certify a request the provider refuses: the refused request
  # is captured and no scripted response is consumed.
  @refused_tool_types ~w(programmatic_tool_calling web_search_preview)

  @doc "The provider's refusal text for a request it refuses before generating, or `nil` (findings#333)."
  @spec provider_refusal_message(term()) :: String.t() | nil
  def provider_refusal_message(%{"metadata" => _metadata}), do: "Unsupported parameter: metadata"

  def provider_refusal_message(%{"tools" => tools}) when is_list(tools) do
    Enum.find_value(tools, fn
      %{"type" => type} when type in @refused_tool_types -> "Unsupported tool type: " <> type
      _tool -> nil
    end)
  end

  def provider_refusal_message(_json), do: nil

  @doc """
  The wrapped error frame the Codex backend's websocket answers to a request it refuses before generating, for the
  refusals whose HTTP answer is a `{"detail": ...}` body (direct probes 2026-10-06 and 2026-10-07, findings#333 and
  findings#336): the HTTP detail's text in `error.message` and `error.type`, and no `code` and no `param` key at all.
  The frames of the validators that answer HTTP with a coded error carry `code` and `param` as null and the message in
  one of two wordings; the tests that need them script the captured frame text with `websocket_text_frames/1`.
  """
  @spec provider_refusal_frame(String.t()) :: String.t()
  def provider_refusal_frame(message) when is_binary(message) do
    CodexPooler.JSON.encode!(%{
      "type" => "error",
      "status" => 400,
      "error" => %{"type" => "invalid_request_error", "message" => message}
    })
  end

  @doc """
  A scripted provider refusal: `400 {"detail": message}` over HTTP, and over the websocket the codeless wrapped error
  frame with the same text, after which the connection answers nothing more and is dropped without a Close frame when
  the next request frame arrives on it (the provider drops it about 3 s after the refusal, ignoring what it receives
  meanwhile; direct probe 2026-10-06, findings#333).
  """
  @spec provider_refusal(String.t()) :: mode()
  def provider_refusal(message) when is_binary(message), do: {:provider_refusal, message}

  defp provider_request_refusal(%{method: "POST", path: path, json: %{} = json}) do
    if String.ends_with?(path, "/codex/responses"), do: provider_refusal_message(json)
  end

  defp provider_request_refusal(_request), do: nil

  defp respond_provider_refusal(conn, message) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(400, CodexPooler.JSON.encode!(%{"detail" => message}))
  end

  @provider_reasoning_efforts ~w(none minimal low medium high xhigh max)
  @lite_async_refusal_message "X-OpenAI-Internal-Codex-Responses-Lite does not support async tools because they inject acknowledgments and resume sampling."

  @doc """
  Opts this fake into the provider's intermittent Lite `async: true` refusal
  (findings#343). The default remains acceptance; Full and `async: false`
  requests are unaffected. The refusal is coded `unsupported_value` at `tools`
  on both HTTP and websocket, and consumes no scripted response.
  """
  @spec refuse_lite_async_tools(t()) :: :ok
  def refuse_lite_async_tools(%__MODULE__{pid: pid}) do
    Agent.update(pid, &%{&1 | refuse_lite_async_tools?: true})
  end

  @doc "Measured #343 provider validation error, with the physical request's indexed parameter path."
  @spec provider_validation_error(term(), [provider_validation_option()]) :: provider_validation_error() | nil
  def provider_validation_error(json, opts \\ [])

  def provider_validation_error(%{} = json, opts) when is_list(opts) do
    configuration_update_error(Map.get(json, "input", [])) ||
      async_tool_error(json, opts)
  end

  def provider_validation_error(_json, _opts), do: nil

  @doc false
  @spec provider_validation_rejection(pid(), map()) :: provider_validation_error() | nil
  def provider_validation_rejection(pid, %{method: method, path: path, json: json, headers: headers})
      when method in ["POST", "WEBSOCKET"] do
    if String.ends_with?(path, "/codex/responses") do
      refuse_async? = Agent.get(pid, & &1.refuse_lite_async_tools?)
      provider_validation_error(json, lite: lite_request?(json, headers), refuse_lite_async_tools: refuse_async?)
    end
  end

  def provider_validation_rejection(_pid, _request), do: nil

  @doc "The coded websocket error frame for the measured #343 refusal arm; the connection remains usable."
  @spec provider_validation_error_frame(provider_validation_error()) :: String.t()
  def provider_validation_error_frame(error) when is_map(error) do
    CodexPooler.JSON.encode!(%{"type" => "error", "status" => 400, "error" => error})
  end

  @spec respond_provider_validation_rejection(Plug.Conn.t(), provider_validation_error()) :: Plug.Conn.t()
  defp respond_provider_validation_rejection(conn, error) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(400, CodexPooler.JSON.encode!(%{"error" => error}))
  end

  @spec lite_request?(term(), [{String.t(), String.t()}]) :: boolean()
  defp lite_request?(json, headers) do
    header_lite? =
      Enum.any?(headers, fn {name, value} ->
        String.downcase(name) == "x-openai-internal-codex-responses-lite" and value == "true"
      end)

    marker_lite? =
      case json do
        %{"client_metadata" => %{"ws_request_header_x_openai_internal_codex_responses_lite" => marker}} -> marker in [true, "true"]
        _other -> false
      end

    header_lite? or marker_lite?
  end

  @spec configuration_update_error(term()) :: provider_validation_error() | nil
  defp configuration_update_error(input) when is_list(input) do
    input
    |> Enum.with_index()
    |> Enum.reduce_while(nil, &reduce_configuration_update/2)
    |> case do
      %{} = error -> error
      _last_type -> nil
    end
  end

  defp configuration_update_error(_input), do: nil

  defp reduce_configuration_update({%{"type" => "configuration_update"} = item, index}, previous_type) do
    error =
      if previous_type == "configuration_update" do
        validation_error("unsupported_value", "input[#{index}].type", "Consecutive 'configuration_update' items are not allowed.")
      else
        configuration_update_reasoning_error(item, "input[#{index}].reasoning")
      end

    if error, do: {:halt, error}, else: {:cont, "configuration_update"}
  end

  defp reduce_configuration_update(_item, _previous_type), do: {:cont, nil}

  @spec configuration_update_reasoning_error(map(), String.t()) :: provider_validation_error() | nil
  defp configuration_update_reasoning_error(item, path) do
    case Map.fetch(item, "reasoning") do
      :error ->
        validation_error("missing_required_parameter", path, "Missing required parameter: '#{path}'.")

      {:ok, %{} = reasoning} ->
        unknown = reasoning |> Map.keys() |> Enum.sort() |> Enum.find(&(&1 != "effort"))

        if unknown do
          validation_error("unknown_parameter", path <> "." <> unknown, "Unknown parameter: '#{path}.#{unknown}'.")
        else
          configuration_update_effort_error(reasoning, path <> ".effort")
        end

      {:ok, _not_object} ->
        validation_error("invalid_type", path, "Invalid type for '#{path}': expected an object.")
    end
  end

  @spec configuration_update_effort_error(map(), String.t()) :: provider_validation_error() | nil
  defp configuration_update_effort_error(reasoning, path) do
    case Map.fetch(reasoning, "effort") do
      :error ->
        validation_error("missing_required_parameter", path, "Missing required parameter: '#{path}'.")

      {:ok, effort} when effort in @provider_reasoning_efforts ->
        nil

      {:ok, effort} when is_binary(effort) ->
        validation_error("invalid_value", path, "Invalid reasoning effort. Supported values are: 'none', 'minimal', 'low', 'medium', 'high', 'xhigh', and 'max'.")

      {:ok, _not_string} ->
        validation_error("invalid_type", path, "Invalid type for '#{path}': expected a string.")
    end
  end

  @spec async_tool_error(map(), [provider_validation_option()]) :: provider_validation_error() | nil
  defp async_tool_error(json, opts) do
    groups = tools_with_paths(json)

    Enum.find_value(groups, fn {tools, path} -> async_tool_shape_error(tools, path) end) ||
      if Keyword.get(opts, :lite, false) and Keyword.get(opts, :refuse_lite_async_tools, false) and
           Enum.any?(groups, fn {tools, _path} -> async_tools?(tools) end) do
        validation_error("unsupported_value", "tools", @lite_async_refusal_message)
      end
  end

  @spec tools_with_paths(map()) :: [{list(), String.t()}]
  defp tools_with_paths(json) do
    full = if is_list(json["tools"]), do: [{json["tools"], "tools"}], else: []

    manifest =
      case json["input"] do
        input when is_list(input) ->
          input
          |> Enum.with_index()
          |> Enum.flat_map(fn
            {%{"type" => "additional_tools", "tools" => tools}, index} when is_list(tools) -> [{tools, "input[#{index}].tools"}]
            _item -> []
          end)

        _not_list ->
          []
      end

    full ++ manifest
  end

  @spec async_tool_shape_error(list(), String.t()) :: provider_validation_error() | nil
  defp async_tool_shape_error(tools, path) do
    tools
    |> Enum.with_index()
    |> Enum.find_value(fn
      {%{"type" => "namespace"} = tool, index} ->
        if Map.has_key?(tool, "async") do
          param = "#{path}[#{index}].async"
          validation_error("unknown_parameter", param, "Unknown parameter: '#{param}'.")
        else
          namespace_async_tool_shape_error(tool, "#{path}[#{index}].tools")
        end

      {%{"type" => type, "async" => async}, index} when type in ["function", "custom"] and not is_boolean(async) ->
        param = "#{path}[#{index}].async"
        validation_error("invalid_type", param, "Invalid type for '#{param}': expected a boolean.")

      _tool ->
        nil
    end)
  end

  defp namespace_async_tool_shape_error(%{"tools" => tools}, path) when is_list(tools), do: async_tool_shape_error(tools, path)
  defp namespace_async_tool_shape_error(_tool, _path), do: nil

  @spec async_tools?(list()) :: boolean()
  defp async_tools?(tools) do
    Enum.any?(tools, fn
      %{"type" => type, "async" => true} when type in ["function", "custom"] -> true
      %{"type" => "namespace", "tools" => members} when is_list(members) -> async_tools?(members)
      _tool -> false
    end)
  end

  @spec validation_error(String.t(), String.t(), String.t()) :: provider_validation_error()
  defp validation_error(code, param, message) do
    %{"type" => "invalid_request_error", "code" => code, "param" => param, "message" => message}
  end

  @doc """
  Makes the fake refuse `model` on `POST .../codex/responses` the way the
  Codex backend refuses a model the ChatGPT account cannot serve: `400`
  `{"detail": "The '<model>' model is not supported when using Codex with a
  ChatGPT account."}`. Over HTTP the backend checks the model before
  `previous_response_id`, so this refusal answers an anchored request too
  (findings#232 row 232-279, live probe 2026-09-23). Like the anchor refusal,
  the request is captured and no scripted response is consumed.
  """
  @spec refuse_http_model(t(), String.t()) :: :ok
  def refuse_http_model(%__MODULE__{pid: pid}, model) when is_binary(model) do
    Agent.update(pid, fn state -> Map.update(state, :refused_http_models, MapSet.new([model]), &MapSet.put(&1, model)) end)
  end

  @doc "The body the Codex backend answers, with status 400, to a model the ChatGPT account cannot serve."
  @spec http_model_rejection_body(String.t()) :: String.t()
  def http_model_rejection_body(model) when is_binary(model),
    do: CodexPooler.JSON.encode!(%{"detail" => "The '#{model}' model is not supported when using Codex with a ChatGPT account."})

  defp provider_rejects_http_model?(pid, %{method: "POST", path: path, json: %{"model" => model}}) when is_binary(model) do
    String.ends_with?(path, "/codex/responses") and
      Agent.get(pid, fn state -> state |> Map.get(:refused_http_models, MapSet.new()) |> MapSet.member?(model) end)
  end

  defp provider_rejects_http_model?(_pid, _request), do: false

  defp respond_http_model_rejection(conn, model) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(400, http_model_rejection_body(model))
  end

  defp record_rejected_request(pid, request) do
    Agent.update(pid, &record_physical_request(&1, request))
  end

  @doc false
  @spec record_unanswered_request(pid(), map()) :: :ok
  def record_unanswered_request(pid, request) when is_pid(pid) and is_map(request), do: record_rejected_request(pid, request)

  defp record_physical_request(state, request) do
    state
    |> record_physical_receipt(request)
    |> Map.update!(:requests, &[request | &1])
  end

  defp record_physical_receipt(state, request) do
    ordinal = state.physical_ordinal + 1
    transport = if request.method == "WEBSOCKET", do: :websocket, else: :http
    json = if is_map(request.json), do: request.json, else: %{}
    headers = Map.new(request.headers, fn {key, value} -> {String.downcase(key), value} end)

    receipt = %{
      ordinal: ordinal,
      kind: physical_request_kind(request, json),
      transport: transport,
      path: request.path,
      connection_id: Map.get(request, :websocket_connection_id),
      model_fingerprint: physical_fingerprint(json["model"]),
      identity_fingerprint: physical_fingerprint(headers["chatgpt-account-id"])
    }

    %{state | physical_receipts: [receipt | state.physical_receipts], physical_ordinal: ordinal}
  end

  defp physical_request_kind(%{method: "WEBSOCKET"}, %{"type" => type})
       when type in ["response.create", "response.compact"], do: :generation

  defp physical_request_kind(%{method: "WEBSOCKET"}, _json), do: :other

  defp physical_request_kind(%{method: "POST", path: path}, _json)
       when path in ["/backend-api/codex/responses", "/backend-api/codex/responses/compact", "/v1/responses", "/v1/responses/compact"], do: :generation

  defp physical_request_kind(%{method: "POST", path: path}, _json)
       when path in ["/backend-api/wham/rate-limit-reset-credits/consume", "/wham/rate-limit-reset-credits/consume", "/api/codex/rate-limit-reset-credits/consume", "/backend-api/codex/rate-limit-reset-credits/consume"], do: :consume

  defp physical_request_kind(%{method: "GET", path: path}, _json)
       when path in ["/api/codex/usage", "/backend-api/codex/usage", "/backend-api/wham/usage", "/wham/usage"], do: :usage

  defp physical_request_kind(_request, _json), do: :other

  defp physical_fingerprint(value) when is_binary(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
  defp physical_fingerprint(_value), do: nil

  defp respond_http_previous_response_id_rejection(conn) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(400, @http_previous_response_id_rejection)
  end

  defp read_body(conn), do: read_body(conn, [])

  defp read_body(conn, chunks) do
    case Plug.Conn.read_body(conn, length: @max_body_bytes, read_length: @max_body_bytes) do
      {:ok, body, conn} -> {:ok, IO.iodata_to_binary(Enum.reverse([body | chunks])), conn}
      {:more, body, conn} -> read_body(conn, [body | chunks])
    end
  end

  defp websocket_upgrade?(conn) do
    conn
    |> Plug.Conn.get_req_header("upgrade")
    |> Enum.any?(&(String.downcase(&1) == "websocket"))
  end

  defp next_response_mode({:sequence, [mode]}), do: {mode, mode}

  defp next_response_mode({:sequence, [mode | remaining]}), do: {mode, {:sequence, remaining}}

  defp next_response_mode({:strict_sequence, [mode | remaining]}),
    do: {mode, {:strict_sequence, remaining}}

  defp next_response_mode({:repeat_last, [mode]}), do: {mode, {:repeat_last, [mode]}}

  defp next_response_mode({:repeat_last, [mode | remaining]}),
    do: {mode, {:repeat_last, remaining}}

  defp next_response_mode(mode), do: {mode, mode}

  defp take_response_mode_from_state(%{mode: {:strict_sequence, []}} = state, request) do
    failure =
      "unexpected_extra_request transport=#{request_transport(request)} method=#{request.method} path=#{request.path} websocket_connection_ordinal=#{inspect(Map.get(request, :websocket_connection_id))} remaining=0 consumed=#{state.strict_consumed}"

    Logger.warning("fake upstream scenario failure " <> failure)

    {{:scenario_failure, failure}, %{state | scenario_failures: [failure | state.scenario_failures]}}
  end

  defp take_response_mode_from_state(state, request) do
    {mode, next_mode} = next_response_mode(state.mode)

    strict_consumed =
      state.strict_consumed + if(match?({:strict_sequence, _}, state.mode), do: 1, else: 0)

    state = %{state | mode: next_mode, strict_consumed: strict_consumed}

    case mode do
      {:expect_request, expectations, respond} ->
        case expectation_failures(expectations, request) do
          [] ->
            {respond, state}

          failures ->
            diagnostic = "expectation_mismatch " <> Enum.join(failures, " ")
            # Surface the exact field path in the test output even when the
            # owning test fails on a downstream assertion before `verify!/1`.
            Logger.warning("fake upstream scenario failure " <> diagnostic)

            {{:scenario_failure, diagnostic}, %{state | scenario_failures: [diagnostic | state.scenario_failures]}}
        end

      mode ->
        {mode, state}
    end
  end

  defp request_transport(%{method: "WEBSOCKET"}), do: "websocket"
  defp request_transport(_request), do: "http"

  defp expectation_failures(expectations, request) do
    []
    |> compare_scalar("method", Keyword.get(expectations, :method), request.method)
    |> compare_scalar("path", Keyword.get(expectations, :path), request.path)
    |> compare_scalar(
      "websocket_connection_ordinal",
      Keyword.get(expectations, :websocket_connection_ordinal),
      Map.get(request, :websocket_connection_id)
    )
    |> validate_headers(Keyword.get(expectations, :headers, []), request.headers)
    |> validate_json(Keyword.get(expectations, :json, []), request.json)
  end

  defp compare_scalar(failures, _field, nil, _actual), do: failures

  defp compare_scalar(failures, _field, expected, actual) when expected == actual, do: failures

  defp compare_scalar(failures, field, expected, actual),
    do: failures ++ [diagnostic(field, expected, actual)]

  defp validate_headers(failures, expectations, headers) do
    headers =
      Map.new(headers, fn {name, value} -> {String.downcase(to_string(name)), value} end)

    failures =
      expectations
      |> Keyword.get(:required, %{})
      |> Enum.sort_by(fn {name, _value} -> name end)
      |> Enum.reduce(failures, fn {name, expected}, acc ->
        compare_scalar(
          acc,
          "headers.#{String.downcase(name)}",
          expected,
          Map.get(headers, String.downcase(name), :missing)
        )
      end)

    expectations
    |> Keyword.get(:forbidden, [])
    |> Enum.sort()
    |> Enum.reduce(failures, fn name, acc ->
      name = String.downcase(name)

      case Map.fetch(headers, name) do
        :error -> acc
        {:ok, actual} -> acc ++ [diagnostic("headers.#{name}", :forbidden, actual)]
      end
    end)
  end

  defp validate_json(failures, expectations, json) do
    failures =
      if Keyword.get(expectations, :valid, false) and not (is_map(json) or is_list(json)) do
        failures ++ [diagnostic("json", :valid_json, :invalid_json)]
      else
        failures
      end

    failures
    |> validate_required_json_paths(Keyword.get(expectations, :required, []), json)
    |> validate_forbidden_json_paths(Keyword.get(expectations, :forbidden, []), json)
    |> validate_equal_json_paths(Keyword.get(expectations, :equals, %{}), json)
  end

  defp validate_required_json_paths(failures, paths, json) do
    Enum.reduce(paths, failures, fn path, acc ->
      case fetch_json_path(json, path) do
        {:ok, _value} -> acc
        :error -> acc ++ [diagnostic("json.#{path_string(path)}", :required, :missing)]
      end
    end)
  end

  defp validate_forbidden_json_paths(failures, paths, json) do
    Enum.reduce(paths, failures, fn path, acc ->
      case fetch_json_path(json, path) do
        :error -> acc
        {:ok, actual} -> acc ++ [diagnostic("json.#{path_string(path)}", :forbidden, actual)]
      end
    end)
  end

  defp validate_equal_json_paths(failures, paths, json) do
    paths
    |> Enum.sort_by(fn {path, _value} -> path_string(path) end)
    |> Enum.reduce(failures, fn {path, expected}, acc ->
      actual =
        case fetch_json_path(json, path) do
          {:ok, value} -> value
          :error -> :missing
        end

      compare_scalar(acc, "json.#{path_string(path)}", expected, actual)
    end)
  end

  defp fetch_json_path(json, path) do
    path
    |> path_segments()
    |> Enum.reduce_while({:ok, json}, fn segment, {:ok, value} ->
      case fetch_segment(value, segment) do
        {:ok, next} -> {:cont, {:ok, next}}
        :error -> {:halt, :error}
      end
    end)
  end

  defp fetch_segment(map, segment) when is_map(map), do: Map.fetch(map, segment)

  defp fetch_segment(list, segment) when is_list(list) do
    with {index, ""} <- Integer.parse(segment),
         true <- index >= 0,
         {:ok, value} <- Enum.fetch(list, index) do
      {:ok, value}
    else
      _other -> :error
    end
  end

  defp fetch_segment(_value, _segment), do: :error

  defp path_segments(path) when is_binary(path), do: String.split(path, ".", trim: true)
  defp path_segments(path) when is_list(path), do: Enum.map(path, &to_string/1)
  defp path_string(path), do: path |> path_segments() |> Enum.join(".")

  defp diagnostic(field, expected, actual),
    do: "field=#{field} expected=#{inspect(expected)} actual=#{inspect(actual)}"

  defp strict_entry_count({:strict_sequence, modes}), do: length(modes)
  defp strict_entry_count(_mode), do: 0

  defp validate_mode!({:strict_sequence, modes}), do: Enum.each(modes, &validate_mode!/1)
  defp validate_mode!({:repeat_last, modes}), do: Enum.each(modes, &validate_mode!/1)

  defp validate_mode!({:expect_request, expectations, respond}) do
    if Keyword.get(expectations, :method) == "WEBSOCKET" and not native_websocket_mode?(respond) do
      raise ArgumentError, "native websocket expectation requires a native websocket response mode"
    end

    validate_mode!(respond)
  end

  defp validate_mode!(_mode), do: :ok

  defp native_websocket_mode?({:websocket_text, _messages}), do: true
  defp native_websocket_mode?({:websocket_text_then_abrupt_close, _messages}), do: true
  defp native_websocket_mode?({:websocket_frame_barrier, _, _, _}), do: true
  defp native_websocket_mode?({:websocket_interruptible, _opening, _spec}), do: true
  defp native_websocket_mode?({:websocket_steerable, _opening, _settings}), do: true
  defp native_websocket_mode?({:websocket_sse_then_close, _chunks, _code, _reason}), do: true
  defp native_websocket_mode?({:websocket_terminal_then_close_barrier, _, _, _, _, _}), do: true
  defp native_websocket_mode?({:websocket_connection_limit_terminal_barrier, _, _, _}), do: true
  defp native_websocket_mode?({:websocket_close_without_terminal_barrier, _, _, _, _}), do: true
  defp native_websocket_mode?({:websocket_upgrade_error, _, _, _, _, _}), do: true
  defp native_websocket_mode?({:provider_refusal, _message}), do: true
  defp native_websocket_mode?(_mode), do: false

  defp respond(_pid, conn, {:provider_refusal, message}, _request), do: respond_provider_refusal(conn, message)

  defp respond(_pid, conn, {:scenario_failure, _diagnostic}, _request) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(
      500,
      CodexPooler.JSON.encode!(%{"error" => %{"code" => "fake_upstream_scenario_failure"}})
    )
  end

  defp respond(_pid, conn, {:json, status, payload}, _request) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, CodexPooler.JSON.encode!(payload))
  end

  defp respond(_pid, conn, {:json_headers, status, payload, headers}, _request) do
    conn =
      Enum.reduce(headers, conn, fn {key, value}, conn ->
        Plug.Conn.put_resp_header(conn, key, value)
      end)

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, CodexPooler.JSON.encode!(payload))
  end

  defp respond(_pid, conn, {:raw_body, status, body, headers}, _request) do
    conn =
      Enum.reduce(headers, conn, fn {key, value}, conn ->
        Plug.Conn.put_resp_header(conn, key, value)
      end)

    Plug.Conn.send_resp(conn, status, body)
  end

  defp respond(_pid, conn, {:chunked_body, status, chunks, headers}, _request) do
    conn =
      Enum.reduce(headers, conn, fn {key, value}, conn ->
        Plug.Conn.put_resp_header(conn, key, value)
      end)

    conn = Plug.Conn.send_chunked(conn, status)

    Enum.reduce(chunks, conn, fn chunk, conn ->
      {:ok, conn} = Plug.Conn.chunk(conn, chunk)
      conn
    end)
  end

  defp respond(_pid, conn, {:barrier_json, status, payload, notify, release_ref}, _request) do
    wait_for_timeout_release(:before_headers, notify, release_ref)

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, CodexPooler.JSON.encode!(payload))
  end

  defp respond(
         _pid,
         conn,
         {:gated_json_headers, status, payload, notify, release_ref},
         _request
       ) do
    wait_for_gate_release(:before_headers, notify, release_ref)

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, CodexPooler.JSON.encode!(payload))
  end

  defp respond(_pid, conn, {:gated_sse_headers, chunks, notify, release_ref}, _request) do
    wait_for_gate_release(:before_headers, notify, release_ref)
    conn = start_sse_response(conn)

    Enum.reduce(chunks, conn, fn chunk, conn ->
      {:ok, conn} = Plug.Conn.chunk(conn, chunk)
      conn
    end)
  end

  defp respond(pid, conn, {:path_json, routes}, request) do
    case Map.get(routes, conn.request_path) do
      {status, payload} ->
        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.send_resp(status, CodexPooler.JSON.encode!(payload))

      nil ->
        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.send_resp(404, CodexPooler.JSON.encode!(%{"error" => "not found"}))

      mode ->
        respond(pid, conn, mode, request)
    end
  end

  defp respond(pid, conn, {:file_protocol, config}, request) do
    file_id = config.file_id

    case {request.method, request.path} do
      {"POST", "/backend-api/files"} ->
        file_protocol_create_response(conn, config)

      {"POST", "/backend-api/files/" <> rest} ->
        case String.split(rest, "/") do
          [^file_id, "uploaded"] ->
            file_protocol_finalize_response(pid, conn, config, request)

          _other ->
            conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.send_resp(
              404,
              CodexPooler.JSON.encode!(%{"error" => %{"code" => "file_not_found"}})
            )
        end

      {"PUT", "/upload/" <> rest} ->
        if String.split(rest, "?") |> List.first() == file_id do
          Plug.Conn.send_resp(conn, config.upload_status, config.upload_body)
        else
          conn
          |> Plug.Conn.put_resp_content_type("application/json")
          |> Plug.Conn.send_resp(
            404,
            CodexPooler.JSON.encode!(%{"error" => %{"code" => "file_not_found"}})
          )
        end

      _other ->
        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.send_resp(404, CodexPooler.JSON.encode!(%{"error" => "not found"}))
    end
  end

  defp respond(
         _pid,
         conn,
         {:reject_json_field, field, success_status, success_payload, error_status, error_payload},
         request
       ) do
    if is_map(request.json) and Map.has_key?(request.json, field) do
      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(error_status, CodexPooler.JSON.encode!(error_payload))
    else
      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(success_status, CodexPooler.JSON.encode!(success_payload))
    end
  end

  defp respond(
         _pid,
         conn,
         {:require_json_field, field, success_status, success_payload, error_status, error_payload},
         request
       ) do
    if is_map(request.json) and Map.has_key?(request.json, field) do
      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(success_status, CodexPooler.JSON.encode!(success_payload))
    else
      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(error_status, CodexPooler.JSON.encode!(error_payload))
    end
  end

  defp respond(_pid, conn, {:sse, chunks}, _request) do
    conn =
      conn
      |> Plug.Conn.put_resp_header("cache-control", "no-cache")
      |> Plug.Conn.put_resp_content_type("text/event-stream")
      |> Plug.Conn.send_chunked(200)

    Enum.reduce(chunks, conn, fn chunk, conn ->
      {:ok, conn} = Plug.Conn.chunk(conn, chunk)
      conn
    end)
  end

  defp respond(pid, conn, {:sse_headers, chunks, headers}, request) do
    conn =
      Enum.reduce(headers, conn, fn {key, value}, conn ->
        Plug.Conn.put_resp_header(conn, key, value)
      end)

    respond(pid, conn, {:sse, chunks}, request)
  end

  defp respond(_pid, conn, {:delayed_sse, chunks, interval_ms, notify}, _request) do
    conn =
      conn
      |> Plug.Conn.put_resp_header("cache-control", "no-cache")
      |> Plug.Conn.put_resp_content_type("text/event-stream")
      |> Plug.Conn.send_chunked(200)

    chunks
    |> Enum.with_index(1)
    |> Enum.reduce(conn, fn {chunk, index}, conn ->
      if index > 1, do: wait_for_delay(interval_ms)
      {:ok, conn} = Plug.Conn.chunk(conn, chunk)
      notify_chunk_sent(notify, index)
      conn
    end)
  end

  defp respond(
         _pid,
         conn,
         {:delayed_terminal_sse, before_terminal, terminal, notify, release_ref},
         _request
       ) do
    conn = start_sse_response(conn)

    conn =
      Enum.reduce(before_terminal, conn, fn chunk, conn ->
        {:ok, conn} = Plug.Conn.chunk(conn, chunk)
        conn
      end)

    wait_for_timeout_release(:before_terminal, notify, release_ref)

    Enum.reduce(terminal, conn, fn chunk, conn ->
      {:ok, conn} = Plug.Conn.chunk(conn, chunk)
      conn
    end)
  end

  defp respond(
         _pid,
         conn,
         {:gated_terminal_sse, before_terminal, terminal, notify, release_ref},
         _request
       ) do
    conn = start_sse_response(conn)

    conn =
      Enum.reduce(before_terminal, conn, fn chunk, conn ->
        {:ok, conn} = Plug.Conn.chunk(conn, chunk)
        conn
      end)

    wait_for_gate_release(:before_terminal, notify, release_ref)

    Enum.reduce(terminal, conn, fn chunk, conn ->
      {:ok, conn} = Plug.Conn.chunk(conn, chunk)
      conn
    end)
  end

  defp respond(_pid, conn, {:abrupt_close_mid_stream, chunks}, _request) do
    conn =
      conn
      |> Plug.Conn.put_resp_header("cache-control", "no-cache")
      |> Plug.Conn.put_resp_content_type("text/event-stream")
      |> Plug.Conn.send_chunked(200)

    Enum.each(chunks, fn chunk ->
      {:ok, _conn} = Plug.Conn.chunk(conn, chunk)
    end)

    Process.exit(self(), :kill)
  end

  defp respond(_pid, _conn, :close_before_headers, _request) do
    Process.exit(self(), :kill)
  end

  defp respond(
         pid,
         conn,
         {:barrier_sse, chunks, barrier_after, notify, release_ref, settings},
         _request
       ) do
    conn =
      conn
      |> Plug.Conn.put_resp_header("cache-control", "no-cache")
      |> Plug.Conn.put_resp_content_type("text/event-stream")
      |> Plug.Conn.send_chunked(200)

    maybe_wait_for_sse_barrier(conn, 0, barrier_after, notify, release_ref)
    chunk_count = length(chunks)

    chunks
    |> Enum.with_index(1)
    |> Enum.reduce_while(conn, fn {chunk, index}, conn ->
      # A write result is never assumed: the chunk-delivered acknowledgement
      # follows only a successful write, so a failed tail write can never be
      # reported as delivered.
      case Plug.Conn.chunk(conn, chunk) do
        {:ok, conn} ->
          notify_chunk_sent(notify, index)
          maybe_wait_for_sse_barrier(conn, index, barrier_after, notify, release_ref)
          {:cont, conn}

        {:error, reason} ->
          {:halt,
           barrier_sse_write_failed(pid, conn, settings, notify, release_ref, %{
             chunk_index: index,
             chunk_count: chunk_count,
             reason: reason
           })}
      end
    end)
  end

  defp respond(_pid, conn, {:malformed_json, status, body}, _request) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, body)
  end

  defp respond(_pid, conn, {:json_error, status, payload}, _request) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, CodexPooler.JSON.encode!(payload))
  end

  defp respond(_pid, conn, {:non_json_error, status, body}, _request) do
    conn
    |> Plug.Conn.put_resp_content_type("text/plain")
    |> Plug.Conn.send_resp(status, body)
  end

  defp respond(_pid, conn, {:timeout_before_headers, notify, release_ref}, _request) do
    wait_for_timeout_release(:before_headers, notify, release_ref)

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(200, CodexPooler.JSON.encode!(%{"late" => true}))
  end

  defp respond(_pid, conn, {:timeout_after_sse_headers, notify, release_ref}, _request) do
    conn =
      conn
      |> Plug.Conn.put_resp_header("cache-control", "no-cache")
      |> Plug.Conn.put_resp_content_type("text/event-stream")
      |> Plug.Conn.send_chunked(200)

    wait_for_timeout_release(:after_sse_headers, notify, release_ref)
    conn
  end

  defp respond(_pid, conn, {:timeout_mid_stream, first_chunk, notify, release_ref}, _request) do
    conn =
      conn
      |> Plug.Conn.put_resp_header("cache-control", "no-cache")
      |> Plug.Conn.put_resp_content_type("text/event-stream")
      |> Plug.Conn.send_chunked(200)

    {:ok, conn} = Plug.Conn.chunk(conn, first_chunk)
    wait_for_timeout_release(:mid_stream, notify, release_ref)
    {:ok, conn} = Plug.Conn.chunk(conn, "data: late\n\n")

    conn
  end

  defp sse_chunk(chunk) when is_binary(chunk), do: chunk

  defp sse_chunk({event, payload}) when is_binary(event) do
    "event: #{event}\ndata: #{CodexPooler.JSON.encode!(payload)}\n\n"
  end

  defp sse_chunk(payload) when is_map(payload) do
    "data: #{CodexPooler.JSON.encode!(payload)}\n\n"
  end

  defp start_sse_response(conn) do
    conn
    |> Plug.Conn.put_resp_header("cache-control", "no-cache")
    |> Plug.Conn.put_resp_content_type("text/event-stream")
    |> Plug.Conn.send_chunked(200)
  end

  defp decode_json(""), do: nil

  defp decode_json(body) do
    case CodexPooler.JSON.decode(body) do
      {:ok, payload} -> payload
      {:error, _} -> nil
    end
  end

  defp file_protocol_config(opts) do
    opts = Map.new(opts)
    file_id = Map.get(opts, :file_id, "file_fake_upload")

    %{
      mode: :success,
      file_id: file_id,
      file_name: Map.get(opts, :file_name, "fixture-upload.txt"),
      mime_type: Map.get(opts, :mime_type, "text/plain"),
      upload_url:
        Map.get(
          opts,
          :upload_url,
          "https://fake-upload.invalid/upload/#{file_id}?sig=fake-upload"
        ),
      download_url:
        Map.get(
          opts,
          :download_url,
          "https://fake-download.invalid/download/#{file_id}?sig=fake-download"
        ),
      upload_status: Map.get(opts, :upload_status, 201),
      upload_body: Map.get(opts, :upload_body, ""),
      unauthorized_payload: Map.get(opts, :unauthorized_payload, %{"error" => %{"code" => "invalid_api_key"}}),
      error_body: Map.get(opts, :error_body, "fake upstream file finalize failure"),
      create_error_status: Map.get(opts, :create_error_status, 429),
      create_error_payload: Map.get(opts, :create_error_payload, %{"error" => %{"code" => "rate_limit_exceeded"}})
    }
  end

  defp file_protocol_create_response(conn, %{mode: :unauthorized, unauthorized_payload: payload}) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(401, CodexPooler.JSON.encode!(payload))
  end

  defp file_protocol_create_response(conn, %{
         mode: :create_error,
         create_error_status: status,
         create_error_payload: payload
       }) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, CodexPooler.JSON.encode!(payload))
  end

  defp file_protocol_create_response(conn, %{mode: :non_json_error, error_body: body}) do
    conn
    |> Plug.Conn.put_resp_content_type("text/plain")
    |> Plug.Conn.send_resp(502, body)
  end

  defp file_protocol_create_response(conn, config) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(
      200,
      CodexPooler.JSON.encode!(%{"file_id" => config.file_id, "upload_url" => config.upload_url})
    )
  end

  defp file_protocol_finalize_response(
         _pid,
         conn,
         %{mode: :unauthorized, unauthorized_payload: payload},
         _request
       ) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(401, CodexPooler.JSON.encode!(payload))
  end

  defp file_protocol_finalize_response(
         _pid,
         conn,
         %{mode: :non_json_error, error_body: body},
         _request
       ) do
    conn
    |> Plug.Conn.put_resp_content_type("text/plain")
    |> Plug.Conn.send_resp(502, body)
  end

  defp file_protocol_finalize_response(pid, conn, %{mode: :finalize_retry} = config, request) do
    finalize_call = bump_route_count(pid, {request.method, request.path})

    if finalize_call == 1 do
      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(200, CodexPooler.JSON.encode!(%{"status" => "retry"}))
    else
      file_protocol_finalize_success(conn, config)
    end
  end

  defp file_protocol_finalize_response(_pid, conn, config, _request) do
    file_protocol_finalize_success(conn, config)
  end

  defp file_protocol_finalize_success(conn, config) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(
      200,
      CodexPooler.JSON.encode!(%{
        "status" => "success",
        "download_url" => config.download_url,
        "file_name" => config.file_name,
        "mime_type" => config.mime_type
      })
    )
  end

  defp bump_route_count(pid, key) do
    Agent.get_and_update(pid, fn state ->
      route_counts = Map.get(state, :route_counts, %{})
      count = Map.get(route_counts, key, 0) + 1
      {count, %{state | route_counts: Map.put(route_counts, key, count)}}
    end)
  end

  defp notify_chunk_sent(nil, _index), do: :ok
  defp notify_chunk_sent(pid, index), do: send(pid, {:fake_upstream_chunk_sent, index})

  # What a zero-timeout recv reports once the peer closed.
  @client_gone_recv_reasons [:closed, :econnreset]
  # What a write to a peer that went away can report, Bandit's own closure set
  # plus the broken pipe a half-closed socket yields.
  @client_gone_write_reasons [:closed, :enotconn, :einval, :econnaborted, :econnreset, :epipe]
  # The client going away (`:closed`, or the reset the kernel reports for the
  # same event) is the one write failure a cancellation scenario may expect:
  # it ends the tail with one bounded outcome and a notification, never an
  # acknowledgement. In strict mode, and for every other write error, the
  # handler fails with the owner correlator in its message.

  defp barrier_sse_write_failed(
         pid,
         conn,
         %{on_client_close: :expected, owner: owner},
         notify,
         release_ref,
         %{reason: reason} = failure
       )
       when reason in @client_gone_write_reasons do
    outcome = %{
      scenario: :barrier_sse,
      owner: owner,
      outcome: :client_closed_expected,
      chunk_index: failure.chunk_index,
      chunk_count: failure.chunk_count,
      reason: reason
    }

    Agent.update(pid, fn state -> %{state | sse_outcomes: [outcome | state.sse_outcomes]} end)

    if is_pid(notify) do
      send(notify, {:fake_upstream_client_closed, failure.chunk_index, self(), release_ref})
    end

    conn
  end

  defp barrier_sse_write_failed(_pid, _conn, settings, _notify, _release_ref, failure) do
    raise SseWriteError,
      owner: settings.owner,
      chunk_index: failure.chunk_index,
      chunk_count: failure.chunk_count,
      reason: safe_write_reason(failure.reason),
      mode: settings.on_client_close
  end

  defp safe_write_reason(reason) when is_atom(reason), do: reason

  # Bandit returns `Exception.message/1` text for non-transport errors; keep
  # it when it is a bounded identifier, otherwise fingerprint it.
  defp safe_write_reason(reason) when is_binary(reason) do
    if byte_size(reason) <= 80 and Regex.match?(~r/^[A-Za-z0-9_.:-]+$/, reason),
      do: reason,
      else: "sha256:" <> short_hash(reason)
  end

  defp safe_write_reason(reason), do: "sha256:" <> short_hash(reason)

  defp short_hash(term),
    do:
      :crypto.hash(:sha256, :erlang.term_to_binary(term))
      |> Base.encode16(case: :lower)
      |> String.slice(0, 12)

  defp wait_for_delay(interval_ms) do
    receive do
    after
      interval_ms -> :ok
    end
  end

  @barrier_release_timeout_ms 30_000
  @barrier_poll_ms 20

  defp maybe_wait_for_sse_barrier(conn, index, index, notify, release_ref) when is_pid(notify) do
    send(notify, {:fake_upstream_chunk_barrier, index, self(), release_ref})
    deadline = System.monotonic_time(:millisecond) + @barrier_release_timeout_ms
    await_sse_barrier_release(conn, index, notify, release_ref, deadline, false)
  end

  defp maybe_wait_for_sse_barrier(_conn, _index, _barrier_after, _notify, _release_ref), do: :ok

  # While parked, the handler watches its own socket the way Bandit does
  # (`recv(socket, 0, 0)`): a peer that closed is reported to the owning test
  # exactly once as `{:fake_upstream_client_gone, index, handler, release_ref}`,
  # so a scenario that closes its client can wait for the server-side
  # observation before releasing the tail instead of racing the FIN.
  defp await_sse_barrier_release(conn, index, notify, release_ref, deadline, gone_reported?) do
    receive do
      {:fake_upstream_release_chunk, ^release_ref} -> :ok
    after
      @barrier_poll_ms ->
        if System.monotonic_time(:millisecond) > deadline do
          raise "timed out waiting for fake upstream SSE barrier release"
        end

        gone_reported? =
          if not gone_reported? and client_gone?(conn) do
            send(notify, {:fake_upstream_client_gone, index, self(), release_ref})
            true
          else
            gone_reported?
          end

        await_sse_barrier_release(conn, index, notify, release_ref, deadline, gone_reported?)
    end
  end

  # Bandit keeps the socket passive while the plug runs, so a zero-timeout
  # recv distinguishes "still open, no data" from a peer close. Observing the
  # close also closes the port, so the next write fails deterministically.
  # Any other shape (active socket, unknown adapter) answers false and leaves
  # detection to the write result.
  defp client_gone?(%Plug.Conn{
         adapter: {Bandit.Adapter, %{transport: %{socket: %ThousandIsland.Socket{} = socket}}}
       }) do
    case ThousandIsland.Socket.recv(socket, 0, 0) do
      {:error, reason} when reason in @client_gone_recv_reasons ->
        true

      {:error, _still_open_or_active} ->
        false

      {:ok, _bytes} ->
        # No barrier scenario has its client speak mid-response; bytes here
        # would otherwise be swallowed before Bandit's keep-alive read.
        raise "fake upstream barrier SSE received client bytes while parked"
    end
  end

  defp client_gone?(_conn), do: false

  defp wait_for_timeout_release(stage, notify, release_ref) do
    if is_pid(notify) do
      send(notify, {:fake_upstream_timeout_barrier, stage, self(), release_ref})
    end

    receive do
      {:fake_upstream_release_timeout, ^release_ref} -> :ok
    after
      30_000 -> raise "timed out waiting for fake upstream timeout release"
    end
  end

  defp wait_for_gate_release(stage, notify, release_ref) do
    send(notify, {:fake_upstream_gate, stage, self(), release_ref})

    receive do
      {:fake_upstream_release_gate, ^release_ref} -> :ok
    after
      30_000 -> raise "timed out waiting for fake upstream gate release"
    end
  end

  defp safe_stop(fun) do
    fun.()
  catch
    :exit, _ -> :ok
  end

  defmodule Supervisor do
    @moduledoc false

    use Elixir.Supervisor

    def start_link(opts \\ []) do
      Elixir.Supervisor.start_link(__MODULE__, :ok, opts)
    end

    @impl Elixir.Supervisor
    def init(:ok) do
      Elixir.Supervisor.init([], strategy: :one_for_one, auto_shutdown: :any_significant)
    end
  end

  defmodule Websocket do
    @moduledoc false

    @behaviour WebSock

    @impl WebSock
    def init(%{mode: {:websocket_init_barrier, mode, notify, release_ref}} = state) do
      await_websocket_barrier(:before_init, notify, release_ref)
      init_websocket(%{state | mode: mode}, notify, release_ref)
    end

    def init(state) do
      init_websocket(state, nil, nil)
    end

    defp init_websocket(%{pid: pid} = state, notify, release_ref) do
      websocket_pid = self()

      {connection_id, _opaque_connection_id} =
        Agent.get_and_update(pid, fn agent_state ->
          connection_count = Map.get(agent_state, :websocket_connection_count, 0) + 1
          opaque_connection_id = make_ref()

          agent_state = %{
            agent_state
            | websocket_connection_count: connection_count,
              websocket_connection_ids: [
                opaque_connection_id | Map.get(agent_state, :websocket_connection_ids, [])
              ],
              websocket_pids: MapSet.put(Map.get(agent_state, :websocket_pids, MapSet.new()), websocket_pid),
              websocket_pids_by_connection:
                Map.put(
                  Map.get(agent_state, :websocket_pids_by_connection, %{}),
                  connection_count,
                  websocket_pid
                )
          }

          {{connection_count, opaque_connection_id}, agent_state}
        end)

      if is_pid(notify) do
        send(notify, {:fake_upstream_websocket_initialized, websocket_pid, release_ref})
      end

      {:ok, Map.put(state, :connection_id, connection_id)}
    end

    @impl WebSock
    def handle_info(:fake_upstream_close_websocket, state),
      do: {:stop, :normal, {1001, "fake upstream closed websocket"}, state}

    def handle_info({:fake_upstream_close_websocket, code, reason}, state),
      do: {:stop, :normal, {code, reason}, state}

    # The pushed frames are already written to the socket; killing the
    # connection process closes it without a close frame. `terminate/2` does
    # not run for a killed process, so drop its registration first.
    def handle_info(:fake_upstream_abrupt_close_websocket, %{pid: pid} = state) do
      websocket_pid = self()

      Agent.update(pid, fn agent_state ->
        %{
          agent_state
          | websocket_pids: MapSet.delete(Map.get(agent_state, :websocket_pids, MapSet.new()), websocket_pid),
            websocket_pids_by_connection:
              Map.delete(
                Map.get(agent_state, :websocket_pids_by_connection, %{}),
                state.connection_id
              )
        }
      end)

      Process.exit(websocket_pid, :kill)
      {:ok, state}
    end

    def handle_info(
          {:fake_upstream_close_websocket, code, reason, notify, close_ref},
          state
        ) do
      {:stop, :normal, {code, reason}, Map.put(state, :peer_close_ack, {notify, close_ref})}
    end

    def handle_info(
          {:fake_upstream_delayed_websocket_message, message, remaining, interval_ms},
          state
        ) do
      schedule_delayed_websocket_message(remaining, interval_ms)
      {:push, {:text, message}, state}
    end

    def handle_info(
          {:fake_upstream_release_timeout, release_ref},
          %{delayed_terminal: %{release_ref: release_ref, messages: messages}} = state
        ) do
      {:push, Enum.map(messages, &{:text, &1}), Map.delete(state, :delayed_terminal)}
    end

    def handle_info(
          {:fake_upstream_terminal_close_barrier, code, reason, notify, release_ref},
          state
        ) do
      await_websocket_barrier(:before_close, notify, release_ref)
      {:stop, :normal, {code, reason}, state}
    end

    def handle_info(
          {:fake_upstream_frame_barrier_continue, release_ref},
          %{frame_barrier: %{release_ref: release_ref} = barrier} = state
        ) do
      continue_frame_barriers(barrier, state)
    end

    def handle_info(
          {:fake_upstream_release_interruptible, release_ref},
          %{interruptible: %{release_ref: release_ref, completion: completion}} = state
        ) do
      {:push, Enum.map(completion, &{:text, &1}), Map.delete(state, :interruptible)}
    end

    def handle_info({:fake_upstream_steerable_opened, notify, ref}, state) do
      send(notify, {:fake_upstream_steerable_open, self(), ref})
      {:ok, state}
    end

    def handle_info({:fake_upstream_release_steerable, ref}, %{steerable: %{ref: ref} = settings} = state) do
      send(self(), {:fake_upstream_steerable_released, settings.notify, ref})

      if settings.released? do
        {:ok, state}
      else
        {:push, Enum.map(settings.terminal_frames, &{:text, &1}), Map.put(state, :steerable, %{settings | released?: true})}
      end
    end

    def handle_info({:fake_upstream_steerable_released, notify, ref}, state) do
      finish_steerable(state, ref, :released)
      send(notify, {:fake_upstream_steerable_released, self(), ref})
      {:ok, state}
    end

    def handle_info({:fake_upstream_steerable_accepted, notify, ref}, state) do
      finish_steerable(state, ref, :accepted)
      send(notify, {:fake_upstream_steered, self(), ref})
      {:ok, state}
    end

    def handle_info(_message, state), do: {:ok, state}

    @impl WebSock
    def terminate(_reason, %{pid: pid} = state) do
      websocket_pid = self()

      Agent.update(pid, fn agent_state ->
        %{
          agent_state
          | websocket_pids: MapSet.delete(Map.get(agent_state, :websocket_pids, MapSet.new()), websocket_pid),
            websocket_pids_by_connection:
              Map.delete(
                Map.get(agent_state, :websocket_pids_by_connection, %{}),
                state.connection_id
              ),
            steerable_handlers: Map.reject(agent_state.steerable_handlers, fn {_ref, held} -> held.handler == websocket_pid end)
        }
      end)

      case Map.get(state, :peer_close_ack) do
        {notify, close_ref} when is_pid(notify) ->
          Agent.update(pid, fn agent_state ->
            %{
              agent_state
              | acknowledged: MapSet.put(agent_state.acknowledged, {:peer_close, close_ref})
            }
          end)

          send(notify, {:fake_upstream_websocket_peer_closed, state.connection_id, close_ref})

        _other ->
          :ok
      end

      :ok
    catch
      :exit, _reason -> :ok
    end

    @impl WebSock
    def handle_control({_payload, opcode: opcode}, %{pid: pid} = state)
        when opcode in [:ping, :pong] do
      frame = %{opcode: opcode, websocket_connection_id: state.connection_id}

      notify =
        Agent.get_and_update(pid, fn agent_state ->
          notify = Map.get(agent_state, :websocket_control_notify)

          {notify,
           %{
             agent_state
             | websocket_control_frames: [
                 frame | Map.get(agent_state, :websocket_control_frames, [])
               ]
           }}
        end)

      if is_pid(notify) do
        send(notify, {:fake_upstream_websocket_control, opcode, state.connection_id})
      end

      {:ok, state}
    end

    @impl WebSock
    def handle_in({payload, [opcode: :text]}, state) do
      case decode_json(payload) do
        %{"type" => "response.interrupt"} = interrupt -> handle_interrupt(interrupt, state)
        %{"type" => "response.steer"} = steer -> handle_steer(payload, steer, state)
        _request -> handle_request_frame(payload, state)
      end
    end

    def handle_in({_payload, [opcode: :binary]}, state), do: {:stop, :unsupported_binary, state}

    @spec handle_steer(binary(), map(), map()) :: WebSock.handle_result()
    defp handle_steer(payload, steer, %{pid: pid} = state) do
      request = websocket_request(payload, steer, state)
      CodexPooler.FakeUpstream.record_websocket_steer(pid, request)

      if Map.get(state, :refused?, false) do
        send(self(), :fake_upstream_abrupt_close_websocket)
        {:ok, state}
      else
        case Map.get(state, :steerable) do
          %{} = settings -> handle_steer_outcome(steer, settings, state)
          nil -> push_steer_failure(steer, :response_not_found, state)
        end
      end
    end

    @spec handle_steer_outcome(map(), map(), map()) :: WebSock.handle_result()
    defp handle_steer_outcome(steer, settings, state) do
      cond do
        unavailable_steer_target?(steer, settings) ->
          push_steer_failure(steer, :response_not_found, state)

        settings.outcome == :native_lane_error ->
          finish_steerable(state, settings.ref, :closed)
          {:stop, :normal, {1000, ""}, {:text, native_lane_error_frame()}, state}

        settings.outcome == :invalid_input or not valid_steer_input?(steer) ->
          push_steer_failure(steer, :invalid_input, state)

        settings.outcome == :post_terminal_close ->
          finish_steerable(state, settings.ref, :closed)
          {:stop, :normal, {1000, ""}, state}

        true ->
          accept_steer(steer, settings, state)
      end
    end

    defp unavailable_steer_target?(steer, settings) do
      not is_binary(steer["previous_response_id"]) or steer["previous_response_id"] != settings.response_id or
        settings.outcome == :response_not_found or settings.accepted?
    end

    @spec valid_steer_input?(map()) :: boolean()
    defp valid_steer_input?(steer) do
      Enum.sort(Map.keys(steer)) == ~w(input previous_response_id type) and is_list(steer["input"])
    end

    @spec accept_steer(map(), map(), map()) :: WebSock.handle_result()
    defp accept_steer(steer, settings, state) do
      accepted =
        CodexPooler.JSON.encode!(%{
          "type" => "response.steer.accepted",
          "sequence_number" => 1,
          "steer" => %{"id" => settings.steer_id, "previous_response_id" => steer["previous_response_id"]}
        })

      terminal = if settings.released?, do: [], else: settings.terminal_frames
      frames = [accepted | terminal ++ settings.successor_frames]
      state = Map.put(state, :steerable, %{settings | released?: true, accepted?: true})

      case settings.batches do
        :coalesced ->
          send(self(), {:fake_upstream_steerable_accepted, settings.notify, settings.ref})
          {:push, Enum.map(frames, &{:text, &1}), state}

        :separate ->
          CodexPooler.FakeUpstream.register_frame_barriers(state.pid, settings.ref, length(frames))

          continue_frame_barriers(
            %{frames: frames, pushed: 0, notify: settings.notify, release_ref: settings.ref, steerable_ref: settings.ref},
            state
          )
      end
    end

    @spec push_steer_failure(map(), :response_not_found | :invalid_input, map()) :: WebSock.handle_result()
    defp push_steer_failure(steer, outcome, state) do
      {code, message} =
        case outcome do
          :response_not_found -> {"response_not_found", "The target response is not available on this connection."}
          :invalid_input -> {"invalid_input", "response.steer requires only type, previous_response_id, and input."}
        end

      failed = %{
        "type" => "response.steer.failed",
        "sequence_number" => 1,
        "steer" => Map.take(steer, ["previous_response_id", "input"]),
        "error" => %{"type" => "invalid_request_error", "code" => code, "message" => message}
      }

      {:push, {:text, CodexPooler.JSON.encode!(failed)}, state}
    end

    @spec native_lane_error_frame() :: String.t()
    defp native_lane_error_frame do
      CodexPooler.JSON.encode!(%{
        "type" => "error",
        "status" => 400,
        "error" => %{
          "type" => "invalid_request_error",
          "code" => "unsupported_native_inflight_message",
          "message" => "The experimental native turn lane cannot accept stateful WebSocket messages while a native turn is running. Start a new independent response.create turn instead."
        }
      })
    end

    @spec finish_steerable(map(), reference(), :accepted | :released | :closed) :: :ok
    defp finish_steerable(%{pid: pid}, ref, phase) do
      Agent.update(pid, fn agent_state ->
        handlers =
          case Map.get(agent_state.steerable_handlers, ref) do
            nil -> agent_state.steerable_handlers
            held -> Map.put(agent_state.steerable_handlers, ref, %{held | phase: phase})
          end

        acknowledged = MapSet.put(agent_state.acknowledged, {:steerable_finished, ref})
        acknowledged = if phase == :released, do: MapSet.put(acknowledged, {:steerable_release, ref}), else: acknowledged

        %{
          agent_state
          | steerable_handlers: handlers,
            acknowledged: acknowledged
        }
      end)
    end

    @spec websocket_request(binary(), term(), map()) :: map()
    defp websocket_request(payload, json, state) do
      %{
        method: "WEBSOCKET",
        path: "/backend-api/codex/responses",
        query_string: "",
        headers: Map.get(state, :headers, []),
        websocket_connection_id: state.connection_id,
        body: payload,
        json: json
      }
    end

    # The provider answers an interrupt only on the connection that carries the
    # response it names, and only while that response runs.
    defp handle_interrupt(interrupt, %{pid: pid} = state) do
      Agent.update(pid, fn agent_state ->
        Map.update(agent_state, :websocket_interrupts, [%{websocket_connection_id: state.connection_id, json: interrupt}], &[%{websocket_connection_id: state.connection_id, json: interrupt} | &1])
      end)

      requested = Map.get(interrupt, "response_id")

      case Map.get(state, :interruptible) do
        %{response_id: ^requested, interrupted: interrupted, notify: notify, release_ref: release_ref} ->
          send(notify, {:fake_upstream_interrupted, self(), release_ref})
          {:push, Enum.map(interrupted, &{:text, &1}), Map.delete(state, :interruptible)}

        _not_running ->
          failed = %{
            "type" => "response.interrupt.failed",
            "response_id" => requested,
            "error" => %{"code" => "response_not_in_progress", "param" => "response_id", "type" => "invalid_request_error"}
          }

          {:push, {:text, CodexPooler.JSON.encode!(failed)}, state}
      end
    end

    defp handle_request_frame(payload, %{pid: pid} = state) do
      request = websocket_request(payload, decode_json(payload), state)

      cond do
        # The provider answers nothing more on a connection it refused a request on and drops it without a Close
        # frame (findings#333): the request is captured, nothing is consumed.
        Map.get(state, :refused?, false) ->
          CodexPooler.FakeUpstream.record_unanswered_request(pid, request)
          send(self(), :fake_upstream_abrupt_close_websocket)
          {:ok, state}

        message = CodexPooler.FakeUpstream.provider_refusal_message(request.json) ->
          CodexPooler.FakeUpstream.record_unanswered_request(pid, request)
          {:push, {:text, CodexPooler.FakeUpstream.provider_refusal_frame(message)}, Map.put(state, :refused?, true)}

        error = CodexPooler.FakeUpstream.provider_validation_rejection(pid, request) ->
          CodexPooler.FakeUpstream.record_unanswered_request(pid, request)
          {:push, {:text, CodexPooler.FakeUpstream.provider_validation_error_frame(error)}, state}

        true ->
          mode = CodexPooler.FakeUpstream.take_response_mode(pid, request)

          handle_websocket_message(websocket_messages(mode, request), state)
      end
    end

    defp handle_websocket_message({:provider_refusal, frame}, state),
      do: {:push, {:text, frame}, Map.put(state, :refused?, true)}

    defp handle_websocket_message({:close, code, reason}, state),
      do: {:stop, reason, {code, reason}, state}

    defp handle_websocket_message({:push_then_abrupt_close, messages}, state) do
      send(self(), :fake_upstream_abrupt_close_websocket)
      {:push, Enum.map(messages, &{:text, &1}), state}
    end

    defp handle_websocket_message({:push_then_close, messages, code, reason}, state) do
      send(self(), {:fake_upstream_close_websocket, code, reason})
      {:push, Enum.map(messages, &{:text, &1}), state}
    end

    defp handle_websocket_message(
           {:barrier_push_then_close, terminal, code, reason, notify, release_ref},
           state
         ) do
      await_websocket_barrier(:before_terminal, notify, release_ref)

      send(
        self(),
        {:fake_upstream_terminal_close_barrier, code, reason, notify, release_ref}
      )

      {:push, {:text, terminal}, state}
    end

    defp handle_websocket_message({:barrier_push, terminal, notify, release_ref}, state) do
      await_websocket_barrier(:before_terminal, notify, release_ref)
      {:push, {:text, terminal}, state}
    end

    defp handle_websocket_message({:barrier_close, code, reason, notify, release_ref}, state) do
      await_websocket_barrier(:before_close, notify, release_ref)
      {:stop, :normal, {code, reason}, state}
    end

    defp handle_websocket_message({:frame_barriers, frames, notify, release_ref}, state) do
      CodexPooler.FakeUpstream.register_frame_barriers(state.pid, release_ref, length(frames))

      continue_frame_barriers(
        %{frames: frames, pushed: 0, notify: notify, release_ref: release_ref},
        state
      )
    end

    defp handle_websocket_message({:delayed_push, messages, interval_ms}, state) do
      schedule_delayed_websocket_message(messages, interval_ms)
      {:ok, state}
    end

    defp handle_websocket_message(
           {:delayed_terminal, messages, terminal, notify, release_ref},
           state
         ) do
      send(notify, {:fake_upstream_timeout_barrier, :before_terminal, self(), release_ref})

      next_state =
        Map.put(state, :delayed_terminal, %{release_ref: release_ref, messages: terminal})

      {:push, Enum.map(messages, &{:text, &1}), next_state}
    end

    defp handle_websocket_message({:interruptible, opening, spec}, %{pid: pid} = state) do
      handler = self()

      Agent.update(pid, fn agent_state ->
        Map.update(agent_state, :interruptible_handlers, %{spec.release_ref => handler}, &Map.put(&1, spec.release_ref, handler))
      end)

      send(spec.notify, {:fake_upstream_interruptible_open, self(), spec.release_ref})
      {:push, Enum.map(opening, &{:text, &1}), Map.put(state, :interruptible, spec)}
    end

    defp handle_websocket_message({:steerable, opening, settings}, %{pid: pid} = state) do
      handler = self()

      Agent.update(pid, fn agent_state ->
        %{
          agent_state
          | steerable_handlers: Map.put(agent_state.steerable_handlers, settings.ref, %{handler: handler, phase: :open}),
            required_acknowledgements: MapSet.put(agent_state.required_acknowledgements, {:steerable_finished, settings.ref})
        }
      end)

      send(self(), {:fake_upstream_steerable_opened, settings.notify, settings.ref})
      settings = Map.merge(settings, %{released?: false, accepted?: false})
      {:push, Enum.map(opening, &{:text, &1}), Map.put(state, :steerable, settings)}
    end

    defp handle_websocket_message(messages, state),
      do: {:push, Enum.map(messages, &{:text, &1}), state}

    defp websocket_messages({:json, _status, payload}, _request),
      do: [CodexPooler.JSON.encode!(payload)]

    defp websocket_messages({:scenario_failure, _diagnostic}, _request),
      do: {:close, 1011, "fake upstream scenario failure"}

    defp websocket_messages({:json_headers, _status, payload, _headers}, _request),
      do: [CodexPooler.JSON.encode!(payload)]

    defp websocket_messages(
           {:reject_json_field, field, _success_status, success_payload, error_status, error_payload},
           request
         ) do
      if is_map(request.json) and Map.has_key?(request.json, field) do
        [
          CodexPooler.JSON.encode!(%{
            "type" => "error",
            "status" => error_status,
            "error" => error_payload["error"] || error_payload
          })
        ]
      else
        [CodexPooler.JSON.encode!(success_payload)]
      end
    end

    defp websocket_messages(
           {:require_json_field, field, _success_status, success_payload, error_status, error_payload},
           request
         ) do
      if is_map(request.json) and Map.has_key?(request.json, field) do
        [CodexPooler.JSON.encode!(success_payload)]
      else
        [
          CodexPooler.JSON.encode!(%{
            "type" => "error",
            "status" => error_status,
            "error" => error_payload["error"] || error_payload
          })
        ]
      end
    end

    defp websocket_messages({:sse, chunks}, _request),
      do: messages_from_sse_chunk(Enum.join(chunks))

    defp websocket_messages({:sse_headers, chunks, _headers}, _request),
      do: messages_from_sse_chunk(Enum.join(chunks))

    defp websocket_messages({:delayed_sse, chunks, interval_ms, _notify}, _request),
      do: {:delayed_push, Enum.flat_map(chunks, &messages_from_sse_chunk/1), interval_ms}

    defp websocket_messages(
           {:delayed_terminal_sse, before_terminal, terminal, notify, release_ref},
           _request
         ) do
      {:delayed_terminal, Enum.flat_map(before_terminal, &messages_from_sse_chunk/1), Enum.flat_map(terminal, &messages_from_sse_chunk/1), notify, release_ref}
    end

    defp websocket_messages({:timeout_mid_stream, first_chunk, notify, release_ref}, _request) do
      if is_pid(notify) do
        send(notify, {:fake_upstream_timeout_barrier, :mid_stream, self(), release_ref})
      end

      messages_from_sse_chunk(first_chunk)
    end

    defp websocket_messages({:websocket_idle_timeout, notify, release_ref}, _request) do
      send(notify, {:fake_upstream_timeout_barrier, :websocket_idle, self(), release_ref})
      []
    end

    defp websocket_messages({:websocket_text, messages}, _request), do: messages

    defp websocket_messages({:provider_refusal, message}, _request),
      do: {:provider_refusal, CodexPooler.FakeUpstream.provider_refusal_frame(message)}

    defp websocket_messages({:websocket_text_then_abrupt_close, messages}, _request),
      do: {:push_then_abrupt_close, messages}

    defp websocket_messages({:websocket_frame_barrier, frames, notify, release_ref}, _request),
      do: {:frame_barriers, frames, notify, release_ref}

    defp websocket_messages({:websocket_interruptible, opening, spec}, _request),
      do: {:interruptible, opening, spec}

    defp websocket_messages({:websocket_steerable, opening, settings}, _request),
      do: {:steerable, opening, settings}

    defp websocket_messages({:websocket_sse_then_close, chunks, code, reason}, _request) do
      {:push_then_close, messages_from_sse_chunk(Enum.join(chunks)), code, reason}
    end

    defp websocket_messages(
           {:websocket_terminal_then_close_barrier, terminal, code, reason, notify, release_ref},
           _request
         ) do
      {:barrier_push_then_close, terminal, code, reason, notify, release_ref}
    end

    defp websocket_messages(
           {:websocket_connection_limit_terminal_barrier, shape, notify, release_ref},
           _request
         ) do
      {:barrier_push, websocket_connection_limit_terminal(shape), notify, release_ref}
    end

    defp websocket_messages(
           {:websocket_close_without_terminal_barrier, code, reason, notify, release_ref},
           _request
         ) do
      {:barrier_close, code, reason, notify, release_ref}
    end

    defp websocket_messages(
           {:barrier_sse, chunks, barrier_after, notify, release_ref, _settings},
           _request
         ) do
      maybe_wait_for_sse_barrier(0, barrier_after, notify, release_ref)

      chunks
      |> Enum.with_index(1)
      |> Enum.flat_map(fn {chunk, index} ->
        send(notify, {:fake_upstream_chunk_sent, index})
        maybe_wait_for_sse_barrier(index, barrier_after, notify, release_ref)

        messages_from_sse_chunk(chunk)
      end)
    end

    defp websocket_messages({:json_error, status, payload}, _request),
      do: [
        CodexPooler.JSON.encode!(%{
          "type" => "error",
          "status" => status,
          "error" => payload["error"] || payload
        })
      ]

    defp websocket_messages({:non_json_error, status, body}, _request),
      do: [
        CodexPooler.JSON.encode!(%{
          "type" => "error",
          "status" => status,
          "error" => %{"message" => body}
        })
      ]

    # Path-routed modes answer a native websocket turn like the matching HTTP
    # route: a JSON route pushes its payload as one text frame (an error status
    # as one type:error frame), and a nested mode keeps its websocket shape.
    defp websocket_messages({:path_json, routes}, request) do
      case Map.get(routes, request.path) do
        {status, payload} when is_integer(status) and status in 200..299 ->
          [CodexPooler.JSON.encode!(payload)]

        {status, payload} when is_integer(status) ->
          websocket_messages({:json_error, status, payload}, request)

        nil ->
          {:close, 1011, "unsupported fake websocket mode"}

        mode ->
          websocket_messages(mode, request)
      end
    end

    defp websocket_messages(_mode, _request),
      do: {:close, 1011, "unsupported fake websocket mode"}

    defp websocket_connection_limit_terminal(:top_level) do
      CodexPooler.JSON.encode!(%{
        "type" => "response.failed",
        "error" => %{
          "code" => "websocket_connection_limit_reached",
          "message" => "synthetic websocket connection limit"
        },
        "response" => %{"status" => "failed"}
      })
    end

    defp websocket_connection_limit_terminal(:nested) do
      CodexPooler.JSON.encode!(%{
        "type" => "response.failed",
        "response" => %{
          "status" => "failed",
          "error" => %{
            "code" => "websocket_connection_limit_reached",
            "message" => "synthetic websocket connection limit"
          }
        }
      })
    end

    defp messages_from_sse_chunk(chunk) do
      chunk
      |> String.split("\n")
      |> Enum.filter(&String.starts_with?(&1, "data: "))
      |> Enum.map(&String.replace_prefix(&1, "data: ", ""))
      |> Enum.reject(&(&1 in ["", "[DONE]"]))
    end

    defp schedule_delayed_websocket_message([], _interval_ms), do: :ok

    defp schedule_delayed_websocket_message([message | remaining], interval_ms) do
      Process.send_after(
        self(),
        {:fake_upstream_delayed_websocket_message, message, remaining, interval_ms},
        interval_ms
      )

      :ok
    end

    defp maybe_wait_for_sse_barrier(index, index, notify, release_ref) when is_pid(notify) do
      send(notify, {:fake_upstream_chunk_barrier, index, self(), release_ref})

      receive do
        {:fake_upstream_release_chunk, ^release_ref} -> :ok
        {:EXIT, _from, reason} -> exit(reason)
      after
        30_000 -> raise "timed out waiting for fake upstream websocket SSE barrier release"
      end
    end

    defp maybe_wait_for_sse_barrier(_index, _barrier_after, _notify, _release_ref), do: :ok

    # Holds at barrier `pushed`, then pushes the next frame (if any) and
    # re-enters through `handle_info` so the push is on the wire before the
    # following barrier is announced.
    defp continue_frame_barriers(%{frames: frames, pushed: pushed} = barrier, state) do
      case barrier do
        %{pushed: 1, steerable_ref: ref, notify: notify} ->
          finish_steerable(state, ref, :accepted)
          send(notify, {:fake_upstream_steered, self(), ref})

        _other ->
          :ok
      end

      await_frame_barrier(state.pid, barrier)

      case frames do
        [] ->
          {:ok, Map.delete(state, :frame_barrier)}

        [frame | rest] ->
          send(self(), {:fake_upstream_frame_barrier_continue, barrier.release_ref})

          {:push, {:text, frame}, Map.put(state, :frame_barrier, %{barrier | frames: rest, pushed: pushed + 1})}
      end
    end

    defp await_frame_barrier(pid, %{pushed: ordinal, notify: notify, release_ref: release_ref}) do
      auto_released? =
        CodexPooler.FakeUpstream.reach_frame_barrier(pid, release_ref, ordinal, self())

      send(notify, {:fake_upstream_frame_barrier, ordinal, self(), release_ref})

      unless auto_released? do
        receive do
          {:fake_upstream_release_frame, ^release_ref} ->
            :ok

          # The handler traps exits: leave promptly when the fake is stopped
          # while the barrier is held instead of waiting out the supervisor's
          # shutdown timeout.
          {:EXIT, _from, reason} ->
            exit(reason)
        after
          30_000 -> raise "timed out waiting for fake upstream websocket frame barrier release"
        end
      end
    end

    # The handler traps exits, so a plain receive would leave a barrier held at
    # stop until ThousandIsland's 15 s `shutdown_timeout` brutal-kills it; leave
    # on the shutdown exit instead. `:before_init` is the exception: lifecycle
    # tests release it after the fake started stopping to prove the state
    # Agent outlives a connection that is still initializing.
    defp await_websocket_barrier(stage, notify, release_ref) do
      send(notify, {:fake_upstream_websocket_barrier, stage, self(), release_ref})

      receive do
        {:fake_upstream_release_websocket, ^release_ref} -> :ok
        {:EXIT, _from, reason} when stage != :before_init -> exit(reason)
      after
        30_000 -> raise "timed out waiting for fake upstream websocket barrier release"
      end
    end

    defp decode_json(""), do: nil

    defp decode_json(body) do
      case CodexPooler.JSON.decode(body) do
        {:ok, payload} -> payload
        {:error, _} -> nil
      end
    end
  end
end
