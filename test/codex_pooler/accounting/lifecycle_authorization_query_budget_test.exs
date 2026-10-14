defmodule CodexPooler.Accounting.LifecycleAuthorizationQueryBudgetTest do
  use CodexPooler.DataCase, async: false

  import Ecto.Query

  import CodexPooler.RequestReplayFixtures, only: [replay_fixture: 1]

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.Request
  alias CodexPooler.Repo

  @tracked_purposes ~w(
    request_snapshot
    turn_session_lookup
    session_lock
    api_key_lock
    turn_lock
    request_lock
    attempt_lock
    latest_attempt_lock
    entitlement_lock
    ledger_lock
  )a

  @no_turn_purposes ~w(
    request_snapshot
    turn_session_lookup
    request_lock
    attempt_lock
    latest_attempt_lock
    entitlement_lock
    ledger_lock
  )a

  test "tracked turn stays within the ten-purpose lifecycle authorization budget" do
    fixture = replay_fixture(reservation?: true)

    {result, statements} = capture_repo_queries(fn -> authorize(fixture) end)

    assert {:ok, :callback_ran} = result
    assert {:ok, summary} = validate_budget(statements, @tracked_purposes)
    assert summary.select_count == 10
    assert summary.purposes == @tracked_purposes
  end

  test "request without a turn stays within the seven-purpose lifecycle authorization budget" do
    fixture = replay_fixture(reservation?: true)
    Repo.delete!(fixture.turn)

    {result, statements} = capture_repo_queries(fn -> authorize(fixture) end)

    assert {:ok, :callback_ran} = result
    assert {:ok, summary} = validate_budget(statements, @no_turn_purposes)
    assert summary.select_count == 7
    assert summary.purposes == @no_turn_purposes
  end

  test "setup query contamination is rejected" do
    fixture = replay_fixture(reservation?: true)

    {_result, statements} =
      capture_repo_queries(fn ->
        Repo.one!(from request in Request, limit: 1)
        authorize(fixture)
      end)

    assert {:error, {:duplicate_purpose, :request_snapshot, 2}} =
             validate_budget(statements, @tracked_purposes)
  end

  defp authorize(fixture) do
    Accounting.with_current_replay_generation(fixture.request, fixture.attempt, fn ->
      :callback_ran
    end)
  end

  defp capture_repo_queries(fun) when is_function(fun, 0) do
    parent = self()
    handler_id = {__MODULE__, System.unique_integer([:positive, :monotonic])}

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        [:codex_pooler, :repo, :query],
        fn _event, _measurements, metadata, _config ->
          if metadata[:repo] == Repo and self() == parent do
            send(parent, {handler_id, normalize_statement(metadata)})
          end
        end,
        nil
      )

    try do
      result = fun.()
      {result, drain_statements(handler_id, [])}
    after
      :telemetry.detach(handler_id)
    end
  end

  defp drain_statements(handler_id, statements) do
    receive do
      {^handler_id, statement} -> drain_statements(handler_id, [statement | statements])
    after
      0 -> Enum.reverse(statements)
    end
  end

  defp normalize_statement(%{query: query} = metadata) when is_binary(query) do
    command = query |> String.trim_leading() |> String.split(~r/\s+/, parts: 2) |> hd() |> String.upcase()
    upper_query = String.upcase(query)
    relations = relation_set(query, metadata[:source])
    lock = lock_shape(upper_query)

    {command, relations, lock, purpose(command, relations, lock, upper_query)}
  end

  defp normalize_statement(_metadata), do: {nil, [], nil, nil}

  defp relation_set(query, source) do
    relations =
      Regex.scan(~r/\b(?:FROM|JOIN|UPDATE|INTO|DELETE\s+FROM)\s+"?([a-zA-Z0-9_.-]+)"?/i, query, capture: :all_but_first)
      |> Enum.map(&List.first/1)

    (relations ++ [source])
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp lock_shape(query) do
    cond do
      String.contains?(query, "FOR UPDATE") -> :for_update
      String.contains?(query, "FOR SHARE") -> :for_share
      true -> :none
    end
  end

  defp purpose("SELECT", ["requests"], :none, _query), do: :request_snapshot
  defp purpose("SELECT", ["codex_turns"], :none, _query), do: :turn_session_lookup
  defp purpose("SELECT", ["codex_sessions"], :for_update, _query), do: :session_lock
  defp purpose("SELECT", ["api_keys"], :for_share, _query), do: :api_key_lock
  defp purpose("SELECT", ["codex_turns"], :for_update, _query), do: :turn_lock
  defp purpose("SELECT", ["requests"], :for_update, _query), do: :request_lock

  defp purpose("SELECT", ["attempts"], :for_update, query) do
    if String.contains?(query, "ORDER BY") do
      :latest_attempt_lock
    else
      :attempt_lock
    end
  end

  defp purpose("SELECT", ["request_replay_entitlements"], :for_update, _query), do: :entitlement_lock
  defp purpose("SELECT", ["ledger_entries"], :for_update, _query), do: :ledger_lock
  defp purpose(_command, _relations, _lock_shape, _query), do: nil

  defp validate_budget(statements, expected_purposes) do
    select_statements = Enum.filter(statements, fn {command, _relations, _lock, _purpose} -> command == "SELECT" end)
    purposes = Enum.map(select_statements, fn {_command, _relations, _lock, purpose} -> purpose end)

    cond do
      Enum.any?(select_statements, fn {_command, _relations, _lock, purpose} -> is_nil(purpose) end) ->
        {:error, :unclassified_statement}

      Enum.any?(expected_purposes, &(Enum.count(purposes, fn purpose -> purpose == &1 end) > 1)) ->
        duplicate =
          Enum.find(expected_purposes, &(Enum.count(purposes, fn purpose -> purpose == &1 end) > 1))

        {:error, {:duplicate_purpose, duplicate, Enum.count(purposes, fn purpose -> purpose == duplicate end)}}

      Enum.sort(purposes) != Enum.sort(expected_purposes) ->
        {:error, {:purpose_set_mismatch, purposes}}

      true ->
        {:ok, %{select_count: length(select_statements), purposes: purposes}}
    end
  end
end
