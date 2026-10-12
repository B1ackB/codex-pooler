defmodule Mix.Tasks.Dev.CodexRetainedUsageFixture do
  @moduledoc "Boot or provision only an explicitly owned retained-usage runtime."
  use Mix.Task
  alias CodexPooler.Dev.{CodexRetainedUsageFixture, CodexRetainedUsageFixtureData}
  @requirements ["app.config"]
  @shortdoc "Run owned provider-free retained-usage fixture"

  @impl Mix.Task
  def run([action]) when action in ["provision", "serve"] do
    unless Mix.env() == :dev, do: Mix.raise("retained fixture requires MIX_ENV=dev")
    CodexRetainedUsageFixture.boot!()
    if action == "provision", do: CodexRetainedUsageFixtureData.provision!()
  end

  def run(_), do: Mix.raise("use provision or serve with owned runtime environment")
end
