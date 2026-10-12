defmodule CodexPoolerWeb.Admin.PolicyEditorComponentsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias CodexPoolerWeb.Admin.PolicyEditorComponents

  test "ordinary policy editors retain click navigation" do
    html = render_editor()
    assert has_selector?(html, "#example-editor-tab-details[type='button'][phx-click='change_step'][phx-value-step='details']")
    refute has_selector?(html, "button[name='wizard_step']")
    refute has_selector?(html, "button[hidden]")
  end

  test "form-associated steps serialize fields without validation and preserve the implicit save action" do
    html = render_editor(step_form_id: "example-form")
    assert has_selector?(html, "#example-editor-tab-details[type='submit'][form='example-form'][name='wizard_step'][value='details'][formnovalidate]")
    refute has_selector?(html, "#example-editor-tab-details[phx-click]")
    [first | _] = LazyHTML.from_fragment(html) |> LazyHTML.query("button[type='submit']") |> Enum.to_list()
    assert LazyHTML.attribute(first, "hidden") == [""]
    assert LazyHTML.attribute(first, "form") == ["example-form"]
    assert LazyHTML.attribute(first, "name") == []
  end

  defp render_editor(attrs \\ []) do
    render_component(
      &PolicyEditorComponents.policy_editor_dialog/1,
      Keyword.merge(
        [id: "example-editor", title: "Example editor", steps: [%{id: "details", label: "Details"}], current_step: "details", step_event: "change_step", inner_block: [%{inner_block: fn _, _ -> "" end}], actions: [%{inner_block: fn _, _ -> "" end}]],
        attrs
      )
    )
  end

  defp has_selector?(html, selector), do: html |> LazyHTML.from_fragment() |> LazyHTML.query(selector) |> Enum.any?()
end
