defmodule CodexPoolerWeb.Plugs.RejectDuplicateHost do
  @moduledoc false
  @behaviour Plug

  import Plug.Conn, only: [halt: 1, put_resp_content_type: 2, send_resp: 3]

  @impl true
  @spec init(keyword()) :: keyword()
  def init(opts), do: opts

  @impl true
  @spec call(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def call(conn, _opts) do
    if Enum.count(conn.req_headers, fn {name, _value} -> String.downcase(name, :ascii) == "host" end) > 1 do
      conn
      |> put_resp_content_type("text/plain")
      |> send_resp(400, "Bad Request")
      |> halt()
    else
      conn
    end
  end
end
