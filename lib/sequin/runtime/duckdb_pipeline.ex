defmodule Sequin.Runtime.DuckdbPipeline do
  @moduledoc false
  @behaviour Sequin.Runtime.SinkPipeline

  alias Broadway.Message
  alias Sequin.Consumers.DuckdbSink
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Duckdb.Client

  @impl SinkPipeline
  def deliver_messages(messages, %DuckdbSink{} = sink, opts \\ []) do
    records = Enum.map(messages, fn message -> message.data.record end)
    
    case Client.write_batch(sink, records, opts) do
      {:ok, _count} -> {:ok, messages, []}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl SinkPipeline
  def prepare_messages(messages, _sink) do
    messages
  end

  @impl SinkPipeline
  def validate_sink_config(%DuckdbSink{} = sink) do
    case File.dir?(Path.dirname(sink.database_path)) do
      true -> :ok
      false -> {:error, "Database directory does not exist: #{Path.dirname(sink.database_path)}"}
    end
  end

  @impl SinkPipeline
  def handle_failed_message(%Message{} = message, reason) do
    SinkPipeline.default_handle_failed_message(message, reason)
  end
end
