defmodule Sequin.Sinks.Duckdb.Client do
  @moduledoc false
  alias Sequin.Consumers.DuckdbSink
  alias Sequin.Error

  require Logger

  def write_batch(%DuckdbSink{} = sink, records, opts \\ []) do
    case connect(sink) do
      {:ok, conn} ->
        try do
          write_records(conn, sink, records, opts)
        after
          :duckdb.close(conn)
        end

      {:error, reason} ->
        {:error, Error.service(service: :duckdb, code: :connection_error, message: "Failed to connect: #{inspect(reason)}")}
    end
  end

  defp connect(%DuckdbSink{database_path: path}) do
    :duckdb.open(String.to_charlist(path))
  end

  defp write_records(conn, %DuckdbSink{table_name: table_name} = _sink, records, _opts) do
    # Ensure records is a list of maps
    records = Enum.map(records, &normalize_record/1)

    case create_table_if_not_exists(conn, table_name, records) do
      :ok ->
        insert_records(conn, table_name, records)

      {:error, reason} ->
        {:error, Error.service(service: :duckdb, code: :table_creation_error, message: "Failed to create table: #{inspect(reason)}")}
    end
  end

  defp normalize_record(record) when is_map(record), do: record
  defp normalize_record(record), do: Map.new(record)

  defp create_table_if_not_exists(conn, table_name, [first_record | _]) do
    columns = map_to_columns(first_record)
    create_table_sql = "CREATE TABLE IF NOT EXISTS #{table_name} (#{columns})"
    
    case :duckdb.query(conn, String.to_charlist(create_table_sql)) do
      {:ok, _, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp create_table_if_not_exists(_conn, _table_name, []), do: :ok

  defp map_to_columns(record) do
    record
    |> Map.keys()
    |> Enum.map(&"#{&1} VARCHAR")
    |> Enum.join(", ")
  end

  defp insert_records(_conn, _table_name, []), do: {:ok, []}
  
  defp insert_records(conn, table_name, records) do
    columns = Map.keys(hd(records))
    placeholders = Enum.map(1..length(columns), fn i -> "$#{i}" end) |> Enum.join(", ")
    
    insert_sql = "INSERT INTO #{table_name} (#{Enum.join(columns, ", ")}) VALUES (#{placeholders})"

    results = 
      Enum.map(records, fn record ->
        values = Enum.map(columns, fn col -> Map.get(record, col) end)
        case :duckdb.query(conn, String.to_charlist(insert_sql), values) do
          {:ok, _, _} -> :ok
          {:error, reason} -> {:error, reason}
        end
      end)

    case Enum.find(results, &(match?({:error, _}, &1))) do
      nil -> {:ok, length(results)}
      {:error, reason} -> 
        {:error, Error.service(service: :duckdb, code: :insert_error, message: "Failed to insert records: #{inspect(reason)}")}
    end
  end
end
