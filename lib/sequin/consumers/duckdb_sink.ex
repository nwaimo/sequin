defmodule Sequin.Consumers.DuckdbSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:database_path, :table_name]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:duckdb], default: :duckdb
    field :database_path, :string
    field :table_name, :string
    field :batch_size, :integer, default: 100
    field :timeout_seconds, :integer, default: 30
    field :routing_mode, Ecto.Enum, values: [:dynamic, :static]
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :database_path,
      :table_name,
      :batch_size,
      :timeout_seconds,
      :routing_mode
    ])
    |> validate_required([:database_path])
    |> validate_database_path()
    |> validate_length(:table_name, max: 1024)
    |> validate_number(:batch_size, greater_than: 0, less_than_or_equal_to: 10_000)
    |> validate_number(:timeout_seconds, greater_than: 0, less_than_or_equal_to: 3600)
    |> validate_routing()
  end

  defp validate_database_path(changeset) do
    changeset
    |> validate_change(:database_path, fn :database_path, path ->
      cond do
        String.trim(path) == "" ->
          [database_path: "cannot be blank"]

        not String.ends_with?(path, ".db") and not String.ends_with?(path, ".duckdb") ->
          [database_path: "must end with .db or .duckdb"]

        String.contains?(path, "..") ->
          [database_path: "cannot contain '..' for security reasons"]

        true ->
          []
      end
    end)
    |> validate_length(:database_path, max: 4096)
  end

  defp validate_routing(changeset) do
    routing_mode = get_field(changeset, :routing_mode)

    cond do
      routing_mode == :dynamic ->
        put_change(changeset, :table_name, nil)

      routing_mode == :static ->
        validate_required(changeset, [:table_name])

      true ->
        add_error(changeset, :routing_mode, "is required")
    end
  end

  def client_params(%__MODULE__{} = sink) do
    [
      database_path: sink.database_path,
      table_name: sink.table_name,
      timeout_seconds: sink.timeout_seconds
    ]
  end
end
