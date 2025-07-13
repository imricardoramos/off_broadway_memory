defmodule OffBroadwayMemory.Buffer do
  @moduledoc """
  An in-memory buffer implementation using `:queue`.
  """

  use GenServer
  require Logger

  @initial_state %{queue: :queue.new(), seen: MapSet.new(), length: 0, enabled?: true}

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, nil, opts)
  end

  @impl true
  def init(_opts) do
    {:ok, @initial_state}
  end

  @doc """
  Push messages to the end of thebuffer.
  """
  @spec push(GenServer.server(), list(any()) | any()) :: :ok
  def push(server, messages) do
    GenServer.call(server, {:push, messages})
  end

  @doc """
  Push messages to the start of the buffer.
  """
  @spec push(GenServer.server(), list(any()) | any()) :: :ok
  def push_reverse(server, messages) do
    GenServer.call(server, {:push_reverse, messages})
  end

  @doc """
  Push messages to the buffer asynchronously.
  """
  @spec async_push(GenServer.server(), list(any()) | any()) :: :ok
  def async_push(server, messages) do
    GenServer.cast(server, {:push, messages})
  end

  @doc """
  Pop messages from the buffer.
  """
  @spec pop(GenServer.server(), non_neg_integer()) :: list(any())
  def pop(server, count \\ 1) do
    GenServer.call(server, {:pop, count})
  end

  @doc """
  Clear all messages from the buffer.
  """
  @spec clear(GenServer.server()) :: :ok
  def clear(server) do
    GenServer.call(server, :clear)
  end

  @doc """
  Get the length of the buffer.
  """
  @spec length(GenServer.server()) :: non_neg_integer()
  def length(server) do
    GenServer.call(server, :length)
  end

  def enable(server) do
    GenServer.cast(server, :enable)
  end

  def disable(server) do
    GenServer.cast(server, :disable)
  end

  @impl true
  def handle_call({:push, messages}, _from, state) when is_list(messages) do
    state = push_to_state(state, messages)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:push_reverse, messages}, _from, state) when is_list(messages) do
    state = push_to_state(state, messages, true)

    {:reply, :ok, state}
  end

  def handle_call({:push, _message}, _from, %{enabled?: false} = state),
    do: {:reply, :ok, state} |> dbg()

  def handle_call({:push, message}, _from, state) do
    if MapSet.member?(state.seen, message) do
      {:reply, :ok, %{state | queue: state.queue}}
    else
      updated_queue = :queue.in(message, state.queue)
      updated_seen = MapSet.put(state.seen, message)
      {:reply, :ok, %{state | queue: updated_queue, length: state.length + 1, seen: updated_seen}}
    end
  end

  def handle_call({:pop, _count}, _from, %{length: 0} = state) do
    {:reply, [], state}
  end

  def handle_call({:pop, count}, _from, %{length: length} = state) when count >= length do
    new_state = %{@initial_state | seen: state.seen}
    Process.send_after(self(), :maybe_clear_seen, 60_000)
    {:reply, :queue.to_list(state.queue), new_state}
  end

  def handle_call({:pop, count}, _from, state) do
    {messages, updated_queue} = :queue.split(count, state.queue)

    updated_state = %{state | queue: updated_queue, length: state.length - count}

    {:reply, :queue.to_list(messages), updated_state}
  end

  def handle_call(:clear, _from, state) do
    {:reply, :ok, %{@initial_state | enabled?: state.enabled?}}
  end

  def handle_call(:length, _from, %{length: length} = state) do
    {:reply, length, state}
  end

  def handle_cast(:enable, state) do
    {:noreply, %{state | enabled?: true}}
  end

  def handle_cast(:disable, state) do
    {:noreply, %{state | enabled?: false}}
  end

  @impl true
  def handle_cast({:push, messages}, state) when is_list(messages) do
    state = push_to_state(state, messages)

    {:noreply, state}
  end

  def handle_cast({:push, _message}, %{enabled?: false} = state), do: {:noreply, state} |> dbg()

  def handle_cast({:push, message}, state) do
    updated_queue = :queue.in(message, state.queue)

    {:noreply, %{state | queue: updated_queue, length: state.length + 1}}
  end

  @impl true
  def handle_info(:maybe_clear_seen, state) do
    if state.length == 0 do
      # Reset seen state if queue is still empty after x amount of time
      Logger.debug("OffBroadwayMememory Buffer cleared after 10s")
      {:noreply, %{state | seen: MapSet.new()}}
    else
      {:noreply, state}
    end
  end

  defp push_to_state(state, messages, reversed \\ false) do
    messages = if(state.enabled?, do: reject_seen(messages, state.seen), else: [])
    messages_length = Kernel.length(messages)

    join = :queue.from_list(messages)

    updated_queue =
      if reversed do
        :queue.join(join, state.queue)
      else
        :queue.join(state.queue, join)
      end

    updated_seen = MapSet.union(state.seen, MapSet.new(messages))

    %{state | queue: updated_queue, length: state.length + messages_length, seen: updated_seen}
  end

  defp reject_seen(messages, seen) do
    Enum.reject(messages, &MapSet.member?(seen, &1))
  end
end
