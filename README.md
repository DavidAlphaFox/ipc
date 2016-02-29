IPC
===

The IPC api ( one instance only for now )

    ipc:create(file_name(), MemorySize) -> ok | {error,Reason}
    ipc:attach(file_name()) -> ok | {error,Reason}
    ipc:create_queue(ipc_name(), type(), Size) -> {ok,ipc_id()} | {error,Reason}
    ipc:create_condition(ipc_name(), condition()) -> {ok,ipc_id()} | {error,Reason}
    ipc:lookup_queue(ipc_name()) -> {ok,ipc_id()} | {error,Reason}
    ipc:lookup_condition(ipc_name()) -> {ok,ipc_id()} | {error,Reason}
    ipc:info(ipc_ref(), ipc_item()) -> Value
    ipc:info(ipc_ref()) -> [{ipc_item(),term()}]
    ipc:publish(ipc_ref(), Value) -> ok | {error,Reason}
    ipc:value(ipc_ref()) -> number()
    ipc:subscribe(ipc_ref()) -> {ok,subscription()} | {error,Reason}
    ipc:unsubscribe(subscription())

    ipc_ref() :: ipc_name() | ipc_id().
    filename() :: string().
    ipc_name() :: atom().
    ipc_id() :: unsigned().
    subscription() :: binary()

    condition() :: true | false | ipc_name() | id() |
	       {'not', condition()} |
	       {'and', condition(), condition()} |
	       {'or', condition(), condition()} |
	       {'xor', condition(), condition()} |
	       {'all', [condition()]} |
	       {'any', [condition()]}.
