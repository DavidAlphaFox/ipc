IPC
===

The IPC api ( one instance only for now )

	ipc:create(ShmName, MemorySize) -> ok | {error,Reason}

	ipc:attach(ShmName) -> ok | {error,Reason}

    ipc:create_queue(Name, Type, Size) -> {ok,ID} | {error,Reason}

	ipc:create_subscriber(Name, Expr) -> {ok, ID} | {error,Reason}

	ipc:lookup_queue(Name) -> {ok,ID} | {error,Reason}

	ipc:lookup_subscriber(Name) -> {ok,ID} | {error,Reason}

	ipc:publish(Name|ID, Value) -> ok | {error,Reason}

	ipc:subscribe(Name|ID) -> {ok,Ref} | {error,Reason}

	ipc:unsubscribe(Ref)
