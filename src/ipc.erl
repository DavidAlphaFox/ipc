%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2016, Tony Rogvall
%%% @doc
%%%    ipc api
%%% @end
%%% Created : 24 Feb 2016 by Tony Rogvall <tony@rogvall.se>

-module(ipc).

-on_load(init/0).

-export([start/0]).
-export([create/2, attach/1]).
-export([create_queue/3, lookup_queue/1]).
-export([create_condition/2, lookup_condition/1]).
-export([first/0, next/1, info/1, info/2]).
-export([value/1, value/2]).
-export([publish/2]).
-export([subscribe/2]).

-export([compile/1, compile/2, compile_/2]).
-export([optimise/1]).
-export([eval/3]).
-export([dump/0]).
-export([test_srv/0, test_cli/0]).
-export([bench_write/0, bench_read/0, bench_read_more/0]).
-export([bench_write/1, bench_read/1, bench_read_more/1]).
-export([test_compile/0, test_comp/2]).

%% low level nifs
-export([publish_/2]).
-export([subscribe_/2]).
-export([create_cond_/3]).


-type expr() :: {all,[expr()]} | {any,[expr()]} | {'not',expr()} | 
		atom() | integer().

start() ->
    application:start(ipc).

init() ->
    Nif = filename:join([code:priv_dir(ipc), "ipc_nif"]),
    erlang:load_nif(Nif, 0).

create(_ShmName, _MemorySize) ->
    erlang:error(nif_not_loaded).

attach(_ShmName) ->
    erlang:error(nif_not_loaded).

first() ->
    erlang:error(nif_not_loaded).

next(_Offset) ->
    erlang:error(nif_not_loaded).

info(Offset) ->
    Name = info(Offset, name),
    ObjectType = info(Offset, object_type),
    if ObjectType =:= queue ->
	    [{object_type, ObjectType},
	     {name,Name},
	     {id, Offset},
	     {size, info(Offset,size)}, 
	     {type,info(Offset,type)},
	     {link, info(Offset,link)}
	    ];
       ObjectType =:= condition ->
	    [{object_type, ObjectType},
	     {name,Name}, {id, Offset},
	     {links, info(Offset,links)}
	    ]
    end.

info(_Offset, _What) ->
    erlang:error(nif_not_loaded).

create_queue(_Name,_Type,_Size) ->
    erlang:error(nif_not_loaded).

lookup_queue(Name) when is_atom(Name) ->
    lookup_(first(), queue, Name).

-spec create_condition(Name::atom(), Expr::expr()) -> {ok,integer()} |
						      {error,atom()}.
create_condition(Name, Expr) ->
    {Queues,Prog} = compile(Expr),
    create_cond_(Name, Prog, Queues).
    
create_cond_(_Name, _Prog, _Queues) ->
    erlang:error(nif_not_loaded).

-spec lookup_condition(Name::atom()) -> {ok,integer()} | {error,term()}.
lookup_condition(Name) when is_atom(Name) ->
    lookup_(first(), condition, Name).

-spec publish(Name::atom()|integer(), Value::number()) -> ok | {error,term()}.
publish(Name, Value) when is_atom(Name) ->
    case lookup_queue(Name) of
	{ok, ID} -> publish_(ID, Value);
	Error -> Error
    end;
publish(ID, Value) when is_integer(ID) ->
    publish_(ID, Value).

publish_(_ID, _Value) ->
    erlang:error(nif_not_loaded).

-spec value(Name::atom()|integer()) -> number().
value(Name) ->
    value(Name, 0).

-spec value(Name::atom()|integer(), Index::integer()) -> number().
value(Name,Index) when is_atom(Name), is_integer(Index), Index >= 0 ->
    case lookup_queue(Name) of
	{ok, ID} -> value_(ID,Index);
	Error -> Error
    end;
value(ID,Index) when is_integer(ID),is_integer(Index), Index >= 0 ->
    value_(ID, Index).

value_(_ID,_Index) ->
    erlang:error(nif_not_loaded).

-spec subscribe(Name::atom()|integer(),Message::term()) ->
		       {ok,binary()} | {error,term()}.
subscribe(Name,Message) when is_atom(Name) ->
    case lookup_condition(Name) of
	{ok, ID} -> subscribe_(ID,Message);
	Error -> Error
    end;
subscribe(ID,Message) when is_integer(ID) ->
     subscribe_(ID,Message).

subscribe_(_ID,_Message) ->
    erlang:error(nif_not_loaded).

lookup_(eot, _ObjectType, _Name) ->
    {error,enoent};
lookup_(Offset, ObjectType, Name) ->
    case info(Offset, object_type) of
	ObjectType ->
	    case info(Offset, name) of
		Name ->
		    {ok,Offset};
		_ ->
		    lookup_(next(Offset), ObjectType, Name)
	    end;
	_ ->
	    lookup_(next(Offset), ObjectType, Name)
    end.

dump() ->
    dump(first()).

dump(eot) ->
    ok;
dump(Offset) ->
    io:format("~p\n", [info(Offset)]),
    dump(next(Offset)).

test_srv() ->
    create("foo", 64*1024),
    {ok,I1} = create_queue(queue1, unsigned32, 1024),
    {ok,I2} = create_queue(queue2, unsigned32, 1024),
    {ok,I3} = create_queue(my_queue, unsigned16, 8),
    {ok,I4} = create_queue(my_output, float32, 8),
    %%
    {ok,C1} = create_condition(cond1, {'or', queue1, queue2}),
    {ok,C2} = create_condition(cond2, {'and', queue1, queue2}),
    {ok,C3} = create_condition(cond3, {'xor', queue1, queue2}),
    {ok,C4} = create_condition(cond4, 
			       {'xor', {'and', my_queue, my_output},
				{'and', queue1, queue2}}),
    %%
    [I1,I2,I3,I4,C1,C2,C3,C4].

test_cli() ->
    attach("foo").

-define(BENCH_N,     1000).
-define(BENCH_SLEEP, 50).   %% micro

bench_write() ->
    bench_write(queue1).

bench_write(Queue) ->
    N = ?BENCH_N,
    case lookup_queue(Queue) of
	{ok,QID} ->
	    T0 = erlang:system_time(micro_seconds),
	    bench_write_loop(N,QID),
	    T1 = erlang:system_time(micro_seconds),
	    trunc((N/(T1-T0))*1000000);
	Error ->
	    Error
    end.

bench_write_loop(0,_QID) ->
    ok;
bench_write_loop(I,QID) ->
    publish(QID, I),
    sleep_micro(?BENCH_SLEEP),
    bench_write_loop(I-1,QID).

bench_read() ->
    bench_read(cond1).

bench_read(Cond) when is_atom(Cond) ->
    case lookup_condition(Cond) of
	{ok,CID} ->
	    subscribe(CID, ready),
	    timer:send_after(10000, stop),
	    read_loop(CID, ready, ?BENCH_N, 0);
	Error -> Error
    end.

%% no extra subscription
bench_read_more()  ->
    bench_read_more(cond1).

bench_read_more(Cond) when is_atom(Cond) ->
    case lookup_condition(Cond) of
	{ok,CID} ->
	    timer:send_after(10000, stop),
	    read_loop(CID, ready, ?BENCH_N, 0);
	Error -> Error
    end.

read_loop(_CID, _Tag, I, N) when I =< 0 ->
    N;
read_loop(CID, Tag, I, N) ->
    receive
	{subscription, CID, Vs, Tag} ->
	    J = element(1, Vs),
	    if I =/= J ->
		    read_loop(CID, Tag, J-1, N+1);
	       true ->
		    read_loop(CID, Tag, I-1, N)
	    end;
	stop ->
	    N
    end.

sleep_micro(Micro) ->
    T0 = erlang:system_time(micro_seconds),
    Milli = Micro div 1000,
    if Milli > 10 -> 
	    timer:sleep(Milli - 2),
	    T1 = erlang:system_time(micro_seconds),
	    sleep_micro_spin_(T1,(T1-T0)-Micro);
       true ->
	    sleep_micro_spin_(T0,Micro)
    end.

sleep_micro_spin_(T,Micro) ->
    erlang:yield(),
    T1 = erlang:system_time(micro_seconds),
    if T1 - T >= Micro -> ok;
       true -> sleep_micro_spin_(T,Micro)
    end.

test_compile() ->
    Env = #{ a => 10, b => 12, c=>13 },
    test_comp({'and', a, b}, Env),
    test_comp({'or', a, b}, Env),
    test_comp({'xor', a, b}, Env),
    test_comp({'and', c, {'and',a, b}}, Env),
    test_comp({'any', 
	       [{'not',a}, {'not',b}, {'not',c},{'and',a,b}]},Env).


    
test_comp(E, Env) ->
    io:format("compile: ~p\n", [E]),
    Prog1 = compile(E, Env),
    Prog2 = optimise(Prog1),
    Prog2.

%% encode and check subscribtion expression
%% expression encoding:
%%
%%  Q <offs>   test if Q is updated, if so jump to offs
%%
%%  special values Q=1 means always, Q=0 means never
%%  offs = 1 means return true, offs=0 means return false
%%  negative Q means not updated
%% 
-define(TRUE,   1).
-define(FALSE, -1).
-define(RETURN_TRUE,  {?TRUE,1}).
-define(RETURN_FALSE, {?TRUE,0}).
-define(NOP,          {0,0}). %% special case!

compile(E) ->
    Prog1 = compile(E, #{}),
    Prog2 = optimise(Prog1),
    R = {Q,Prog3} = normalize(Prog2),
    io:format("Q = ~p, Prog=~p\n", [Q,Prog3]),
    R.

normalize(Prog) ->
    Ls = tuple_to_list(Prog),
    Qs = [0,1|lists:usort([abs(Q) || {Q,_} <- Ls, abs(Q) > 1])],
    Map = maps:from_list(lists:zip(Qs, lists:seq(0,length(Qs)-1))),
    NProg = list_to_tuple([{maps:get(Q,Map),Offs} || {Q,Offs} <- Ls]),
    {list_to_tuple(Qs),NProg}.

compile(E,Env) ->
    Es = compile_(E,Env),
    list_to_tuple(Es ++ [?RETURN_FALSE, ?RETURN_TRUE]).

compile_(true,_Env)  -> [{?TRUE,2}];
compile_(false,_Env) -> [{?FALSE,2}];
compile_(Name,Env) when is_atom(Name) ->
    case maps:find(Name, Env) of
	{ok,QID} -> [{QID,2}];
	error ->
	    {ok,QID} = lookup_queue(Name),
	    [{QID,2}]
    end;
compile_(QID,_Env) when is_integer(QID), QID =/= 0 ->
    [{QID,2}];
compile_({'not', E},Env) ->
    %% A F T => A +2 F T
    compile_(E,Env)++[{?TRUE,2}];
compile_({'and', A, B},Env) ->
    %%  A Fa Ta   B Fb Tb  -> A L(B)+1 B Fab Tab
    As = compile_(A,Env),
    Bs = compile_(B,Env),
    As ++ [{?TRUE,length(Bs)+1}] ++ Bs;
compile_({'or', A, B}, Env) ->
    %%  A Fa Ta   B Fb Tb  -> A +2 L(B)+2 B Fab Tab
    As = compile_(A, Env),
    Bs = compile_(B, Env),
    As ++ [{?TRUE,2}] ++ [{?TRUE,length(Bs)+2}] ++ Bs;
compile_({'xor', A, B}, Env) ->
    %% ((not A) and B) or (A and not B)
    compile_({'or',{'and',{'not',A},B},{'and',A,{'not',B}}}, Env);
compile_({'all', Es}, Env) ->
    compile_(make_op('and', Es), Env);
compile_({'any', Es}, Env) ->
    compile_(make_op('or', Es), Env).

make_op('and', []) -> true;
make_op('or', []) -> false;
make_op(_Op, [A]) -> A;
make_op(Op, [A,B]) -> {Op,A,B};
make_op(Op, [A|As]) ->  {Op,A,make_op(Op,As)}.

%%      {Q, Pos}
%% Pos: {?TRUE, Pos2} | {Q, Pos2}
%% ------------------------------
%%  {Q, Pos2}
%%
optimise(Prog) ->
    io:format(" prog: ~p\n", [Prog]),
    Prog1 = short_jumps(1, Prog),
    io:format(" short: ~p\n", [Prog1]),
    %% Prog2 = move_jumps(1, Prog1),
    %% io:format(" move: ~p\n", [Prog2]),
    Prog2 = remove_unreach(Prog1),
    io:format(" unreach(1): ~p\n", [Prog2]),
    Prog3 = remove_nops(Prog2),
    io:format(" nops: ~p\n", [Prog3]),
    Prog4 = remove_unreach(Prog3),
    io:format(" unreach(2): ~p\n", [Prog4]),
    Prog4.

%% remove nop {0,0} from the code
remove_nops(Prog) ->
    remove_instructions(nop_list(Prog,1,[]),Prog).

nop_list(Prog,I,Acc) when I > tuple_size(Prog) ->
    Acc;
nop_list(Prog,I,Acc) ->
    case element(I,Prog) of
	?NOP -> nop_list(Prog,I+1,[I|Acc]);
	_ -> nop_list(Prog,I+1,Acc)
    end.
    
%% add negations 
%%     {Q,Offs}, {1,0} ... {A,B} => 
%%        {-Q,0}, {1,Offs-1} ... {A,B}
%% ( need to remove unreach after this pass )

remove_unreach(Prog) ->
    Reachable = sets:to_list(reach(1, Prog, sets:new())),
    io:format("reachable = ~p\n", [Reachable]),
    Unreach = lists:reverse(lists:sort(lists:seq(1,tuple_size(Prog)) -- 
					   Reachable)),
    remove_instructions(Unreach, Prog).

remove_instructions([I|Is], Prog) ->
    Prog1 = remove_instruction(I, Prog),
    io:format("  remove: ~w  ~p\n", [I, Prog1]),
    remove_instructions(Is, Prog1);
remove_instructions([], Prog) ->
    Prog.

remove_instruction(I, Prog) ->
    Prog1 = erlang:delete_element(I, Prog),
    update_offset(I-1, Prog1, 1).

update_offset(0, Prog, _Jump) ->
    Prog;
update_offset(I, Prog, Jump) ->
    case element(I, Prog) of
	{_Q, 2} when 2 > Jump -> %% test is noop
	    update_offset(I-1, setelement(I, Prog, ?NOP), Jump+1);
	{Q, J} when J > Jump ->
	    update_offset(I-1, setelement(I, Prog, {Q,J-1}), Jump+1);
	_ ->
	    update_offset(I-1, Prog, Jump+1)
    end.

%% mark all reachable instructions
reach(I, Prog, Mark) when I > tuple_size(Prog) ->
    Mark;
reach(I, Prog, Mark) ->
    case sets:is_element(I, Mark) of
	true -> 
	    Mark;
	false ->
	    Mark1 = sets:add_element(I, Mark),
	    case element(I, Prog) of
		?RETURN_TRUE  -> Mark1;
		?RETURN_FALSE -> Mark1;
		{?FALSE, _} -> reach(I+1,Prog,Mark1);
		{?TRUE,Offs} -> reach(I+Offs,Prog,Mark1);
		{_Q,Offs} ->
		    Mark2 = reach(I+Offs, Prog, Mark1),
		    reach(I+1,Prog,Mark2)
	    end
    end.

%%
%% Move a jump always with the destination instruction
%%
move_jumps(I, Prog) when I > tuple_size(Prog) ->
    Prog;
move_jumps(I, Prog) ->
    case element(I, Prog) of
	{_, 1} -> move_jumps(I+1, Prog);
	{_, 0} -> move_jumps(I+1, Prog);
	{?TRUE,Offs} ->
	    Instr = move_jump(I,Prog,Offs),
	    io:format("move: i=~w, instr ~w replaced by ~w\n", 
		      [I,{1,Offs}, Instr]),
	    Prog1 = setelement(I, Prog, Instr),
	    move_jumps(I+1, Prog1);
	{_Q,_Offs} ->
	    move_jumps(I+1, Prog)
    end.

move_jump(I,Prog,Offs) ->
    case element(I+Offs,Prog) of
	{Q, 1} -> {Q,1};
	{Q, 0} -> {Q,0};
	{?TRUE,Offs1} -> move_jump(I,Prog,Offs+Offs1);
	{Q,Offs1} -> {Q,Offs+Offs1}
    end.

%%
%% Short circuit jumps by moving jumps leading to jumps
%% into a longer jump.
%%
short_jumps(I, Prog) when I > tuple_size(Prog) ->
    Prog;
short_jumps(I, Prog) ->
    case element(I, Prog) of
	{_, 1} -> short_jumps(I+1, Prog);
	{_, 0} -> short_jumps(I+1, Prog);
	{Q,Offs} ->
	    Offs1 = short_jump(I,Prog,Offs,Q),
	    io:format("short: i=~w, offs ~w replace by ~w\n", 
		      [I,Offs,Offs1]),
	    Prog1 = setelement(I, Prog, {Q, Offs1}),
	    short_jumps(I+1, Prog1)
    end.

short_jump(I,Prog,Offs,Q) ->
    case element(I+Offs,Prog) of
	{Q, 1}        -> 1;
	{?TRUE, 1}    -> 1;
	{Q, 0}        -> 0;
	{?TRUE, 0}    -> 0;
	{Q, Offs1}    -> short_jump(I,Prog,Offs+Offs1,Q);
	{?TRUE,Offs1} -> short_jump(I,Prog,Offs+Offs1,Q);
	_ -> Offs
    end.

%% eval (just for semantics) really imlemented in C code
eval(Prog, Clock, Env) when is_list(Env) ->
    eval(1, Prog, Clock, maps:from_list(Env));
eval(Prog, Clock, Env) ->
    eval(1, Prog, Clock, Env).

eval(I, Prog, Clock, Env) ->
    case element(I, Prog) of
	?NOP -> eval(I+1,Prog,Clock,Env);
	?RETURN_TRUE -> true;
	?RETURN_FALSE -> false;
	{Q,Offs} ->
	    Cond = 
		if Q =:= ?TRUE -> true;
		   Q =:= ?FALSE -> false;
		   Q < 0 -> maps:get({clock,-Q}, Env) =< Clock;
		   Q > 0 -> maps:get({clock,Q}, Env) > Clock
		end,
	    if not Cond -> eval(I+1,Prog,Clock,Env);
	       Offs =:= 0 -> false;
	       Offs =:= 1 -> true;
	       Offs > 1 ->  eval(I+Offs,Prog,Clock,Env)
	    end
    end.

	    

		    
