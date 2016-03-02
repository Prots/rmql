-define(LOG_ERROR(Format, Data),
    lager:log(error, [], "~p:~p(): " ++ Format ++ "~n~n", [?MODULE, ?LINE] ++ Data)).
-define(LOG_ERROR(Data),
    lager:log(error, [], "~p:~p():~p~n~n", [?MODULE, ?LINE, Data])).

-define(LOG_WARNING(Format, Data),
    lager:log(warning, [], "~p:~p(): " ++ Format ++ "~n~n", [?MODULE, ?LINE] ++ Data)).
-define(LOG_WARNING(Data),
    lager:log(warning, [], "~p:~p():~p~n~n", [?MODULE, ?LINE, Data])).

-define(LOG_INFO(Format, Data),
    lager:log(info, [], "~p.erl:~p: " ++ Format ++ "~n", [?MODULE, ?LINE] ++ Data)).
-define(LOG_INFO(Data),
    lager:log(info, [], "~p:~p():~p~n~n", [?MODULE, ?LINE, Data])).
-define(LOG_DEBUG(Data),
    lager:log(info, [], "~p:~p(): ~p: ~w~n~n", [?MODULE, ?LINE, ??Data, Data])).