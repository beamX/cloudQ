-module(http).

-export([put/4,
         post/4,
         resp_body/1,
         delete/4
        ]).

put(URL, Headers, Payload, Options) ->
    hackney:put(URL, Headers, Payload, Options).

post(URL, Headers, Payload, Options) ->
    hackney:post(URL, Headers, Payload, Options).

resp_body(Client) -> hackney:body(Client).

delete(URL, Headers, Payload, Options) ->
    hackney:delete(URL, Headers, Payload, Options).
