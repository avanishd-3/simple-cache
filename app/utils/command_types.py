from typing import Literal

# These types improve type checking by LSPs
BASIC_COMMANDS: set[Literal["PING", "ECHO", "TYPE", "EXISTS", "DEL"]] = {"PING", "ECHO", "TYPE", "EXISTS", "DEL"}
STRING_COMMANDS: set[Literal["SET", "GET"]] = {"SET", "GET"}
LIST_COMMANDS: set[Literal["RPUSH", "LPUSH", "LLEN", "LRANGE", "LPOP", "BLPOP"]] = {"RPUSH", "LPUSH", "LLEN", "LRANGE", "LPOP", "BLPOP"}
STREAM_COMMANDS: set[Literal["XADD", "XRANGE"]] = {"XADD", "XRANGE"}
SET_COMMANDS: set[Literal
                  ["SADD", "SCARD", "SDIFF", "SDIFFSTORE", "SINTER", "SINTERSTORE", "SUNION", "SUNIONSTORE", "SISMEMBER", "SMEMBERS", "SMOVE", "SREM"]] = {"SADD", "SCARD", "SDIFF", "SDIFFSTORE", "SINTER","SINTERSTORE", "SUNION", "SUNIONSTORE", "SISMEMBER", "SMEMBERS", "SMOVE", "SREM"}
TRANSACTION_COMMANDS: set[Literal["INCR"]] = {"INCR"}