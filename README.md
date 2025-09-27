# simple-cache

Non-persistent, minimal key-value store inspired by Redis. Uses Python asyncio event loop to manage clients.

The only dependency is snakeviz, which is for visualizing profiling data. The server has no dependencies and can be run without uv.
See: server shell script.

simple-cache uses RESP 2, so using Redis clients like redis-cli and language-specific Redis SDKs should work, though I've only tested with redis-cli.

The supported commands and their differences compared to the Redis versions are outlined below.

## Supported Commands

<details>

   <summary>Basic commands</summary>


   | Command | Deviation from Redis                      |
   | ------- | ----------------------------------------- |
   | PING    | Optional argument message not supported   |
   | ECHO    | None                                      |
   | TYPE    | Only supports strings, lists, and streams |
   | EXISTS  | None                                      |
   | DEL     | None                                      |
</details>

<details>

   <summary>String commands</summary>

   | Command | Deviation from Redis                                                                                |
   | ------- | --------------------------------------------------------------------------------------------------- |
   | SET     | These optional arguments are not supported:<br>NX<br>XX<br>KEEPTTL<br>GET |
   | GET     | None                                                                                                |
   
</details>

<details>

   <summary>List commands</summary>

   | Command | Deviation from Redis                                  |
   | ------- | ----------------------------------------------------- |
   | RPUSH   | None                                                  |
   | LPUSH   | None                                                  |
   | LLEN    | None                                                  |
   | LRANGE  | None                                                  |
   | LPOP    | None                                                  |
   | BLPOP   | Does not support blocking on multiple lists at a time<br><br>Timeout is 0 if not specified |
</details>

<details>

   <summary>Stream commands</summary>

   | Command | Deviation from Redis            |
   | ------- | ------------------------------- |
   | XADD    | No optional arguments supported |
   | XRANGE  | None                            |
</details>

<details>

   <summary>Other commands</summary>


   | Command  | Deviation from Redis                                                       |
   | -------- | -------------------------------------------------------------------------- |
   | FLUSHDB  | None                                                                       |
   | SHUTDOWN | No optional arguments supported<br><br>Does not fail b/c no saving to disk<br><br>Signal handling not implemented |
</details>

## Configuration Options

- Port: Pass --port flag with port number to shell script to change port simple cache uses. The default port is 6379, just like Redis.
- Debug: Pass --debug flag to enable debug mode. Debug mode logs command handling and variable state.

## Run Tests
- By default, the test shell script runs both unit and integration tests. Pass -u to run only unit tests or -i to run only integration tests. 