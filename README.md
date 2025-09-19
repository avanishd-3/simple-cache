# simple-cache

Non-persistent, minimal key-value store inspired by Redis. Uses Python asyncio event loop to manage clients.

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
</details>

<details>

   <summary>String commands</summary>

   | Command | Deviation from Redis                                                                                |
   | ------- | --------------------------------------------------------------------------------------------------- |
   | SET     | These optional arguments are not supported:<br><br>EX<br>EXAT<br>PXAT<br>NX<br>XX<br>KEEPTTL<br>GET |
   | GET     | None                                                                                                |
   
</details>

<details>

   <summary>List commands</summary>

   | Command | Deviation from Redis                                  |
   | ------- | ----------------------------------------------------- |
   | RPUSH   | None                                                  |
   | LRANGE  | None                                                  |
   | LPUSH   | None                                                  |
   | LLEN    | None                                                  |
   | LPOP    | None                                                  |
   | BLPOP   | Does not support blocking on multiple lists at a time |
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


   | Command | Deviation from Redis |
   | ------- | ---------------------|
   | FLUSHDB | None                 |
</details>
