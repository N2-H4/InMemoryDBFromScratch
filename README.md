## In-memory key-value database prototype made from scratch with use of Winsock2.
Made in order to get some exposure to network programming and data structures.

CMDs:  

**keys** - returns all stored keys  

**get [key]** - returns value of key or null if key doesnt exist  

**set [key] [value]** - adds new key with value or changes value of existing key, returns null  

**del [key]** - deletes key value pair specified by key, returns 1 if deleted succesfully or 0 if nothing weas deleted  

**expire [key] [int value]** - sets time-to-live for key  

**ttl [key]** - returns time-to-live of a key, returns -1 if key doesnt have ttl set or -2 if key doesnt exist  

**zadd [name] [float score] [value]** - adds [value] with [score] to sorted set [name], returns 1 if added succesfully otherwise 0, creates new sorted set if needed  

**zrem [name] [value]** - removes [value] from sorted set [name], returns 1 if value was deleted, otherwise 0  

**zscore [name] [value]** - returns score of [value] from sorted set [name], returns null if value not found  

**zquery [name] [float score] [value] [int offset] [int limit]** - returns array of values from sorted set [name]. Array is a sublist of (score, value) pairs >= ([score],[value]).   
Sublist can be offset with [offset] and number of retuned elements can be limited with [limit]. To query by score [value] can be left empty.
To query by value [score] can be fixed. To query by rank [score] can be -inf and [value] equal "".  

**zquerydesc [name] [float score] [value] [int offset] [int limit]** - returns array of values from sorted set [name]. Array is a sublist of (score, value) pairs < ([score],[value]). 
Sublist can be offset with [offset] and number of retuned elements can be limited with [limit]. To query by score [value] can be left empty. To query by value [score] can be fixed. 
To query by rank [score] can be -inf and [value] equal "".
