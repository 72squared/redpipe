Structs
======
*RedPipe* does not provide you with an ORM solution.
There are many other packages out there that do that much better.
Choose *Redpipe* if you really care about optimizing your network i/o.
To do that effectively, you need to combine as many different parallel operations as possible.
ORMs often hide things like automatic unique constraints and indexes beneath the covers.
That makes it tricky to ensure you are batching those operations efficently as possible over the network.

RedPipe exposes these primitives keep the data structures simple inputs and outputs.
This allows us to construct with building blocks that can be pipelined efficiently.

However, it is nice when storing data into redis to be able to load data and persist it into a hash and still preserve the data-type of the original structure.
You could use a string key and serialize in json.
This would defeat the power of redis.


Saving
------


Loading
-------


Deleting
--------


Accessing Properties
--------------------


Extra Fields
------------


Underlying Hash
---------------



