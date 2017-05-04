Latency
=======


Pipelining isn't a magic bullet.
If you pipeline 10 thousand commands together, you have to wait until all 10k commands execute and stream back over the wire.

Most of the time, you will find a happy middle ground where 10 or 20 different commands can easily be combined together.
This will make a difference.

When in doubt, profile your code.
Look for the slow spots.
If you dozens or hundreds of network round-trips to redis, RedPipe can help!