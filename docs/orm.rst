
Why no ORM?
===========


Have an Object relational mapping can make life much simpler.
Automatic indexes, foreign keys, unique constraints, etc.
It hides all that pesky complexity from you.
If you want a good ORM for redis, check out `ROM <http://pythonhosted.org/rom/rom.html#documentation>`_.
It's pretty cool.


`RedPipe` has a different philosophy.
It emphasizes exposing the full power of redis as much as possible.
That means keeping the commands in redis close to the surface so you can use them as you see fit.
An ORM has the tendency to reduce things to the lowest common denominator.
It also bundles lots of multi-step operations together, where one operation feeds another.
`RedPipe` encourages you to take an input and produce an output with at most one network round-trip.
This allows you to pipeline many operations together efficiently and create reusable building-blocks.

