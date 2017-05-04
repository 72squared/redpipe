Error Handling
==============

Redis Pipelining and Errors
---------------------------
**RedPipe** is opinionated on this point.
When we execute the pipeline, we always raise any errors from the redis client.
Some of your commands may have run.
Others may not.
Any attached callbacks will not be triggered if an exception was raised.
`Futures` will not have any results populated inside of them.

It is your job as an application developer when using redis to make your API behave in an idempotent way.

One way of handling this is to allow the exception to bubble up.
When the call is retried later, make it pick up where it left off.
Figure out how to repair any prior state and complete the operation.
Also design your application to handle partially written records and handle them appropriately.

Another way is to try to roll back the changes.
This is more difficult.
Frankly I'm not exactly sure how it would work.
I don't design my own applications this way.
It seems like you could do it.
But there's also a good chance that the problem that caused the exception may persist.
And that multiple tries one way or another may not be able to restore you to a clean state.
I think it's a losing battle.

If you choose redis, try to think about error cases and don't assume all the commands will proceed in lockstep.
Ask yourself what could go wrong, and how might I recover from it when I read this dirty state,


Errors Raised by RedPipe
------------------------
**RedPipe** raises exceptions of its own under the following scenarios:

* Trying to access the result of a Future object before it has been received.
* Misconfiguring the pipeline object.
* Invalid data type passed to a defined field in a Hash

Maybe there are others?
Anyway, those are the ones that come to mind.
If you run into an issue and don't understand it, `let me know <https://github.com/72squared/redpipe/issues>`_.
I will update the documentation to help better explain it.

