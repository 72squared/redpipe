Futures
=======
When redis clients communicate with the server, they send a command, then wait for the response.
The `redis-py` client reflects this design choice.
But when you pipeline, you don't wait.
You queue up a bunch of commands.
Then you execute them.
Then you gather these results and feed them back to where they need to go.

This forces the code invoking the execute method to *know* what to do with all the responses.

That's pretty inconvenient.

The **RedPipe** `Future` makes it possible to get a reference to the data before pipeline execute happens.
A `Future` is a contract that says the result of this redis command will be populated into this object once the pipeline executes.
And you can use a `Future` just like you would the actual result.
But only after the pipeline executes.
If you try to use it prior to that, it raises a `redpipe.ResultNotReady` exception.
Kaboom!

Well, what use is that?

For one, it makes it easier to assign it a variable name in the context of calling the command.
No confusion about how to get the result you need from the array of results returned by `pipeline.execute`.
Unlike `redis-py` **RedPipe** does not return an array of results on the `execute` call.
You already have the result as a variable returned from the command you called initially.

More importantly, the `Future` can be passed into a python closure that can do additional work.
Tell the pipeline to execute your closure callback function after it runs.
Now you have a powerful mechanism to format those results and build modular reusable components.

Still don't quite see how? Read more about `callbacks <callbacks.html>`_.