Contributing
============

I welcome new ideas.
And bug fixes.
But I want to keep RedPipe clear and focused.
Code bloat stinks.


The changes submitted should adhere to the following principles:

* modular component that does one job well
* allows for efficient network i/o talking to redis
* doesn't nest network round-trips that defeat the point of pipelining
* exposes the power of the redis API first and foremost
* KISS: keep it simple, stupid!

If any individual component starts to feel really complex, it's time to break it up.
Or time to cut it.

For a patch to be accepted, it must pass all the unit tests and flake8 tests.
It should do so for all supported versions of python.

That said, I'm happy to take rough patch requests and make them suitable for merging if the idea is good.
And of course, I'm happy to give credit where it is due.

