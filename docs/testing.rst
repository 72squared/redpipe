Testing
=======
Testing is really important with any code library.
It is especially important when working with database libraries.
So much depends on them.

I try to be as thorough as I can in testing each facet of code.

All of the tests are contained in one file at the root of the repo:

`./test.py`

I could split it up, but it is convenient at the moment to have all the tests in one file.
And it can find the path to the redpipe package without any special hoops to jump through.

If you see an area that has not been well tested, `let me know <https://github.com/72squared/redpipe/issues>`_.


Test Setup
----------

Check out the code from `GitHub <https://github.com/72squared/redpipe/>`_.

Open a shell at the root of the repo.

Then type this command:

.. code-block:: bash

    ./activate

This will set up the virtualenv and install all the necessary test packages.

It also puts you in a shell with the virtualenv path declared.

Running the Tests
-----------------

If you only want to run the test, you can just run the test script:

.. code-block:: bash

    ./test.py

When you are done, hit control-d to exit the shell.


Running Tests Against Supported Python Versions
-----------------------------------------------
To go through a more thorough test suite, run:

.. code-block:: bash

    make test

This will run tox against a bunch of different python versions and print out coverage.
To run this, you need the following python versions installed and discoverable in your path:

* python2.7
* python3.3
* python3.4
* python3.5

This will also print out code coverage statistics and lint tests.

I expect all of these code tests to pass fully before accepting patches to master.


Using Docker to Test
--------------------
There's a docker image to help you set up all these versions of python.
It will check them out and run tox.

To run the tests, type:

.. code-block:: bash

    docker build . -t redpipe && docker run redpipe

If you need to jump in and debug stuff, do:

.. code-block:: bash

    docker build . -t redpipe && docker run -it redpipe /bin/bash


Building Documentation
----------------------
To build this documentation, there's a make command:

.. code-block:: bash

    make documentation

This will run the `sphinx-build` command to create the local version of the docs.
The docs are automatically published to `Read the Docs <http://redpipe.readthedocs.io/en/latest/>`_.
But it's handy to build locally before publishing.