Testing
=======


Test Setup
----------

To will set up the virtualenv and install all the necessary test packages.
It also puts you in a shell with the virtualenv path declared.


.. code-block:: bash

    ./activate

If you only want to run the test, you can just run the test script:

.. code-block:: bash

    ./test.py

When you are done, hit control-d to exit the shell.


To go through a more thorough test suite, run:

.. code-block:: bash

    make test

This will run tox against a bunch of different python versions and print out coverage.
To run this, you need the following python versions installed and discoverable in your path:

* python2.7
* python3.3
* python3.4
* python3.5

