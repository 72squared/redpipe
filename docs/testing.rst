Testing
=======

There
This will set up the virtualenv and install all the necessary test packages.
It also puts you in a shell with the virtualenv path declared.


.. code-block:: bash

    ./activate


Then you can run the tests:

.. code-block:: bash

    make test

This will run tox against a bunch of different python versions.



If you only want to run the test with your local python version, you can do:

.. code-block:: bash

    ./test.py

When you are done, hit control-d to exit the shell.


