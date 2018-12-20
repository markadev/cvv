=======================
Concise Version Vectors
=======================

*Playing around with concise version vectors*


Introduction
============

This project is my playground for experimenting with Predecessor Vectors with
Exceptions (PVEs) as described initially in the research paper
`Concise Version Vectors in WinFS <https://dahliamalkhi.files.wordpress.com/2016/08/winfs-version-vectors-dc2007.pdf>`_.


Installation
============

Requirements:

 * Python >= 3.5

I highly recommend using a
`virtual environment <https://pypi.python.org/pypi/virtualenv>`_ to ensure
that you don't mess up your system python packages::

    $ virtualenv -p python3 venv
    $ . venv/bin/activate
    $ pip install -r dev-requirements.txt

To run the tests::

    $ tox


License
=======

This project is licensed under the MIT license.
