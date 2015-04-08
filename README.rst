.. image:: https://travis-ci.org/galaxyproject/bioblend.png
        :target: https://travis-ci.org/galaxyproject/bioblend

.. image:: https://pypip.in/d/bioblend/badge.png
        :target: https://pypi.python.org/pypi/bioblend/

.. image:: https://landscape.io/github/galaxyproject/bioblend/master/landscape.svg?style=flat
        :target: https://landscape.io/github/galaxyproject/bioblend/master
        :alt: Code Health


BioBlend is a Python library for interacting with `CloudMan`_ and `Galaxy`_'s
API.

BioBlend is supported and tested on:

- Python 2.6 and 2.7 (Python 3.3 and 3.4 have been added in the unreleased master branch)
- Galaxy release_14.02 and later.

Full docs are available at http://bioblend.readthedocs.org with a quick library
overview also available in `ABOUT.rst <./ABOUT.rst>`_.

.. References/hyperlinks used above
.. _CloudMan: http://usecloudman.org/
.. _Galaxy: http://usegalaxy.org/

-----------------------------------------------------------------------------------------

Modifications to Bioblend to make it work with Galaxy in the CCC.

All development will be done using Python 2.7 (should work with 2.6, but no 
guarantees)

Sample scripts are in the directory sample_scripts - you may have to tweak them 
to work with your Galaxy instance. Most of them expect a Python file called 
galaxy_key.py with the following two lines to be present in your Python search 
path (use PYTHONPATH environment variable).

galaxy_key='\<Galaxy_API_key\>';

galaxy_host='http://\<galaxy_host\>:\<port\>';

Note that the galaxy_key is a secret key and should NOT be shared with other 
users. Make sure your galaxy_key.py file cannot be read by other users.

Make sure Python imports this Bioblend and not the system default. Some Python 
installations have their own versions installed and simply setting PYTHONPATH 
will not override the search path. Instead, add something like this in your 
galaxy_key.py file:

import sys;

sys.path = [ "\<path_to_this_bioblend's_parent_directory\>" ] + sys.path;



