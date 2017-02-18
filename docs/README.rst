E-Maj Documentation
===================

The documentation can be reached at http://emaj.readthedocs.io/


Compile the doc
---------------

* Install Sphinx (here in debian style) ::

   apt-get install python-sphinx


* Get the Sphinx Theme

Install the [Read The Doc theme](https://github.com/snide/sphinx_rtd_theme)::

   pip install sphinx_rtd_theme

And then add the following lines to the ``conf.py`` file::

   import sphinx_rtd_theme
   html_theme = "sphinx_rtd_theme"
   html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]


* Build:: 

   cd docs/<selected_language>
   make html

The resulting files are then located under docs/<language>/_build/html.

