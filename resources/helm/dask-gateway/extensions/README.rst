Extensions
==========

The extensions directory is one of two mechanisms for injecting extensions into
the Dask Gateway API.

One approach is to inject your extensions through ``extraConfig`` in
``values.yaml``.

The other approach is to store your extensions as discrete Python modules in the
``extensions`` directory.

The extensions directory approach provides the following benefits:

- It disentangles your Python code from your Helm YAML. 
- It allows you to implement unit tests and maintain continuous integration.

That being said, the extensions directory approach assumes that you have cloned
the upstream chart locally, which may not necessarily be the case. Different
use-cases call for different solutions. 

To leverage the extensions directory approach, copy your Python modules into
``extensions/gateway`` and update ``extraConfig`` in ``values.yaml`` to import
and implement them.

For example, suppose you copy ``foo.py`` into ``extensions/gateway``.

``foo.py`` contains a single function:

.. code-block:: python

    def bar():
        print("bar")

Simply update ``extraConfig`` in ``values.yaml`` to import your module:

.. code-block:: YAML

    from foo import bar

You now have access to the ``bar`` function. 

Practically speaking, your modules will most likely consist of subclasses that
inherit from upstream base classes alongside configuration declarations that
point to the subclasses.

Please note, for a custom authenticator, you can use the ``custom``
authentication type in ``values.yaml``:

.. code-block:: python

  auth:
    type: custom custom:
      class: <module>.<class>

Under the hood, Helm globs the contents of ``extensions/gateway`` and adds them
as key-value pairs to a ConfigMap. When the ConfigMap is mounted on the Dask
Gateway API pods, all of the files are available in ``/etc/dask-gateway``, which
is part of the Python path.

Please note, if your extensions rely on packages that are not installed by
default in the upstream Dask Gateway API image, you will need to add those
packages yourself.