Extensions
==========

The extensions sub-directory is one of two mechanisms for injecting
configuration into ``dask_gateway_config.py``. 

The first method relies on ``extraConfig`` in ``values.yaml``. 

To use this method, simply copy your extensions as either packages or modules
into this directory or sub-directories within this directory. 

Helm will copy the contents of any files that end in ``_config.py`` into
``dask_gateway_config.py``. 

The benefit of this approach is it lets you compartmentalize your Python code
from your Helm YAML, which makes it possible to implement unit tests and
continuous integration. 

Please note, if your extensions rely on packages that are not installed by
default in the upstream Dask Gateway API image, you will need to add those
packages yourself.