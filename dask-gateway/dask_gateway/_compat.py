from distutils.version import LooseVersion

import distributed


DISTRIBUTED_2_17_0 = distributed.__version__ >= LooseVersion("2.17.0")
