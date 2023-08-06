from setuptools import setup
from setuptools_scm import get_version

try:
    version = get_version()
    assert version is not None
except (AttributeError, AssertionError):
    version = '0.0.0'

setup(name='lwa-observing',
      version=version,
      url='http://github.com/ovro-lwa/lwa-observing',
      packages=['observing'],
      entry_points='''
        [console_scripts]
        lwaobservingt=observing.cli:cli
      ''',
      zip_safe=False))
