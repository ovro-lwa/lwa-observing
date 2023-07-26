from setuptools import setup
from version import get_git_version

try:
    version = get_git_version()
    assert version is not None
except (AttributeError, AssertionError):
    version = '0.0.0'

setup(name='lwa-observing',
      version=version,
      url='http://github.com/ovro-lwa/lwa-observing',
      packages=['observing'],
      zip_safe=False)