from setuptools import setup
from version import get_git_version

setup(name='lwa-observing',
      version=get_git_version(),
      url='http://github.com/ovro-lwa/lwa-observing',
      packages=['observing'],
      entry_points='''
        [console_scripts]
        lwaobserving=observing.cli:cli
      ''',
      install_requires=[
          "click",
          "pandas",
          "mnc-python",
          "dsa110-pyutils",
          "uvicorn",
          "fastapi",
          "slack_sdk",
          "protobuf<3.21"
      ],
      zip_safe=False)
