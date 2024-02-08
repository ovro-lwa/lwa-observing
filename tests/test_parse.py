import pytest
import os.path
from observing import parsesdf

_install_dir = os.path.abspath(os.path.dirname(__file__))

def test_read():
    fn = os.path.join(_install_dir, 'test.sdf')
    d = parsesdf.sdf_to_dict(fn)
    assert isinstance(d, dict)


def test_read_nm():
    fn = os.path.join(_install_dir, 'test_nm.sdf')
    d = parsesdf.sdf_to_dict(fn)
    assert isinstance(d, dict)


def test_make():
    fn = os.path.join(_install_dir, 'test.sdf')
    d = parsesdf.sdf_to_dict(fn)
    session, obs_list = parsesdf.make_obs_list(d)
    df = parsesdf.power_beam_obs(obs_list, session)
