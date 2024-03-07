import pytest
import os.path
from observing import parsesdf
import pandas as pd
from observing.parsesdf import make_command


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


def test_make_nm():
    fn = os.path.join(_install_dir, 'test_nm.sdf')
    d = parsesdf.sdf_to_dict(fn)
    session, obs_list = parsesdf.make_obs_list(d)
    df = parsesdf.volt_beam_obs(obs_list, session)

def test_make_command():
    mjd = 2459597.5
    command = "settings.update"
    expected_df = pd.DataFrame({mjd: [command]}, index=['command']).transpose()

    result_df = make_command(mjd, command)

    assert result_df.equals(expected_df)

def test_make_command_invalid():
    mjd = 2459597.5
    command = "invalid.command"
    expected_df = None

    result_df = make_command(mjd, command)

    assert result_df == expected_df
