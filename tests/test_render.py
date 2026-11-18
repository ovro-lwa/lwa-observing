import os
import pytest
import os.path
import tempfile
from observing import makesdf, parsesdf, classes


def test_make_obs_block_tiptilt_true():
    text = makesdf.make_obs_block(
        1, '2024-02-08T17:18:19', 10000, ra=0.0, dec=90.0, obj_name='NCP',
        integration_time=100, obs_mode=classes.EphemModes('TRK_RADEC'), tiptilt=True)
    assert 'OBS_APPLY_TIPTILT True' in text


def test_make_obs_block_tiptilt_absent_by_default():
    text = makesdf.make_obs_block(
        1, '2024-02-08T17:18:19', 10000, ra=0.0, dec=90.0, obj_name='NCP',
        integration_time=100, obs_mode=classes.EphemModes('TRK_RADEC'))
    assert 'OBS_APPLY_TIPTILT' not in text


def test_make():
    outname = tempfile.mktemp(suffix='.sdf')
    makesdf.create(outname,
                   sess_id = 1,
                   sess_mode = 'VOLT',
                   beam_num = 1,
                   pi_name = 'jdowell',
                   n_obs = 1,
                   obs_start = '2024-2-8T17:18:19',
                   obs_dur = 10000,
                   ra = 0.0,
                   dec = 90.0,
                   obj_name = 'NCP')

    try:
        os.unlink(outname)
    except OSError:
        pass


def test_make_and_read():
    outname = tempfile.mktemp(suffix='.sdf')
    makesdf.create(outname,
                   sess_id = 1,
                   sess_mode = 'VOLT',
                   beam_num = 1,
                   pi_name = 'jdowell',
                   n_obs = 1,
                   obs_start = '2024-2-8T17:18:19',
                   obs_dur = 10000,
                   ra = 0.0,
                   dec = 90.0,
                   obj_name = 'NCP')
    
    d = parsesdf.sdf_to_dict(outname)
    assert isinstance(d, dict)

    session, obs_list = parsesdf.make_obs_list(d)
    df = parsesdf.volt_beam_obs(obs_list, session)

    try:
        os.unlink(outname)
    except OSError:
        pass
