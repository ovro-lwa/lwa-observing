import pytest
import os.path
from observing import parsesdf
import pandas as pd
from observing.parsesdf import make_command
from observing.classes import ObsType, Session, Observation


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

def test_make_command_script():
    fn = os.path.join(_install_dir, 'test.sdf')
    obs_ra = []
    with open(fn, 'r') as fh:
        for line in fh:
            if line.startswith('OBS_RA'):
                fields = line.split()
                obs_ra.append(float(fields[1]))
                
    d = parsesdf.sdf_to_dict(fn)
    session, obs_list = parsesdf.make_obs_list(d)
    df = parsesdf.power_beam_obs(obs_list, session)
    
    for c in df['command']:
        if c.startswith('con.control_bf'):
            if c.find('coord') != -1:
                _, radec = c.split('coord', 1)
                ra, dec, _ = radec.split(',', 2)
                ra = ra.split('(', 1)[-1]
                dec = dec.split(')', 1)[0]
                assert float(ra) == obs_ra.pop(0)

def _two_obs_session(obs_type, second_freq1=None, second_start=0.01):
    session = Session(obs_type, 1, beam_num=1)
    obs_list = []
    for i, start in enumerate([70000.5, 70000.5 + second_start], start=1):
        obs = Observation(session, i, start, 60000, 'TRK_RADEC')
        freq1 = second_freq1 if (i == 2 and second_freq1 is not None) else 1161394218
        obs.set_beam_props(30., 20., int_time=100, bw=7, freq1=freq1, freq2=1599656187, gain=6)
        obs_list.append(obs)
    return session, obs_list


def _times_of(df, text):
    return [mjd for mjd, cmd in df['command'].items() if text in cmd]


@pytest.mark.parametrize('obs_type, func', [(ObsType.power, parsesdf.power_beam_obs),
                                            (ObsType.volt, parsesdf.volt_beam_obs)])
def test_buffer_mode_start_dr_before_configure(obs_type, func):
    session, obs_list = _two_obs_session(obs_type)
    df = func(obs_list, session, mode='buffer')
    starts = _times_of(df, 'start_dr')
    configure = _times_of(df, 'configure_xengine')[0]
    controller = _times_of(df, 'control.Controller')[0]
    assert len(starts) == 2
    assert all(controller < t < configure for t in starts)
    assert list(df.index) == sorted(df.index)
    for obs, t in zip(obs_list, sorted(starts)):
        assert f"t0 = {obs.obs_start})" in df.loc[t, 'command'] or f"t0={obs.obs_start}," in df.loc[t, 'command']


def test_asap_mode_start_dr_unchanged():
    session, obs_list = _two_obs_session(ObsType.power)
    df = parsesdf.power_beam_obs(obs_list, session, mode='asap')
    starts = _times_of(df, 'start_dr')
    assert all("t0 = 'now'" in df.loc[t, 'command'] for t in starts)
    assert sorted(starts) == pytest.approx([obs.obs_start - 2/(24*3600) for obs in obs_list])


def test_volt_retuned_observation_not_early():
    session, obs_list = _two_obs_session(ObsType.volt, second_freq1=1183307316)
    df = parsesdf.volt_beam_obs(obs_list, session, mode='buffer')
    configure = _times_of(df, 'configure_xengine')[0]
    first, second = sorted(_times_of(df, 'start_dr'))
    assert first < configure
    assert second == pytest.approx(obs_list[1].obs_start - 15/(24*3600))


def test_make_obs_list_out_of_order():
    fn = os.path.join(_install_dir, 'test_nm.sdf')
    d = parsesdf.sdf_to_dict(fn)
    obs = d['OBSERVATIONS']
    obs['OBSERVATION_1'], obs['OBSERVATION_2'] = obs['OBSERVATION_2'], obs['OBSERVATION_1']
    with pytest.raises(ValueError, match='starts before previous observation'):
        parsesdf.make_obs_list(d)


def test_make_command_script_nm():
    fn = os.path.join(_install_dir, 'test_nm.sdf')
    obs_ra = []
    with open(fn, 'r') as fh:
        for line in fh:
            if line.startswith('OBS_RA'):
                fields = line.split()
                obs_ra.append(float(fields[1]))
                
    d = parsesdf.sdf_to_dict(fn)
    session, obs_list = parsesdf.make_obs_list(d)
    df = parsesdf.volt_beam_obs(obs_list, session)
    
    for c in df['command']:
        if c.startswith('con.control_bf'):
            if c.find('coord') != -1:
                _, radec = c.split('coord', 1)
                ra, dec, _ = radec.split(',', 2)
                ra = ra.split('(', 1)[-1]
                dec = dec.split(')', 1)[0]
                assert float(ra) == obs_ra.pop(0)
