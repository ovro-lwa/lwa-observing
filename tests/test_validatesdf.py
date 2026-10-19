import os.path
import shutil
import tempfile

import pytest
from astropy.time import Time

from observing import makesdf, validatesdf
from observing.classes import EphemModes

_install_dir = os.path.abspath(os.path.dirname(__file__))
TEST_SDF = os.path.join(_install_dir, 'test.sdf')
# test.sdf observation starts at 2023-08-05 00:55:00 UTC
BEFORE_TEST_SDF = Time('2023-08-05T00:00:00', format='isot')


def write_sdf(tmp_path, text):
    fn = tmp_path / 'session.sdf'
    fn.write_text(text)
    return str(fn)


def power_sdf(tmp_path, obs_blocks):
    """ Write a POWER beam SDF using makesdf, with obs_blocks as list of (start_isot, dur_ms, ra_deg, dec_deg)
    """
    text = makesdf.make_session_preamble(123, validatesdf.ObsType.power, beam_num=3, config_dir=TEST_SDF)
    for i, (start, dur, ra, dec) in enumerate(obs_blocks, start=1):
        text += makesdf.make_obs_block(i, start, dur, ra=ra, dec=dec, obj_name='src', integration_time=100,
                                       obs_mode=EphemModes.trk_radec) + '\n'
    return write_sdf(tmp_path, text)


def test_valid_sdf():
    report = validatesdf.validate(TEST_SDF, now=BEFORE_TEST_SDF)
    assert report.ok, report.errors
    assert report.session_mode_name == '777_POWER3'
    assert report.sched is not None and len(report.sched)
    assert 'Result: VALID' in report.format(now=BEFORE_TEST_SDF)


def test_volt_sdf_missing_session_mode():
    report = validatesdf.validate(os.path.join(_install_dir, 'test_nm.sdf'), now=Time('2024-02-08T00:00:00'))
    assert any('Missing SESSION_MODE' in msg for msg in report.errors)


def test_volt_sdf(tmp_path):
    text = open(os.path.join(_install_dir, 'test_nm.sdf')).read()
    text = text.replace('SESSION_DRX_BEAM 1', 'SESSION_DRX_BEAM 1\nSESSION_MODE     VOLT')
    report = validatesdf.validate(write_sdf(tmp_path, text), now=Time('2024-02-08T00:00:00'))
    assert report.ok, report.errors
    assert report.session_mode_name == '665_VOLT1'


def test_stage_sdf_unique_readable_copy():
    private = tempfile.mkdtemp(dir='/tmp')
    dest_dir = tempfile.mkdtemp(dir='/tmp')
    try:
        src = os.path.join(private, 'crab_12jun26.sdf')
        open(src, 'w').write('SESSION_ID 1\n')
        os.chmod(private, 0o700)

        first = validatesdf.stage_sdf(src, dest_dir=dest_dir)
        second = validatesdf.stage_sdf(src, dest_dir=dest_dir)
        assert os.path.basename(first).startswith('crab_12jun26.sdf.')
        assert first != second
        assert open(first).read() == 'SESSION_ID 1\n'
        assert os.stat(first).st_mode & 0o777 == 0o644
        os.chmod(dest_dir, 0o755)
        assert validatesdf.other_user_access_block(first) is None
    finally:
        shutil.rmtree(private, ignore_errors=True)
        shutil.rmtree(dest_dir, ignore_errors=True)


def test_other_user_cannot_search_home():
    private = tempfile.mkdtemp(dir='/tmp')  # mode 700 under a world-searchable /tmp
    try:
        sdf = os.path.join(private, 'session.sdf')
        open(sdf, 'w').write('x')
        assert validatesdf.other_user_access_block(sdf) == private

        os.chmod(private, 0o755)
        os.chmod(sdf, 0o644)
        assert validatesdf.other_user_access_block(sdf) is None

        os.chmod(sdf, 0o600)
        assert validatesdf.other_user_access_block(sdf) == sdf
    finally:
        os.chmod(private, 0o700)
        shutil.rmtree(private)


def test_missing_file(tmp_path):
    report = validatesdf.validate(str(tmp_path / 'nope.sdf'))
    assert not report.ok
    assert 'does not exist' in report.errors[0]


def test_past_start_buffer_mode():
    report = validatesdf.validate(TEST_SDF, now=Time('2023-08-05T00:54:30', format='isot'))
    assert not report.ok
    assert any('drops sessions' in msg for msg in report.errors)


def test_past_start_asap_mode():
    report = validatesdf.validate(TEST_SDF, mode='asap', now=Time('2023-08-05T00:54:30', format='isot'))
    assert report.ok, report.errors


def test_session_ended():
    report = validatesdf.validate(TEST_SDF, mode='asap', now=Time('2023-08-06T00:00:00', format='isot'))
    assert any('in the past' in msg for msg in report.errors)


def test_ra_in_degrees(tmp_path):
    text = open(TEST_SDF).read().replace('OBS_RA          0.000000000', 'OBS_RA          180.0')
    report = validatesdf.validate(write_sdf(tmp_path, text), now=BEFORE_TEST_SDF)
    assert any('OBS_RA is in hours' in msg for msg in report.errors)


def test_missing_duration(tmp_path):
    text = open(TEST_SDF).read().replace('OBS_DUR         60000\n', '')
    report = validatesdf.validate(write_sdf(tmp_path, text), now=BEFORE_TEST_SDF)
    assert any('OBS_DUR missing' in msg for msg in report.errors)
    assert report.parse_error is not None


def test_empty_value(tmp_path):
    text = open(TEST_SDF).read().replace('OBS_INT_TIME    100', 'OBS_INT_TIME')
    report = validatesdf.validate(write_sdf(tmp_path, text), now=BEFORE_TEST_SDF)
    assert any('OBS_INT_TIME has no value' in msg for msg in report.errors)


@pytest.mark.parametrize('beam, ok', [('3', True), ('17', False), ('x', False)])
def test_beam_number(tmp_path, beam, ok):
    text = open(TEST_SDF).read().replace('SESSION_DRX_BEAM       3', f'SESSION_DRX_BEAM       {beam}')
    report = validatesdf.validate(write_sdf(tmp_path, text), now=BEFORE_TEST_SDF)
    assert report.ok == ok


def test_out_of_order_observations(tmp_path):
    fn = power_sdf(tmp_path, [('2030-01-01T01:00:00', 60000, 30., 20.), ('2030-01-01T00:00:00', 60000, 30., 20.)])
    report = validatesdf.validate(fn, now=Time('2029-12-31T00:00:00'))
    assert any('before the previous observation ends' in msg for msg in report.errors)


def test_back_to_back_observations_warn_late_pointing(tmp_path):
    fn = power_sdf(tmp_path, [('2030-01-01T00:00:00', 60000, 30., 20.), ('2030-01-01T00:01:00', 60000, 30., 20.)])
    report = validatesdf.validate(fn, now=Time('2029-12-31T00:00:00'))
    assert report.ok, report.errors
    assert not any('start_dr' in msg for msg in report.warnings)
    assert any('runs about 10 s late' in msg and 'control_bf' in msg for msg in report.warnings)


def test_spaced_observations_no_late_warning(tmp_path):
    fn = power_sdf(tmp_path, [('2030-01-01T00:00:00', 60000, 30., 20.), ('2030-01-01T00:05:00', 60000, 30., 20.)])
    report = validatesdf.validate(fn, now=Time('2029-12-31T00:00:00'))
    assert report.ok, report.errors
    assert not any('late' in msg for msg in report.warnings)
