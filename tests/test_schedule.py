import pytest
from pandas import DataFrame
from observing.schedule import sched_update, sdfdict_keys_to_keep

def test_sched_update_single_schedule():
    sched = DataFrame({'command': ['cmd1', 'cmd2', 'cmd3'], 'session_id': [1, 2, 3]}, index=[99991.0, 99992.0, 99993.0])
    updated_sched = sched_update(sched)
    assert len(updated_sched) == 3
    assert updated_sched.index.tolist() == [99991.0, 99992.0, 99993.0]

def test_sched_update_multiple_schedules():
    sched1 = DataFrame({'command': ['cmd1', 'cmd2'], 'session_id': [1, 2]}, index=[99991.0, 99992.0])
    sched2 = DataFrame({'command': ['cmd3', 'cmd4'], 'session_id': [3, 4]}, index=[99993.0, 99994.0])
    sched = [sched1, sched2]
    updated_sched = sched_update(sched)
    assert len(updated_sched) == 4
    assert updated_sched.index.tolist() == [99991.0, 99992.0, 99993.0, 99994.0]

def test_sched_update_asap_mode():
    sched1 = DataFrame({'command': ['cmd1', 'cmd2'], 'session_id': [1, 2]}, index=[99991.0, 99992.0])
    sched2 = DataFrame({'command': ['cmd3', 'cmd4'], 'session_id': [3, 4]}, index=[99993.0, 99994.0])
    sched = [sched1, sched2]
    updated_sched = sched_update(sched, mode='asap')
    assert len(updated_sched) == 4
    assert updated_sched.index.tolist() == [99991.0, 99992.0, 99993.0, 99994.0]

def _session(start_mjd, dur_ms=60000):
    return {
        "OBSERVATIONS": {
            "OBSERVATION_1": {
                "OBS_START_MJD": str(int(start_mjd)),
                "OBS_START_MPM": str(int(round((start_mjd % 1) * 86400 * 1000))),
                "OBS_DUR": str(dur_ms),
            }
        }
    }


def test_sdfdict_keeps_future_sessions_past_the_finished_limit():
    now = 61000.5
    sdfdict = {f"{i}_VOLT1": _session(61000.0) for i in range(1, 12)}
    sdfdict["2_VOLT1"] = _session(61010.0)
    keep = sdfdict_keys_to_keep(sdfdict, now, limit=10)
    assert "2_VOLT1" in keep
    assert len(keep) == 11


def test_sched_update_remove_old_sessions():
    sched1 = DataFrame({'command': ['cmd1', 'cmd2'], 'session_id': [1, 2]}, index=[99991.0, 99992.0])
    sched2 = DataFrame({'command': ['cmd3', 'cmd4'], 'session_id': [3, 4]}, index=[99993.0, 99994.0])
    sched = [sched1, sched2]
    updated_sched = sched_update(sched)
    assert len(updated_sched) == 4
    assert updated_sched.index.tolist() == [99991.0, 99992.0, 99993.0, 99994.0]