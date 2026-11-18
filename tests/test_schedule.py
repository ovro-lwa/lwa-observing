import logging

import pytest
from pandas import DataFrame
from observing import schedule
from observing.schedule import sched_update, sdfdict_keys_to_keep


class _Store:
    def __init__(self):
        self.data = {}

    def get_dict(self, key):
        return self.data.get(key)

    def put_dict(self, key, value):
        self.data[key] = value


@pytest.fixture
def store(monkeypatch):
    fake = _Store()
    monkeypatch.setattr(schedule, "ls", fake)
    return fake

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


def test_put_recorded_keeps_the_five_most_recent_sessions(store):
    for index in range(1, 7):
        schedule.put_recorded(f"{index}_POWER3", 1, [f"/d{index}.dat"])

    names = [session["session_mode_name"] for session in store.data[schedule.RECORDINGS_KEY]["sessions"]]
    assert names == ["6_POWER3", "5_POWER3", "4_POWER3", "3_POWER3", "2_POWER3"]


def test_put_recorded_respects_limit(store):
    for index in range(1, 4):
        schedule.put_recorded(f"{index}_POWER3", 1, [f"/{index}.dat"], limit=2)

    names = [session["session_mode_name"] for session in store.data[schedule.RECORDINGS_KEY]["sessions"]]
    assert names == ["3_POWER3", "2_POWER3"]


def test_put_recorded_updates_an_existing_session(store):
    schedule.put_recorded("777_POWER3", 1, ["/a.dat"])
    schedule.put_recorded("778_VOLT1", 1, ["/b.dat"])
    schedule.put_recorded("777_POWER3", 2, ["/c.dat", "/d.dat"])

    sessions = store.data[schedule.RECORDINGS_KEY]["sessions"]
    assert [session["session_mode_name"] for session in sessions] == ["777_POWER3", "778_VOLT1"]
    assert sessions[0]["observations"] == {"1": ["/a.dat"], "2": ["/c.dat", "/d.dat"]}


def test_put_recorded_logs_an_etcd_failure(monkeypatch, caplog):
    class _Broken:
        def get_dict(self, key):
            raise RuntimeError("etcd down")

    monkeypatch.setattr(schedule, "ls", _Broken())
    with caplog.at_level(logging.WARNING, logger="observing"):
        schedule.put_recorded("777_POWER3", 1, ["/a.dat"])
    assert "etcd down" in caplog.text


def test_print_sched_lists_recorded_sessions(store, caplog):
    store.data[schedule.RECORDINGS_KEY] = {
        "sessions": [
            {"session_mode_name": "778_VOLT1", "observations": {"1": ["/volt.dat"]}},
            {"session_mode_name": "777_POWER3", "observations": {"2": ["/power2.dat"], "1": ["/power1.dat"]}},
        ]
    }
    with caplog.at_level(logging.INFO, logger="observing"):
        schedule.print_sched()

    text = caplog.text
    assert "Last recorded:" in text
    assert text.index("778_VOLT1") < text.index("777_POWER3")
    assert text.index("OBS 1: /power1.dat") < text.index("OBS 2: /power2.dat")


def test_print_sched_filters_recorded_sessions_by_mode(store, caplog):
    store.data[schedule.RECORDINGS_KEY] = {
        "sessions": [
            {"session_mode_name": "778_VOLT1", "observations": {"1": ["/volt.dat"]}},
            {"session_mode_name": "777_POWER3", "observations": {"1": ["/power.dat"]}},
        ]
    }
    with caplog.at_level(logging.INFO, logger="observing"):
        schedule.print_sched("VOLT1")
    assert "/volt.dat" in caplog.text
    assert "/power.dat" not in caplog.text

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="observing"):
        schedule.print_sched("FAST")
    assert "Mode FAST not in recorded history." in caplog.text


def test_sched_update_remove_old_sessions():
    sched1 = DataFrame({'command': ['cmd1', 'cmd2'], 'session_id': [1, 2]}, index=[99991.0, 99992.0])
    sched2 = DataFrame({'command': ['cmd3', 'cmd4'], 'session_id': [3, 4]}, index=[99993.0, 99994.0])
    sched = [sched1, sched2]
    updated_sched = sched_update(sched)
    assert len(updated_sched) == 4
    assert updated_sched.index.tolist() == [99991.0, 99992.0, 99993.0, 99994.0]