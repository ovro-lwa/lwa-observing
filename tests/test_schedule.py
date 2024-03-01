import pytest
from pandas import DataFrame
from observing.schedule import sched_update

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

def test_sched_update_remove_old_sessions():
    sched1 = DataFrame({'command': ['cmd1', 'cmd2'], 'session_id': [1, 2]}, index=[99991.0, 99992.0])
    sched2 = DataFrame({'command': ['cmd3', 'cmd4'], 'session_id': [3, 4]}, index=[99993.0, 99994.0])
    sched = [sched1, sched2]
    updated_sched = sched_update(sched)
    assert len(updated_sched) == 2
    assert updated_sched.index.tolist() == [99993.0, 99994.0]