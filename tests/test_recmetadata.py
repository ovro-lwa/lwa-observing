import json
import os.path

import pytest

from observing import parsesdf, recmetadata

_install_dir = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def sdf_entry():
    fn = os.path.join(_install_dir, "test.sdf")
    return parsesdf.sdf_to_dict(fn)


def test_session_mode_name_from_session(sdf_entry):
    name = recmetadata.session_mode_name_from_session(sdf_entry["SESSION"])
    assert name == "777_POWER3"


def test_build_metadata(sdf_entry):
    metadata = recmetadata.build_metadata(sdf_entry, 1)
    assert metadata["session_mode_name"] == "777_POWER3"
    assert metadata["session"]["SESSION_ID"] == "777"
    assert metadata["observation"]["OBS_ID"] == "1"
    assert "written_at_mjd" in metadata


def test_write_sidecar(tmp_path):
    data_path = tmp_path / "pipeline" / "beam03" / "D1_123_456.dat"
    data_path.parent.mkdir(parents=True)
    data_path.write_bytes(b"data")
    metadata = {"session_mode_name": "777_POWER3", "observation": {"OBS_ID": "1"}}

    sidecar_path = recmetadata.write_sidecar(str(data_path), metadata)

    assert sidecar_path == f"{data_path}.meta.json"
    with open(sidecar_path, encoding="utf-8") as fh:
        loaded = json.load(fh)
    assert loaded == metadata


def test_write_sidecars(tmp_path, monkeypatch):
    data_path = tmp_path / "D1_123.dat"
    recordings = {
        "dr3": {
            "status": "success",
            "path": str(data_path),
            "filename": "D1_123.dat",
            "directory": str(tmp_path),
        }
    }
    sdf_entry = parsesdf.sdf_to_dict(os.path.join(_install_dir, "test.sdf"))
    monkeypatch.setattr(
        recmetadata,
        "metadata_from_etcd",
        lambda session_mode_name, obs_id: recmetadata.build_metadata(sdf_entry, obs_id),
    )

    paths = recmetadata.write_sidecars(recordings, "777_POWER3", 1)

    assert len(paths) == 1
    with open(paths[0], encoding="utf-8") as fh:
        loaded = json.load(fh)
    assert loaded["session_mode_name"] == "777_POWER3"
    assert loaded["recorder"]["name"] == "dr3"


def test_write_sidecars_empty():
    assert recmetadata.write_sidecars({}, "777_POWER3", 1) == []
    assert recmetadata.write_sidecars(None, "777_POWER3", 1) == []


def test_power_schedule_uses_start_dr_and_write_sidecars(sdf_entry):
    session, obs_list = parsesdf.make_obs_list(sdf_entry)
    df = parsesdf.power_beam_obs(obs_list, session)
    record_cmds = [cmd for cmd in df["command"] if "con.start_dr" in cmd]
    assert len(record_cmds) == len(obs_list)
    assert all("write_sidecars(_rec" in cmd for cmd in record_cmds)
    assert all("777_POWER3" in cmd for cmd in record_cmds)


def test_volt_schedule_uses_start_dr_and_write_sidecars():
    fn = os.path.join(_install_dir, "test_nm.sdf")
    d = parsesdf.sdf_to_dict(fn)
    d["SESSION"]["SESSION_MODE"] = "VOLT"
    session, obs_list = parsesdf.make_obs_list(d)
    df = parsesdf.volt_beam_obs(obs_list, session)
    record_cmds = [cmd for cmd in df["command"] if "con.start_dr" in cmd]
    assert len(record_cmds) == len(obs_list)
    assert all("write_sidecars(_rec" in cmd for cmd in record_cmds)
