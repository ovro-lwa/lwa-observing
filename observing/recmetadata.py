"""Write recorder sidecar metadata JSON next to data files.

Sidecar paths are taken from the recorder response returned by
``Controller.start_dr``. On the cluster, VOLT recordings land under
``/lustre/ubuntu/beam01``; POWER recordings land under
``/lustre/pipeline/beam[xx]`` (e.g. ``/lustre/pipeline/beam03``).
"""

import json
import logging
import os

from astropy.time import Time
from dsautils import dsa_store

logger = logging.getLogger(__name__)

_SIDECAR_SUFFIX = ".meta.json"


def session_mode_name_from_session(session: dict) -> str:
    """Build the canonical session_mode_name key used in etcd."""
    name = f"{session['SESSION_ID']}_{session['SESSION_MODE']}"
    if "SESSION_DRX_BEAM" in session:
        beam = session["SESSION_DRX_BEAM"]
        if isinstance(beam, list):
            beam = beam[0]
        name += str(beam)
    return name


def find_observation(sdf_entry, obs_id):
    """Return the observation block matching obs_id."""
    for obs in sdf_entry.get("OBSERVATIONS", {}).values():
        if str(obs.get("OBS_ID")) == str(obs_id):
            return _normalize_fields(obs)
    raise KeyError(f"OBS_ID {obs_id} not found in SDF entry")


def build_metadata(sdf_entry, obs_id):
    """Build sidecar metadata from a parsed SDF dictionary."""
    session = _normalize_fields(sdf_entry["SESSION"])
    return {
        "session_mode_name": session_mode_name_from_session(session),
        "session": session,
        "observation": find_observation(sdf_entry, obs_id),
        "written_at_mjd": Time.now().mjd,
    }


def metadata_from_etcd(session_mode_name, obs_id):
    """Load observation metadata from /mon/observing/sdfdict in etcd."""
    ls = dsa_store.DsaStore()
    sdfdict = ls.get_dict("/mon/observing/sdfdict") or {}
    if session_mode_name not in sdfdict:
        raise KeyError(f"{session_mode_name} not found in /mon/observing/sdfdict")
    return build_metadata(sdfdict[session_mode_name], obs_id)


def write_sidecar(data_path: str, metadata: dict) -> str:
    """Write metadata JSON alongside a recorder output file."""
    sidecar_path = f"{data_path}{_SIDECAR_SUFFIX}"
    directory = os.path.dirname(sidecar_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(sidecar_path, "w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2, sort_keys=True)
        fh.write("\n")
    logger.info("Wrote recorder sidecar metadata to %s", sidecar_path)
    return sidecar_path


def write_sidecar_for_recording(
    data_path: str,
    session_mode_name: str,
    obs_id,
    recorder=None,
):
    """Build metadata from etcd and write a sidecar for one recording."""
    metadata = metadata_from_etcd(session_mode_name, obs_id)
    metadata["recorder"] = recorder_fields(data_path, recorder)
    return write_sidecar(data_path, metadata)


def write_sidecar_from_record_response(
    response,
    metadata,
    recorder=None,
):
    """Attach recorder fields and write a sidecar from a record command response."""
    data_path = data_path_from_response(response)
    payload = dict(metadata)
    payload["recorder"] = recorder_fields(data_path, recorder)
    return write_sidecar(data_path, payload)


def write_sidecars(
    recordings,
    session_mode_name,
    obs_id,
):
    """Write sidecars for all recordings returned by Controller.start_dr."""
    if not recordings:
        logger.warning(
            "No recordings to write sidecars for %s OBS_ID %s",
            session_mode_name,
            obs_id,
        )
        return []

    sidecar_paths = []
    for recorder, info in recordings.items():
        path = info.get("path") if isinstance(info, dict) else None
        if not path:
            continue
        try:
            sidecar_paths.append(
                write_sidecar_for_recording(path, session_mode_name, obs_id, recorder)
            )
        except Exception as exc:
            logger.warning(
                "Failed to write sidecar for %s OBS_ID %s recorder %s: %s",
                session_mode_name,
                obs_id,
                recorder,
                exc,
            )
    return sidecar_paths


def data_path_from_response(response: dict) -> str:
    filename = response["response"]["filename"]
    if os.path.isabs(filename):
        return filename
    directory = response["response"].get("directory")
    if directory:
        return os.path.join(directory, filename)
    return filename


def recorder_fields(data_path, recorder=None):
    return {
        "name": recorder,
        "path": data_path,
        "filename": os.path.basename(data_path),
        "directory": os.path.dirname(data_path) or ".",
    }


def _normalize_fields(fields):
    normalized = {}
    for key, value in fields.items():
        if isinstance(value, list):
            normalized[key] = value[0] if len(value) == 1 else value
        else:
            normalized[key] = value
    return normalized
