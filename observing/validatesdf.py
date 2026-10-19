""" Validate an SDF without submitting it to the executor.

Checks are grouped by severity:
 - errors: the executor will reject or drop the session, or the data will be wrong.
 - warnings: the session will be scheduled, but something may be late or unexpected.
"""

import os.path
import re
import traceback

from astropy.time import Time

from observing import parsesdf
from observing.classes import ObsType, EphemModes

BEAM_TYPES = (ObsType.power, ObsType.volt, ObsType.voltraw)
VALID_BEAMS = {ObsType.power: range(1, 17), ObsType.volt: range(1, 2), ObsType.voltraw: range(1, 2)}
MIN_LEAD_TIME_S = 10  # time for the executor to receive and queue a session before its first command
TIME_TOLERANCE_S = 1  # MJD float round-off and sub-second scheduling jitter
SEC_PER_DAY = 24 * 3600


class SdfReport:
    """ Result of validating one SDF.
    """

    def __init__(self, filename, mode='buffer'):
        self.filename = filename
        self.mode = mode
        self.errors = []
        self.warnings = []
        self.session = {}
        self.observations = []
        self.sched = None
        self.parse_error = None

    @property
    def ok(self):
        return not self.errors

    @property
    def session_mode_name(self):
        """ Name used by the executor to key this session in etcd (e.g. "777_POWER3").
        """
        if 'SESSION_ID' not in self.session or 'SESSION_MODE' not in self.session:
            return None
        name = f"{self.session['SESSION_ID']}_{self.session['SESSION_MODE']}"
        if 'SESSION_DRX_BEAM' in self.session:
            name += str(self.session['SESSION_DRX_BEAM'])
        return name

    def error(self, msg):
        self.errors.append(msg)

    def warn(self, msg):
        self.warnings.append(msg)

    def format(self, now=None):
        now = Time.now() if now is None else now
        lines = [f"SDF: {self.filename}"]

        if self.session:
            beam = self.session.get('SESSION_DRX_BEAM')
            beam_str = f", beam {beam}" if beam is not None else ""
            lines.append(f"Session {self.session.get('SESSION_ID')} ({self.session.get('SESSION_MODE')}{beam_str}), "
                         f"name {self.session_mode_name}, {len(self.observations)} observation(s)")
        for obs in self.observations:
            lines.append(f"  Obs {obs.get('id')}: start {obs.get('start_iso')}, duration {obs.get('dur_s')} s, "
                         f"mode {obs.get('mode')}, target {obs.get('target')}")

        if self.sched is not None and len(self.sched):
            t_first = self.sched.index.min()
            t_last = self.sched.index.max()
            lead = (t_first - now.mjd) * SEC_PER_DAY
            lines.append(f"Schedule ({self.mode} mode): {len(self.sched)} commands from "
                         f"{Time(t_first, format='mjd').isot} to {Time(t_last, format='mjd').isot} "
                         f"(first command {_fmt_offset(lead)})")
        elif self.parse_error is not None:
            lines.append(f"Executor parse FAILED: {self.parse_error}")

        lines.append(f"Errors ({len(self.errors)}):")
        lines.extend(f"  - {msg}" for msg in self.errors)
        lines.append(f"Warnings ({len(self.warnings)}):")
        lines.extend(f"  - {msg}" for msg in self.warnings)

        if self.ok:
            lines.append("Result: VALID" + (" (with warnings)" if self.warnings else ""))
        else:
            lines.append("Result: INVALID. The executor will reject or drop this session.")
        return '\n'.join(lines)


def validate(filename, mode='buffer', now=None):
    """ Parse an SDF the same way the executor does and report problems.
    mode is 'buffer' or 'asap', as used by submit-sdf.
    now (astropy Time) defaults to the current time and is used for timing checks.
    Does not contact etcd, so it does not check for conflicts with the current schedule.
    """

    now = Time.now() if now is None else now
    report = SdfReport(filename, mode=mode)

    if not os.path.isfile(filename):
        report.error(f"File {filename} does not exist")
        return report

    try:
        with open(filename) as fp:
            lines = fp.readlines()
    except (OSError, UnicodeDecodeError) as exc:
        report.error(f"Could not read file: {_describe_exception(exc)}")
        return report

    _check_lines(lines, report)

    try:
        dd = parsesdf.sdf_to_dict(filename)
    except Exception as exc:
        report.error(f"Could not split file into keyword/value pairs: {_describe_exception(exc)}")
        return report

    report.session = dict(dd['SESSION'])
    obs_type = _check_session(dd['SESSION'], report)
    _check_observations(dd['OBSERVATIONS'], obs_type, report)

    try:
        sched = parsesdf.make_sched(filename, mode=mode)
    except Exception as exc:
        report.parse_error = _describe_exception(exc)
        report.error(f"Executor could not build a schedule from this SDF: {report.parse_error}")
        return report

    report.sched = sched
    _check_schedule(sched, report, now)
    return report


def _check_lines(lines, report):
    """ Problems visible in the raw text that sdf_to_dict silently accepts.
    """

    seen_session_keys = {}
    for lineno, line in enumerate(lines, start=1):
        words = line.split()
        if not words:
            continue
        key = words[0]
        if len(words) == 1:
            report.error(f"Line {lineno}: keyword {key} has no value")
        if 'OBS' not in key:
            if key in seen_session_keys:
                report.warn(f"Line {lineno}: session keyword {key} repeats line {seen_session_keys[key]}; "
                            "the last value is used")
            seen_session_keys[key] = lineno


def _check_session(session, report):
    """ Validate session keywords. Returns the ObsType, or None if it is invalid.
    """

    if 'SESSION_ID' not in session:
        report.error("Missing SESSION_ID")
    elif _as_int(session['SESSION_ID']) is None:
        report.error(f"SESSION_ID must be an integer (got {session['SESSION_ID']!r})")

    if 'SESSION_MODE' not in session:
        report.error("Missing SESSION_MODE. The schedule would assume VOLT, but the executor needs SESSION_MODE "
                     "to record session metadata")
        mode_value = ObsType.volt.value
    else:
        mode_value = session['SESSION_MODE']

    try:
        obs_type = ObsType(mode_value)
    except ValueError:
        report.error(f"SESSION_MODE {mode_value!r} is not one of {[o.value for o in ObsType]}")
        return None

    if obs_type in BEAM_TYPES:
        beam = _as_int(session.get('SESSION_DRX_BEAM'))
        if 'SESSION_DRX_BEAM' not in session:
            report.error(f"SESSION_DRX_BEAM is required for {obs_type.value} sessions")
        elif beam not in VALID_BEAMS[obs_type]:
            valid = VALID_BEAMS[obs_type]
            report.error(f"SESSION_DRX_BEAM {session['SESSION_DRX_BEAM']!r} is not valid for {obs_type.value} "
                         f"(allowed {valid.start}-{valid.stop - 1})")

        do_cal = session.get('DO_CAL', 'False')
        if do_cal not in ('True', 'False'):
            report.error(f"DO_CAL must be True or False (got {do_cal!r})")

        cal_dir = session.get('CAL_DIR')
        if cal_dir is not None and not os.path.isdir(str(cal_dir)):
            report.error(f"CAL_DIR {cal_dir} does not exist on this host")

    config_file = session.get('CONFIG_FILE')
    if config_file is not None and not os.path.isfile(str(config_file)):
        report.warn(f"CONFIG_FILE {config_file} does not exist on this host")

    return obs_type


def _check_observations(observations, obs_type, report):
    """ Validate each observation block and the ordering between them.
    """

    blocks = [(name, obs) for name, obs in observations.items() if obs]
    if not blocks:
        report.error("No observation blocks found (expected OBS_ID, OBS_START_MJD/MPM, OBS_DUR, OBS_MODE)")
        return

    prev_end = None
    for index, (_, obs) in enumerate(blocks, start=1):
        label = f"Obs {obs.get('OBS_ID', f'#{index}')}"
        summary = {'id': obs.get('OBS_ID', f'#{index}')}

        if _as_int(obs.get('OBS_ID')) is None:
            report.error(f"{label}: OBS_ID missing or not an integer")

        dur = _as_int(obs.get('OBS_DUR'))
        if dur is None:
            report.error(f"{label}: OBS_DUR missing or not an integer number of milliseconds")
        elif dur <= 0:
            report.error(f"{label}: OBS_DUR must be positive (got {dur} ms)")
        summary['dur_s'] = dur / 1e3 if dur is not None else None

        start = _obs_start_mjd(obs, label, report)
        summary['start_iso'] = Time(start, format='mjd').isot if start is not None else None

        mode = obs.get('OBS_MODE')
        valid_modes = [m.value for m in EphemModes]
        if mode is None:
            report.error(f"{label}: OBS_MODE missing (one of {valid_modes})")
        elif mode not in valid_modes:
            report.error(f"{label}: OBS_MODE {mode!r} is not one of {valid_modes}")
        summary['mode'] = mode

        if obs_type in BEAM_TYPES:
            summary['target'] = _check_target(obs, mode, label, report)
            int_time = obs.get('OBS_INT_TIME')
            if int_time is not None:
                it = _as_int(int_time)
                if it is None or not 0 < it <= 1024:
                    report.error(f"{label}: OBS_INT_TIME must be an integer from 1 to 1024 ms (got {int_time!r})")
        else:
            summary['target'] = None

        if obs_type in (ObsType.volt, ObsType.voltraw):
            for key in ('OBS_FREQ1', 'OBS_FREQ2', 'OBS_BW'):
                if _as_int(obs.get(key)) is None:
                    report.error(f"{label}: {key} missing or not an integer (required for {obs_type.value})")

        if start is not None and dur is not None:
            if prev_end is not None and (prev_end - start) * SEC_PER_DAY > TIME_TOLERANCE_S:
                report.error(f"{label}: starts at {summary['start_iso']}, before the previous observation ends")
            prev_end = start + dur / 1e3 / SEC_PER_DAY

        report.observations.append(summary)


def _obs_start_mjd(obs, label, report):
    """ Return start time as MJD using the same precedence as parsesdf.make_obs_list.
    """

    mjd = _as_int(obs.get('OBS_START_MJD'))
    mpm = _as_int(obs.get('OBS_START_MPM'))
    start_from_mjd = mjd + mpm / 1e3 / SEC_PER_DAY if mjd is not None and mpm is not None else None

    start_from_utc = None
    utc = obs.get('OBS_START')
    if isinstance(utc, list):
        # "UTC YYYY MM DD HH:MM:SS" or "UTC YYYY/MM/DD HH:MM:SS"
        words = ' '.join(utc[1:]).replace('/', ' ').split()
        try:
            start_from_utc = Time(f"{words[0]}-{words[1]}-{words[2]} {words[3]}", format='iso').mjd
        except (IndexError, ValueError):
            pass

    if start_from_mjd is None and start_from_utc is None:
        report.error(f"{label}: no valid start time (need OBS_START_MJD and OBS_START_MPM, "
                     f"or OBS_START as 'UTC YYYY MM DD HH:MM:SS'; got OBS_START {utc!r})")
        return None

    if start_from_mjd is not None and start_from_utc is not None:
        diff_s = abs(start_from_mjd - start_from_utc) * SEC_PER_DAY
        if diff_s > 1:
            report.warn(f"{label}: OBS_START_MJD/MPM and OBS_START differ by {diff_s:.1f} s; "
                        "the executor uses OBS_START_MJD/MPM")

    return start_from_mjd if start_from_mjd is not None else start_from_utc


def _check_target(obs, mode, label, report):
    """ Beam observations need a name, RA/Dec, or Az/Alt. Returns a short description.
    """

    name = obs.get('OBS_TARGET')
    name = ' '.join(name) if isinstance(name, list) else name
    ra, dec = _as_float(obs.get('OBS_RA')), _as_float(obs.get('OBS_DEC'))
    az, alt = _as_float(obs.get('OBS_AZ')), _as_float(obs.get('OBS_ALT'))

    for key, val in (('OBS_RA', ra), ('OBS_DEC', dec), ('OBS_AZ', az), ('OBS_ALT', alt)):
        if key in obs and val is None:
            report.error(f"{label}: {key} is not a number (got {obs[key]!r})")

    if ra is not None and not 0 <= ra < 24:
        report.error(f"{label}: OBS_RA {ra} is outside 0-24. OBS_RA is in hours, not degrees")
    if dec is not None and not -90 <= dec <= 90:
        report.error(f"{label}: OBS_DEC {dec} is outside -90 to +90 degrees")

    if mode == EphemModes.azalt.value:
        if az is None or alt is None:
            report.error(f"{label}: AZALT mode requires OBS_AZ and OBS_ALT")
        return f"Az {az}, Alt {alt} deg"
    if mode in (EphemModes.trk_sol.value, EphemModes.trk_jov.value, EphemModes.trk_lun.value):
        return mode
    if ra is not None and dec is not None:
        return f"RA {ra} h, Dec {dec:+} deg"
    if (ra is None) != (dec is None):
        report.error(f"{label}: OBS_RA and OBS_DEC must be given together")
    if name in (None, 'None'):
        report.error(f"{label}: no target. Set OBS_TARGET, OBS_RA/OBS_DEC, or OBS_AZ/OBS_ALT")
    return name


def _check_schedule(sched, report, now):
    """ Check the generated command list for timing problems.
    Commands of one session run sequentially in a single worker, so a blocking command delays all later ones.
    """

    times = list(sched.index)
    commands = list(sched.command)

    for i in range(1, len(times)):
        if times[i] < times[i - 1]:
            report.warn(f"Commands are out of order: {_short(commands[i])} at {Time(times[i], format='mjd').isot} "
                        f"was generated after a later command and will run earlier than intended")
            break

    n_obs = len(report.observations)
    n_rec = sum('start_dr' in cmd for cmd in commands)
    if n_rec < n_obs:
        report.error(f"{n_obs} observations but only {n_rec} recorder command(s); "
                     "commands with identical timestamps overwrote each other")

    order = sorted(range(len(times)), key=lambda i: times[i])
    for pos, i in enumerate(order):
        match = re.search(r"track=True, duration=([0-9.eE+-]+)", commands[i])
        if match is None:
            continue
        blocked_until = times[i] + float(match.group(1)) / SEC_PER_DAY
        for j in order[pos + 1:]:
            if commands[j].startswith('print('):
                continue
            late_s = (blocked_until - times[j]) * SEC_PER_DAY
            if late_s > TIME_TOLERANCE_S:
                report.warn(f"{_short(commands[j])} is scheduled for {Time(times[j], format='mjd').isot} but "
                            f"runs about {late_s:.0f} s late, because beam tracking from the previous command "
                            "blocks the session until it finishes")
                break

    t_first = min(times)
    t_end = max(times)
    lead_s = (t_first - now.mjd) * SEC_PER_DAY
    if (t_end - now.mjd) * SEC_PER_DAY <= 0:
        report.error(f"Session ended at {Time(t_end, format='mjd').isot}, which is in the past")
    elif report.mode == 'buffer':
        if lead_s <= 0:
            report.error(f"First command was due at {Time(t_first, format='mjd').isot} ({_fmt_offset(lead_s)}). "
                         "In buffer mode the executor drops sessions whose first command has passed. "
                         "Use a later start time or --asap")
        elif lead_s < MIN_LEAD_TIME_S:
            report.warn(f"First command is due {_fmt_offset(lead_s)}, which leaves little time for the executor "
                        "to receive and queue it")


def _describe_exception(exc):
    tb = traceback.extract_tb(exc.__traceback__)
    where = f" (at {os.path.basename(tb[-1].filename)}:{tb[-1].lineno} in {tb[-1].name})" if tb else ""
    return f"{type(exc).__name__}: {str(exc) or 'no message'}{where}"


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _short(command, width=60):
    """ Name a command by its controller call (e.g. "con.start_dr(...)"), or truncate it.
    """
    match = re.search(r"con\.\w+\(", command)
    if match is not None:
        return match.group(0) + '...)'
    return command if len(command) <= width else command[:width - 3] + '...'


def _fmt_offset(seconds):
    return f"in {seconds:.0f} s" if seconds >= 0 else f"{-seconds:.0f} s ago"
