import os.path
import warnings

try:
    from erfa import ErfaWarning
except ImportError:  # astropy < 5 keeps ERFA in astropy._erfa
    from astropy.utils.exceptions import ErfaWarning

# dsa_syslog stamps every log line with Time.now(). ERFA flags that UTC year
# as dubious when it is more than five years past the last leap second in the
# bundled table (astropy 4.1's table ends in 2016, so 2026 warns on each call).
warnings.filterwarnings(
    'ignore',
    category=ErfaWarning,
    message=r'ERFA function "(dtf2d|d2dtf)" yielded .* "dubious year',
)

import click
from time import sleep
from dsautils import dsa_store
from observing import schedule, makesdf, parsesdf, validatesdf
from mnc import control
import sys
import logging
from astropy.time import Time

logger = logging.getLogger('observing')
ls = dsa_store.DsaStore()


@click.group('lwaobserving')
def cli():
    pass

# lwaobserving command-line tool ideas:
# - run calibration pipeline?
# Other tool "lwamnc"?
# - print go/no-go status

def _check_against_executor(report, ignore_schedule=False):
    """ Add checks that need etcd: conflicts and earlier submissions of the same session.
    """

    name = report.session_mode_name
    try:
        sdfdict = ls.get_dict('/mon/observing/sdfdict') or {}
        if name is not None and name in sdfdict:
            report.warn(f"Session {name} was parsed by the executor before. Was this SDF already submitted?")
        if report.sched is not None and not ignore_schedule:
            for conflict in schedule.find_conflicts(report.sched):
                report.error(f"Conflicts with {conflict}. The executor rejects conflicting sessions")
    except Exception as exc:
        report.warn(f"Could not check the current schedule in etcd ({type(exc).__name__}: {exc})")


def _session_state(name):
    """ Return 'scheduled', 'active', or None for a session_mode_name.
    """

    scheduled, active = schedule.get_sched()
    if any(name in sessions for sessions in active.values()):
        return 'active'
    if any(name in sessions for sessions in scheduled.values()):
        return 'scheduled'
    return None


@cli.command()
@click.argument('sdffile')
@click.option('--asap', is_flag=True, default=False, show_default=True,
              help='Validate timing as for "submit-sdf --asap".')
@click.option('--offline', is_flag=True, default=False, show_default=True,
              help='Skip checks against the executor schedule in etcd.')
def validate_sdf(sdffile, asap, offline):
    """ Parse an SDF and report whether the executor can schedule it.
    Exits with status 1 if the SDF has errors.
    """

    sdffile = os.path.abspath(sdffile)
    report = validatesdf.validate(sdffile, mode='asap' if asap else 'buffer')
    if not offline:
        _check_against_executor(report)

    click.echo(report.format())
    if not report.ok:
        sys.exit(1)


@cli.command()
@click.argument('sdffile')
@click.option('--asap', is_flag=True, default=False, show_default=True)
@click.option('--reset', is_flag=True, default=False, show_default=True)
@click.option('--timeout', default=10., type=float, show_default=True,
              help='Seconds to wait for the executor to confirm the session is scheduled.')
def submit_sdf(sdffile, asap, reset, timeout):
    """ Submit and SDF by providing the full path to the file.
    Flag value asap will submit sdf with commands executed as soon as possible.
    Flag value reset will reset the schedule.
    """

    # TODO: submit to processor key, not one watched directly by executor
    if not os.path.isabs(sdffile):
        sdffile = os.path.abspath(sdffile)
        print(f"Not a full path. Assuming {sdffile}...")

    mode = 'asap' if asap else 'buffer'
    report = validatesdf.validate(sdffile, mode=mode)
    _check_against_executor(report, ignore_schedule=reset)
    print(report.format())
    if not report.ok:
        print(f"Not submitting {sdffile}. Fix the errors above, or run 'lwaobserving validate-sdf' to re-check.")
        sys.exit(1)

    name = report.session_mode_name
    if reset:
        ls.put_dict('/mon/observing/schedule', {})
        ls.put_dict('/mon/observing/submitted', {})
        ls.put_dict('/cmd/observing/submitsdf', {'sdffile': None, 'mode': 'reset'})

    # The executor runs as another account and re-opens this path. Stage a world-readable copy.
    submitted = validatesdf.stage_sdf(sdffile)
    print(f"Copied {sdffile} to {submitted} for the executor.")

    ls.put_dict('/cmd/observing/submitsdf', {'filename': submitted, 'mode': mode})
    print(f"Submitted session {name} to executor in {mode} mode. Waiting up to {timeout:.0f} s for confirmation...")

    # poll until the executor shows the session as scheduled or active
    t_stop = Time.now().unix + timeout
    state = None
    while state is None and Time.now().unix < t_stop:
        sleep(0.5)
        state = _session_state(name)

    if state is not None:
        print(f"Success: session {name} is {state}.")
        return

    sdfdict = ls.get_dict('/mon/observing/sdfdict') or {}
    blocked = validatesdf.other_user_access_block(submitted)
    if name in sdfdict:
        print(f"Failed: the executor accepted session {name} but it is not in the schedule. Most likely its "
              "first command time passed before it was queued, so it was dropped. "
              "Check the executor log for 'Removing session'.")
    elif blocked is not None:
        print(f"Failed: this process parsed session {name}, but the executor never opened {submitted}. "
              f"The executor runs as a different account and cannot search {blocked}.")
    else:
        print(f"Failed: the executor did not acknowledge session {name} within {timeout:.0f} s. "
              f"Check that the executor is running and search its log for '{submitted}'.")
    sys.exit(1)

@cli.command()
@click.argument('sdffile')
@click.option('--n-obs', default=1, type=int,
              help='Number of observations to create. Each obs may only have different target/ra/dec/time.')
@click.option('--sess-mode', default='POWER', type=str, help='Session mode (FAST, SLOW, POWER, VOLT)')
@click.option('--beam-num', default=None, type=int, help='POWER/VOLT beam number')
@click.option('--cal-dir', default='/home/pipeline/caltables/latest', type=str,
              help='Calibration directory with tables to apply if not already calibrated.')
@click.option('--do-cal', is_flag=True, default=False, show_default=True, help='Force the application of calibration')
@click.option('--obs-mode', default='TRK_RADEC', type=str,
              help='Observation mode (e.g. TRK_RADEC, AZALT, TRK_JUPITER, TRK_SOLAR, TRK_LUNAR)')
@click.option('--obs-start', default=None, help='Observation start time (UTC) in YYYY-MM-DDTHH:MM:SS format or "now"')
@click.option('--obs-dur', default=None, type=int, help='Observation duration in milliseconds')
@click.option('--ra', default=None, type=float,
              help='RA of target in degrees (written to the SDF in hours). Interpreted as Azimuth (degrees) for AZALT obs-mode.')
@click.option('--dec', default=None, type=float, help='Dec of target (in degrees). Interpreted as Altitude (degrees) for AZALT obs-mode.')
@click.option('--obj-name', default=None, type=str, help='Name of object to track (used as alternative to RA/Dec)')
@click.option('--int-time', default=None, type=int, help='Integration time in milliseconds')
def create_sdf(sdffile, n_obs, sess_mode, beam_num, cal_dir, do_cal, obs_mode, obs_start, obs_dur, ra, dec, obj_name, int_time):
    """ Create an SDF file.
    """

    makesdf.create(sdffile, n_obs=n_obs, sess_mode=sess_mode, obs_mode=obs_mode, beam_num=beam_num, obs_start=obs_start,
                   obs_dur=obs_dur, ra=ra, dec=dec, obj_name=obj_name, int_time=int_time, do_cal=do_cal, cal_dir=cal_dir)


@cli.command()
@click.argument('command', type=str)
@click.option('--mjd', type=float, default=None)
def submit_command(command, mjd):
    """ Submit a command to be added to schedule at time mjd.
    Command should be python code that can be evaluated, complete with imports.
    E.g., "from mnc import settings; settings.update()" to update settings with latest file.
    Currently command is required to include "settings.update".
    Default time is 'now'.
    """

    if mjd is None:
        mjd = Time.now().mjd + 1/(24*3600)  # give it a little delay
    
    ls.put_dict('/cmd/observing/submitsdf', {'mjd': mjd, 'command': command, 'mode': 'buffer'})


@cli.command()
@click.option('--hard', is_flag=True, default=False, show_default=True)
def reset_schedule(hard):
    """ Reset schedule.
    hard reset will cancel observation currently being observed (experimental).
    """

    ls.put_dict('/cmd/observing/submitsdf', {'filename': None, 'mode': 'reset'})
    if hard:
        raise NotImplementedError


@cli.command()
@click.argument('sdffile')
def cancel_sdf(sdffile):
    """ Use SDF to remove session from schedule
    """

    ls.put_dict('/cmd/observing/submitsdf', {'filename': sdffile, 'mode': 'cancel'})


@cli.command()
@click.option('--mode', default=None, help='Display only a single observing mode')
def show_schedule(mode):
    """ Print the schedule currently managed by executor.
    """

    schedule.print_sched(mode)


@cli.command()
@click.option('--recorder', type=str, default='drvs', help='Name of a recorder (drvs, drvf, dr1, drt1, ...)')
@click.option('--duration', type=float, default=None, help='Duration of recording in ms. Default for drvs is to leave it on. Beamformers need duration set.')
def start_dr(recorder, duration):
    """ Start data recorder directly now (no SDF)
    Currently only supports starting recorder now.
    """

    con = control.Controller()
    con.start_dr(recorder, duration=duration)


@cli.command()
@click.option('--recorder', default='drvs', help='Name of a recorder (drvs, drvf, dr1, drt1, ...)')
def stop_dr(recorder):
    """ Stop data recorder directly (no SDF)
    """

    con = control.Controller(recorders=recorder)
    con.stop_dr(recorder)
