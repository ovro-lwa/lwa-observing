import os.path
import click
from time import sleep
from dsautils import dsa_store
from observing import schedule, makesdf, parsesdf
from mnc import control
import sys
import logging

logger = logging.getLogger('observing')
ls = dsa_store.DsaStore()

@click.group('lwaobserving')
def cli():
    pass

# lwaobserving command-line tool ideas:
# - run calibration pipeline?
# Other tool "lwamnc"?
# - print go/no-go status

@cli.command()
@click.argument('sdffile')
@click.option('--asap', is_flag=True, default=False, show_default=True)
@click.option('--reset', is_flag=True, default=False, show_default=True)
def submit_sdf(sdffile, asap, reset):
    """ Submit and SDF by providing the full path to the file.
    Flag value asap will submit sdf with commands executed as soon as possible.
    Flag value reset will reset the schedule.
    """

    # TODO: submit to processor key, not one watched directly by executor
    if not os.path.isabs(sdffile):
        sdffile = os.path.abspath(sdffile)
        print(f"Not a full path. Assuming {sdffile}...")

    assert os.path.exists(sdffile), f"File {sdffile} not found"

    # try parsing SDF
    try:
        dd = parsesdf.sdf_to_dict(sdffile)
        session_mode_name = f"{dd['SESSION']['SESSION_ID']}_{dd['SESSION']['SESSION_MODE']}"
        if 'SESSION_DRX_BEAM' in dd['SESSION']:
            session_mode_name += dd['SESSION']['SESSION_DRX_BEAM']
        if session_mode_name in ls.get_dict('/mon/observing/sdfdict'):
            print(f"Warning: SDF {sdffile} was parsed by scheduler before. Was this SDF already submitted?")

        sched = parsesdf.make_sched(sdffile)
        if schedule.is_conflicted(sched) and not asap:
            print(f"Warning: SDF {sdffile} is conflicted with current schedule")
            return
    except:
        print(f"Warning: SDF {sdffile} could not be parsed into scheduling commands.")
        return

    if reset:
        ls.put_dict('/mon/observing/schedule', {})
        ls.put_dict('/mon/observing/submitted', {})
        ls.put_dict('/cmd/observing/submitsdf', {'sdffile': None, 'mode': 'reset'})

    mode = 'asap' if asap else 'buffer'
    ls.put_dict('/cmd/observing/submitsdf', {'filename': sdffile, 'mode': mode})

    # wait, then see if it got parsed and scheduled
    sleep(1)
    sdfdict = ls.get_dict('/mon/observing/sdfdict')
    if session_mode_name not in sdfdict:
        print(f"SDF failed to get parsed by scheduler (session mode name: {session_mode_name})")
        return

    scheduled = ls.get_dict('/mon/observing/schedule')
    active = ls.get_dict('/mon/observing/submitted')
    any_scheduled = any([key for key in scheduled if session_mode_name in scheduled[key]])
    any_active = any([key for key in active if session_mode_name in active[key]])
    if not any_scheduled and not any_active:
        print(f"Session {session_mode_name} not scheduled or actively observing")
    else:
        print("Successfullly submitted SDF to schedule.")

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
@click.option('--ra', default=None, type=float, help='RA of target (in hours). Interpreted as Azimuth for AZALT obs-mode.')
@click.option('--dec', default=None, type=float, help='Dec of target (in degrees). Interpreted as Azimuth for AZALT obs-mode.')
@click.option('--obj-name', default=None, type=str, help='Name of object to track (used as alternative to RA/Dec)')
@click.option('--int-time', default=None, type=int, help='Integration time in milliseconds')
def create_sdf(sdffile, n_obs, sess_mode, beam_num, cal_dir, do_cal, obs_mode, obs_start, obs_dur, ra, dec, obj_name, int_time):
    """ Create an SDF file.
    """

    makesdf.create(sdffile, n_obs=n_obs, sess_mode=sess_mode, obs_mode=obs_mode, beam_num=beam_num, obs_start=obs_start,
                   obs_dur=obs_dur, ra=ra, dec=dec, obj_name=obj_name, int_time=int_time, do_cal=do_cal, cal_dir=cal_dir)


@cli.command()
@click.argument('mjd', type=float)
@click.argument('command', type=str)
def submit_command(mjd, command):
    """ Submit a command to be added to schedule at time mjd.
    Command should be python code that can be evaluated, complete with imports.
    E.g., "from mnc import settings; settings.update()" to update settings with latest file.
    Currently command is required to include "settings.update".
    """

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
