import os.path
import click
from dsautils import dsa_store
from observing import schedule
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
    if reset:
        ls.put_dict('/cmd/observing/submitsdf', {'sdffile': None, 'mode': 'reset'})

    mode = 'asap' if asap else 'buffer'
    ls.put_dict('/cmd/observing/submitsdf', {'filename': sdffile, 'mode': mode})


@cli.command()
@click.option('--hard', is_flag=True, default=False, show_default=True)
def reset_schedule(hard):
    """ Reset schedule.
    hard reset will cancel observation currently being observed (experimental).
    """

    ls.put_dict('/cmd/observing/submitsdf', {'sdffile': None, 'mode': 'reset'})
    if hard:
        raise NotImplementedError


@cli.command()
@click.option('--mode', default=None, help='Display only a single observing mode')
def show_schedule(mode):
    """ Print the schedule currently managed by executor.
    """

    schedule.print_sched(mode)


@cli.command()
@click.option('--recorder', default='drvs', help='Name of a recorder (drvs, drvf, dr1, drt1, ...)')
@click.option('--duration', default=None, help='Duration of recording in ms. Default for drvs is to leave it on. Beamformers need duration set.')
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


@cli.command()
def run_calibration():
    """ Run calibration pipeline to generate solutions in pipeline/caltables/latest directory
    """

    pass
