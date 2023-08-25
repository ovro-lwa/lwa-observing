import os.path
import click
from dsautils import dsa_store
from observing import schedule
from mnc import control

ls = dsa_store.DsaStore()

@click.group('lwaobserving')
def cli():
    pass

@cli.command()
@click.argument('sdffile')
@click.option('--asap', is_flag=True, default=False, show_default=True)
def submit_sdf(sdffile, asap):
    """ Submit and SDF by providing the full path to the file.
    """

    # TODO: submit to processor key, not one watched directly by executor

    assert os.path.exists(sdffile), f"file {sdffile} not found"
    mode = 'asap' if asap else 'buffer'
    ls.put_dict('/cmd/observing/submitsdf', {'filename': sdffile, 'mode': mode})


@cli.command()
@click.option('--mode', default=None, help='Display only a single observing mode')
def show_schedule(mode):
    """ Print the schedule currently managed by executor.
    """

    schedule.print_sched(mode)


@cli.command()
@click.option('--recorder', default='drvs', help='Name of a recorder (drvs, drvf, dr1, drt1, ...)')
def start_dr(recorder):
    """ Start data recorder directly (no SDF)
    """

    assert recorder == 'drvs', "Only drvs supported currently"
    con = control.Controller()
    con.start_dr(recorder)


@cli.command()
@click.option('--recorder', default='drvs', help='Name of a recorder (drvs, drvf, dr1, drt1, ...)')
def stop_dr(recorder):
    """ Stop data recorder directly (no SDF)
    """

    assert recorder == 'drvs', "Only drvs supported currently"
    con = control.Controller()
    con.stop_dr(recorder)


@cli.command()
def run_calibration():
    """ Run calibration pipeline to generate solutions in pipeline/caltables/latest directory
    """

    pass
