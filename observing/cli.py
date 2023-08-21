import os.path
import click
from dsautils import dsa_store

ls = dsa_store.DsaStore()

@click.group('lwaobserving')
def cli():
    pass

# lwaobserving command-line tool ideas:
# - start slow vis now
# - make sdf for asap bf or fastvis obs
# - run calibration pipeline?
# Other tool "lwamnc"?
# - print go/no-go status
# - change settings (absorb other scripts)
# - turn off arx power
# - ascii plot of f-eng auto spectra

@cli.command()
@click.argument('sdffile')
#@click.option('--asap', is_flag=True, default=False, show_default=True)
def submit_sdf(sdffile):
    """
    """

    assert os.path.exists(sdffile), f"file {sdffile} not found"
    ls.put_dict('/cmd/observing/sdfname', sdffile)


#def start_dr(recorder='', config='', duration=None):
#    from mnc import control
#    con = control.Controller(config=config, recorder=recorder)
#    con.start_dr(recorder, duration=duration)
