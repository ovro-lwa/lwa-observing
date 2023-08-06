import os.path
import click
from dsautils import dsa_store

ls = dsa_store.DsaStore()

@click.group('lwaobserving')
def cli():
    pass

@cli.command()
@click.argument('sdffile')
def submit_sdf(sdffile):
    """
    """

    assert os.path.exists(sdffile), f"file {sdffile} not found"
    ls.put_dict('/cmd/observing/sdfname', sdffile)
