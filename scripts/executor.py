#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 09:59:15 2023

@author: idavis
"""

import sys
from time import sleep
from concurrent.futures import ProcessPoolExecutor, wait

from astropy.time import Time
from observing.classes import ObsType
from observing import parsesdf
from mnc import control


# TODO: add callbacks from etcd
"""
def watch_sdf():
    def a(event):
        global sdfset
        if event=='True':
            sdfset = True
        if event=='False':
            sdfset = False
    return a

ls = ds.DsaStore()
docopy = ls.get_dict('/cmd/observing/sdfset') == 'True'
ls.add_watch('/cmd/observing/sdfest', watch_sdf())
"""


def getdf(sdf_fn):
    d = parsesdf.sdf_to_dict(sdf_fn)
    session, obs_list = parsesdf.make_obs_list(d)

    tn = Time.now().mjd
    t0 = obs_list[0].obs_start

    if session.obs_type is ObsType.power:
        df = parsesdf.power_beam_obs(obs_list, session)
    if session.obs_type is ObsType.volt:
        df = parsesdf.volt_beam_obs(obs_list, session)
    if session.obs_type is ObsType.fast:
        df = parsesdf.fast_vis_obs(obs_list, session)
        pass

    return df


def runrow(row):
#    exec(row.command)
    print(row, command)

if __name__ == "__main__":
    """ Run commands parsed from SDF.
    """

    # initialize by setting up etcd callbacks

    futures = []
    with concurrent.futures.ProcessPoolExecutor() as pool:

    # create and update command df from etcd
    # while True
    #    df update and sort
    #
    #    or run as one-off...

        df = getdf(sys.argv[1])
        df.sort_index(inplace=True)

        for (mjd, row) in df.iterrows():
            if Time.now().mjd >= mjd:  # alternatively, make sessions into a sequence with one start time
                _ = pool.submit(runrow, row)
                futures.append(_)
            else:
                sleepmjd = mjd - Time.now().mjd
                print(f"Need to wait {sleepmjd*24*3600} seconds to run next command...")
                sleep(sleepmjd*24*3600)

    wait(futures)
