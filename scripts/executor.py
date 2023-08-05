#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 09:59:15 2023

@author: idavis
"""

import sys
from time import sleep
from concurrent.futures import ProcessPoolExecutor, wait, as_completed

from pandas import concat
from astropy.time import Time
from observing.classes import ObsType
from observing import parsesdf
from mnc import control
from dsautils import dsa_store


def make_sched(sdf_fn):
    d = parsesdf.sdf_to_dict(sdf_fn)
    session, obs_list = parsesdf.make_obs_list(d)

    tn = Time.now().mjd
    t0 = obs_list[0].obs_start

    if session.obs_type is ObsType.power:
        sched = parsesdf.power_beam_obs(obs_list, session)
    if session.obs_type is ObsType.volt:
        sched = parsesdf.volt_beam_obs(obs_list, session)
    if session.obs_type is ObsType.fast:
        sched = parsesdf.fast_vis_obs(obs_list, session)
        pass

    print(f"Parsed {sdf_fn} into {len(sched)} submissions.")

    return sched


def sched_update(sched=None):
    """ Take a schedule or list of schedules, concatenate and sort them.
    Will either merge input list of scheds or get new sched from etcd set.
    """

    if isinstance(sched, list):
        print('concat sched list')
        sched = concat(sched)
    elif sched is None:
        fn = ls.get_dict('/cmd/observing/sdfest')
        sched = concat(sched, make_sched(fn))
        
    sched.sort_index(inplace=True)
    sched = sched[sched.index > Time.now().mjd]
    print(f"Updated sched to {len(sched)} sorted submissions.")

    return sched


def submit_next(mjd, row, pool):
    """ Waits for mjd and submits the schedule row to the pool
    """

    if Time.now().mjd < mjd:  # alternatively, make sessions into a sequence with one start time
        sleepmjd = mjd - Time.now().mjd
        print(f"Waiting {sleepmjd*24*3600} seconds to run command ({row.command})...")
        sleep(sleepmjd*24*3600)

    fut = pool.submit(runrow, row)

    return fut


def runrow(row):
    """ Runs a command from the schedule
    """

#    exec(row.command)
    sleep(5)
    return row.command


ls = dsa_store.DsaStore()
ls.add_watch('/cmd/observing/sdfest', sched_update)


if __name__ == "__main__":
    """ Run commands parsed from SDF.
    """

    # TODO: initialize by setting up etcd callbacks
    sched = make_sched(sys.argv[1])
    for fn in sys.argv[2:]:
        sched2 = make_sched(fn)
        sched = sched_update([sched, sched2])

    futures = []
    with ProcessPoolExecutor(max_workers=8) as pool:
        while True:
            try:
                for (mjd, row) in sched.iterrows():   # should always have future dated rows
                    fut = submit_next(mjd, row, pool)    # wait, then fire and forget
                    futures.append(fut)
            except KeyboardInterrupt:
                print("Interrupting execution of schedule. Waiting on submissions (Ctrl-C again to interrupt)...")
                try:
                    for fut in as_completed(futures):
                        print(fut.result())
                except KeyboardInterrupt:
                    print(f"Interrupting again. Cancelling {len(futures)} submissions...")
                    for fut in futures:
                        res = fut.cancel()
                        if not res:
                            print("\tCould not cancel a submission...")
                break

            # clean up futures
            futures = [fut for fut in futures if fut.done() or fut.cancelled()]
            print(f"Processing {len(futures)} submissions.")
            sleep(1)
