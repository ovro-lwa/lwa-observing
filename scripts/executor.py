#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 09:59:15 2023

@author: idavis
"""

import sys
from time import sleep
from concurrent.futures import ProcessPoolExecutor, wait, as_completed

from pandas import concat, DataFrame
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


def sched_update(sched):
    """ Take a schedule or list of schedules, concatenate and sort them.
    Will either merge input list of scheds or get new sched from etcd set.
    """

    if isinstance(sched, list):
        print('concat sched list')
        sched = concat(sched)
        
    sched.sort_index(inplace=True)
    sched = sched[sched.index > Time.now().mjd]
    print(f"Updated sched to {len(sched)} sorted submissions.")

    return sched


def submit_next(sched, pool):
    """ Waits for mjd and submits the schedule row to the pool
    """
    # alternatively, make sessions into a sequence with one start time

    row = sched.iloc[0]
    mjd = row.name
    if Time.now().mjd - mjd < 1/(24*3600):
        fut = pool.submit(runrow, row)
        return fut
#        sleepmjd = mjd - Time.now().mjd
#        print(f"Waiting {sleepmjd*24*3600} seconds to run command ({row.command})...")
#        sleep(sleepmjd*24*3600)


def runrow(row):
    """ Runs a command from the schedule
    """

#    exec(row.command)
    sleep(5)
    return row.command


ls = dsa_store.DsaStore()
sched0 = DataFrame([])
def sched_callback():
    def a(event):
        global sched0
        sched = make_sched(event)
        sched0 = sched_update([sched0, sched])
    return a
ls.add_watch('/cmd/observing/sdfname', sched_callback())


if __name__ == "__main__":
    """ Run commands parsed from SDF.
    """

    if len(sys.argv) > 2:
        sched0 = make_sched(sys.argv[1])

    futures = []
    nextmjd = 0
    with ProcessPoolExecutor(max_workers=8) as pool:
        while True:
            try:
                if len(sched0):
                    fut = submit_next(sched0, pool)    # fire and forget
                    futures.append(fut)
                    if sched0.iloc[0].name > nextmjd:
                        nextmjd = sched0.iloc[0].name
                        print(f"Next submission at MJD {nextmjd}, in {(nextmjd-Time.now().mjd)*24*3600}s")
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
            sleep(0.49)  # at least two per second
