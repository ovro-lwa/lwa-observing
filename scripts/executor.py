#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 09:59:15 2023

@author: idavis
"""

import os.path
import sys
from time import sleep
from concurrent.futures import ProcessPoolExecutor, wait, as_completed

from pandas import concat, DataFrame
from astropy.time import Time
from observing.parsesdf import make_sched
from dsautils import dsa_store


def sched_update(sched):
    """ Take a schedule or list of schedules, concatenate and sort them.
    Will either merge input list of scheds or get new sched from etcd set.
    """

    # TODO: remove duplicates

    if isinstance(sched, list):
        sched = concat(sched)
        
    sched.sort_index(inplace=True)
    n_old = sum(sched.index < Time.now().mjd)
    sched = sched[sched.index > Time.now().mjd]
    print(f"Updated sched to {len(sched)} sorted submissions (removed {n_old} old commands).")

    return sched


def submit_next(sched, pool):
    """ Waits for mjd and submits the schedule row to the pool
    """
    # alternatively, make sessions into a sequence with one start time

    row = sched.iloc[0]
    mjd = row.name
    if mjd - Time.now().mjd < 1/(24*3600):
        fut = pool.submit(runrow, row)
        sched.drop(index=sched.index[0], axis=0, inplace=True)
        return fut
    else:
        return None


def runrow(row):
    """ Runs a command from the schedule
    """

    exec(row.command)
    return row.command


ls = dsa_store.DsaStore()
sched0 = DataFrame([])
def sched_callback():
    def a(event):
        global sched0
        if os.path.exists(event):
            sched = make_sched(event)
            sched0 = sched_update([sched0, sched])
        else:
            print(f"File {event} does not exist. Not updating schedule.")
    return a
ls.add_watch('/cmd/observing/sdfname', sched_callback())


if __name__ == "__main__":
    """ Run commands parsed from SDF.
    """

    if len(sys.argv) == 2:
        print(f"Initializing schedule with {sys.argv[1]}")
        sched0 = make_sched(sys.argv[1])

    futures = []
    nextmjd = 0
    with ProcessPoolExecutor(max_workers=8) as pool:
        while True:
            try:
                if len(sched0):
                    fut = submit_next(sched0, pool)    # when time comes, fire and forget
                    if fut is not None:
                        futures.append(fut)
                        print(f"Submitted one. {len(futures)} futures")
                    if len(sched0):
                        if sched0.iloc[0].name != nextmjd:
                            nextmjd = sched0.iloc[0].name
                            print(f"Next submission at MJD {nextmjd}, in {(nextmjd-Time.now().mjd)*24*3600}s")
                    else:
                        print("Schedule contains 0 commands.")
            except KeyboardInterrupt:
                print("Interrupting execution of schedule. Waiting on submissions (Ctrl-C again to interrupt)...")
                try:
                    for fut in as_completed(futures):
                        print(f"Completed command: {fut.result()}")
                except KeyboardInterrupt:
                    print(f"Interrupting again. Cancelling {len(futures)} submissions...")
                    for fut in futures:
                        res = fut.cancel()
                        if not res:
                            print("\tCould not cancel a submission...")
                break

            # clean up futures
            for fut in futures:
                if fut.done():
                    print(f"Completed command: {fut.result()}")
                elif fut.cancelled():
                    print(f"Cancelled command: {fut.result()}")
            futures = [fut for fut in futures if fut.done() or fut.cancelled()]
            sleep(0.49)  # at least two per second
