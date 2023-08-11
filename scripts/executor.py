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
from observing import parsesdf, schedule
from dsautils import dsa_store
from mnc import control


def sched_update(sched):
    """ Take a schedule or list of schedules, concatenate and sort them.
    Will either merge input list of scheds or get new sched from etcd set.
    """

    # TODO: remove duplicates

    if isinstance(sched, list):
        sched = concat(sched)
        
    sched.sort_index(inplace=True)
    n_old = sum(sched.index < Time.now().mjd)
    sched = sched[sched.index > Time.now().mjd - 120/(24*3600)]  # submit within last 2 min
    print(f"Updated sched to {len(sched)} sorted submissions (removed {n_old} old commands).")

    return sched


def submit_next(sched, pool):
    """ Waits for mjd and submits the session rows to the pool
    """
    # alternatively, make sessions into a sequence with one start time

    row = sched.iloc[0]
    mjd = row.name
    if mjd - Time.now().mjd < 1/(24*3600):
        rows = sched[sched.session_id == row.session_id]
        fut = pool.submit(runrow, rows)
        sched.drop(index=rows.index, axis=0, inplace=True)
        return fut
    else:
        return None


def runrow(rows):
    """ Runs a command from the schedule
    """

    for mjd, row in rows.iterrows():
        exec(row.command)


ls = dsa_store.DsaStore()
sched0 = DataFrame([])
def sched_callback():
    def a(event):
        global sched0
        if os.path.exists(event):
            sched = parsesdf.make_sched(event, mode='buffer')
            sched0 = sched_update([sched0, sched])
        else:
            print(f"File {event} does not exist. Not updating schedule.")
    return a
ls.add_watch('/cmd/observing/sdfname', sched_callback())


if __name__ == "__main__":
    """ Run commands parsed from SDF.
    """

    pool = ProcessPoolExecutor(max_workers = 1)

    if len(sys.argv) == 2:
        print(f"Initializing schedule with {sys.argv[1]}")
        sched0 = parsesdf.make_sched(sys.argv[1])

    # initialize
    futures = []
    nextmjd = 0
    lsched0 = len(sched0)
    lfutures = len(futures)
    schedule.put_sched(sched0)  # TODO: do we initialize each time or try to save all schedule in etcd?

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

            # clean up futures
#            for fut in futures:
#                if fut.done():
#                    print(f"Completed command: {fut.result()}")
#                elif fut.cancelled():
#                    print(f"Cancelled command: {fut.result()}")
            futures = [fut for fut in futures if not fut.done() or not fut.cancelled()]
            sleep(0.49)  # at least two per second
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
            
        if len(sched0) != lsched0 or len(futures) != lfutures:
            if len(sched0) != lsched0:
                schedule.put_sched(sched0)   # this will remove ones still being observed...
            lsched0 = len(sched0)
            lfutures = len(futures)
            print(f'Change to length of schedule or futures: {len(sched0)}, {len(futures)}')
            if lsched0:
                print('Current schedule:', sched0)
