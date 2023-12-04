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
from mnc import control  # inherited by threads
from observing import parsesdf, schedule, obsstate
from dsautils import dsa_store
import logging

logging.basicConfig(level=logging.INFO)  # This configures the root logger
logger = logging.getLogger(__name__)

def sched_update(sched, mode='buffer'):
    """ Take a schedule or list of schedules, concatenate and sort them.
    Will either merge input list of scheds or get new sched from etcd set.
    If mode=='asap', then old session will not be removed.
    """

    # TODO: remove duplicates

    if isinstance(sched, list):
        if mode == 'asap':
            sched = concat(sched)
        else:
            include = [DataFrame([])]
            for s0 in sched:
                if len(s0):
                    if s0.index[0] > Time.now().mjd:
                        include.append(s0)
                    else:
                        logger.warning(f"Removing session starting at {s0.index[0]}")
                        try:
                            obsstate.update_session(s0['session_id'], 'skipped')
                        except Exception as exc:
                            logger.warning(f"Could not update session status: {exc}.")

            sched = concat(include)
        
    sched.sort_index(inplace=True)
    logger.info(f"Updated sched to {len(sched)} commands.")

    return sched


def submit_next(sched, pool):
    """ Waits for mjd and submits the session rows to the pool
    """
    # alternatively, make sessions into a sequence with one start time

    row = sched.iloc[0]
    mjd = row.name
    if mjd - Time.now().mjd < 2/(24*3600):
        rows = sched[sched.session_id == row.session_id]
        fut = pool.submit(runrow, rows)
        schedule.put_submitted(rows)
        try:
            obsstate.update_session(row['session_id'], 'observing')
        except Exception as exc:
            logger.warning("Could not update session status.")
        sched.drop(index=rows.index, axis=0, inplace=True)
        return fut
    else:
        return None


def runrow(rows):
    """ Runs a list of rows for a session_id in the schedule
    """

    for mjd, row in rows.iterrows():
        if mjd - Time.now().mjd > 1/(24*3600):
            logger.info(f"Waiting until MJD {mjd}...")
            while mjd - Time.now().mjd > 1/(24*3600):
                sleep(0.49)
        elif mjd - Time.now().mjd < -10/(24*3600):
            logger.warning(f"Skipping command at MJD {mjd}...")
            continue
        else:
            logger.info("Submitting next command...")

        try:
            exec(row.command)
        except Exception as exc:
            logger.warning(exc)

    # if loop completes, then set session to completed
    try:
        obsstate.update_session(row['session_id'], 'completed')
    except Exception as exc:
        logger.warning("Could not update session status.")



if __name__ == "__main__":
    """ Run commands parsed from SDF.
    """

    pool = ProcessPoolExecutor(max_workers = 8)
    ls = dsa_store.DsaStore()

    logger.info("Set up ProcessPool and DsaStore")

    sched0 = DataFrame([])
    def sched_callback():
        def a(event):
            global sched0
            mode = event['mode']
            if mode == 'reset':
                # option to reset schedule
                logger.info("Resetting schedule...")
                sched0 = DataFrame([])
                sched0 = sched_update(sched0)

            if 'filename' in event:
                filename = event['filename']
                if os.path.exists(filename):
                    # TODO: add mode == 'cancel' to re-parse sdf for session id and removing it from schedule in sched_update
                    sched = parsesdf.make_sched(filename, mode=mode)
                    sched.sort_index(inplace=True)
                    sched0 = sched_update([sched0, sched], mode=mode)

                    # add session to obsstate
                    try:
                        obsstate.add_session(filename)
                        logger.info(f'added session {filename}')
                    except Exception as exc:
                        logger.warning("Could not add session to obsstate.")
                        raise exc
                else:
                    logger.warning(f"File {filename} does not exist.")
            else:
                logger.debug(f"No filename defined.")
        return a
    ls.add_watch('/cmd/observing/submitsdf', sched_callback())

    if len(sys.argv) == 2:
        logger.info(f"Initializing schedule with {sys.argv[1]}")
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
                    logger.info(f"Submitted one. {len(futures)} futures")
                if len(sched0):
                    if sched0.iloc[0].name != nextmjd:
                        nextmjd = sched0.iloc[0].name
                        logger.info(f"Next submission at MJD {nextmjd}, in {(nextmjd-Time.now().mjd)*24*3600}s")
                else:
                    logger.info("Schedule contains 0 commands.")

            # clean up futures
#            for fut in futures:
#                if fut.done():
#                    print(f"Completed command: {fut.result()}")
#                elif fut.cancelled():
#                    print(f"Cancelled command: {fut.result()}")
            futures = [fut for fut in futures if not fut.done() or not fut.cancelled()]
            sleep(0.49)  # at least two per second
        except KeyboardInterrupt:
            logger.info("Interrupting execution of schedule. Clearing schedule and waiting on submissions (Ctrl-C again to interrupt)...")
            schedule.put_sched(DataFrame([]))
            for wid in ls.watch_ids:
                ls.cancel(wid)
            try:
                for fut in as_completed(futures):
                    logger.info(f"Completed command: {fut.result()}")
            except KeyboardInterrupt:
                logger.info(f"Interrupting again. Cancelling {len(futures)} submissions...")
                for fut in futures:
                    res = fut.cancel()
                    if not res:
                        logger.warning("\tCould not cancel a submission...")
            break
            
        if len(sched0) != lsched0 or len(futures) != lfutures:
            if len(sched0) != lsched0:
                schedule.put_sched(sched0)   # this will remove ones still being observed...
            lsched0 = len(sched0)
            lfutures = len(futures)
            logger.info(f'Change to length of schedule or futures: {len(sched0)}, {len(futures)}')
            if lsched0:
                logger.info(f'Current schedule: {sched0}')
