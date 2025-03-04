#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 09:59:15 2023

@author: idavis
"""

import os.path
import sys
from time import sleep

import multiprocessing as mp

from pandas import DataFrame
from astropy.time import Time
from mnc import common  # inherited by threads
from observing import parsesdf, schedule, obsstate
from dsautils import dsa_store

logger = common.get_logger(__name__)
#logging.basicConfig(level=logging.INFO)  # This configures the root logger
#logger = logging.getLogger(__name__)


if __name__ == "__main__":
    """ Run commands parsed from SDF.
    """

    ctx = mp.get_context('spawn')
    pool = ctx.Pool(processes=8, maxtasksperchild=1)
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
                sched0 = schedule.sched_update(sched0)
            elif 'filename' in event and mode == 'cancel':
                # option to cancel session
                filename = event['filename']
                if os.path.exists(filename):
                    logger.info(f"Cancelling session {filename}")
                    sched = parsesdf.make_sched(filename)
                    sched0 = sched0[sched0.session_id != sched.session_id.iloc[0]]
                    # remove session from obsstate
                    try:
                        obsstate.update_session(int(sched.session_id.iloc[0]), 'cancelled')
                    except Exception as exc:
                        logger.warning("Could not update session status.")
            elif 'filename' in event and mode in ['asap', 'buffer']:
                # option to submit session with option to execute asap
                filename = event['filename']
                if os.path.exists(filename):
                    logger.info(f"Checking session in {filename}")
                    sched = parsesdf.make_sched(filename, mode=mode)
                    sched.sort_index(inplace=True)

                    if not schedule.is_conflicted(sched):
                        logger.info(f"Adding session {filename}")
                        # add session to obsstate
                        try:
                            obsstate.add_session(filename)
                        except Exception as exc:
                            logger.warning("Could not add session to obsstate.")

                        sched0 = schedule.sched_update([sched0, sched], mode=mode)

                        # make function to parse and add dictionary there, keyed by session_id
                        schedule.put_dict(filename)

                    else:
                        # should probably log this better or return to cli user
                        logger.warning(f"Session {filename} conflicts with existing session.")
                else:
                    logger.warning(f"File {filename} does not exist.")
            elif 'filename' not in event and 'command' in event and 'mjd' in event:
                # option to submit single command
                command = event['command']
                mjd = event['mjd']
                mode = event['mode']

                sched = parsesdf.make_command(mjd, command)
                if sched is None:
                    logger.warning(f"Command ({command}) not allowed.")
                else:
                    # get arbitrary unique session_id and add as column (to avoid submitting multiple commands at once)
                    if len(sched0):
                        settings_id = int(max(set(list(sched0.session_id)))) + 1  # "settings" is a misnomer since this can include x-engine restart too
                    else:
                        settings_id = 1

                    # handy name 
                    sched.insert(1, column='session_id', value=int(settings_id))
                    session_mode_name = f"{settings_id}_settings"
                    sched.insert(1, column='session_mode_name', value=session_mode_name)

                    if not schedule.is_conflicted(sched):
                        logger.info(f"Adding command {command} at MJD {mjd}")
                        sched0 = schedule.sched_update([sched0, sched], mode=mode)
                    else:
                        logger.warning(f"Command {command} conflicts with existing command.")
            else:
                logger.debug(f"No filename defined.")
        return a
    ls.add_watch('/cmd/observing/submitsdf', sched_callback())   # TODO: generalize key name to "submit"?

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
                fut = schedule.submit_next(sched0, pool)    # when time comes, fire and forget
                if fut is not None:
                    futures.append(fut)
                    logger.info(f"Submitted one. {len(futures)} futures")
                if len(sched0):
                    if sched0.iloc[0].name != nextmjd:
                        nextmjd = sched0.iloc[0].name
                        logger.info(f"Next session at MJD {nextmjd}, in {(nextmjd-Time.now().mjd)*24*3600}s")
                else:
                    logger.info("Schedule contains 0 session commands.")

            # clean up futures
            for fut in futures:
                if fut.ready():
                    logger.info(f"Completed command: {fut.get(timeout=1)}")
                    futures.remove(fut)
            sleep(0.49)  # at least two per second
        except KeyboardInterrupt:
            logger.info("Interrupting execution of schedule. Clearing schedule and waiting on submissions (Ctrl-C again to interrupt)...")
            schedule.put_sched(DataFrame([]))
            for wid in ls.watch_ids:
                ls.cancel(wid)
            try:
                for fut in futures:
                    logger.info(f"Completed command: {fut.get(timeout=1)}")
            except KeyboardInterrupt:
                logger.info(f"Interrupting again. Cancelling {len(futures)} submissions...")
# not available in spawned processes
#                for fut in futures:
#                    res = fut.cancel()
#                    if not res:
#                        logger.warning("\tCould not cancel a submission...")
            pool.terminate()
            break
            
        if len(sched0) != lsched0 or len(futures) != lfutures:
            if len(sched0) != lsched0:
                schedule.put_sched(sched0)   # this will remove ones still being observed...
            lsched0 = len(sched0)
            lfutures = len(futures)
            logger.info(f'Change to length of schedule or futures: {len(sched0)}, {len(futures)}')
            if lsched0:
                logger.info(f'Current schedule: {sched0}')
