from time import sleep
from pandas import concat, DataFrame
from astropy import time
import logging
from dsautils import dsa_store
from observing import obsstate

logger = logging.getLogger('observing')
ls = dsa_store.DsaStore()


def create_dict(sched):
    """ Use schedule to create dict to load to etcd
    """

    sched.sort_index(inplace=True)
    dd = {}
    if len(sched.columns) > 1:
        for ss in set(sched.session_mode_name):
            session, mode = ss.split("_")
            if mode not in dd:
                dd[mode] = {}
            times = sched[sched.session_mode_name == ss].index    
            dd[mode][ss] = [times.min(), times.max()]   # time range per session_mode_name per mode

    return dd


def put_sched(sched=None):
    """ Takes schedule dataframe and sets schedule in etcd
    If no schedule provided, etcd key is reset.
    """
    
    if sched is not None:
        sched_dict = create_dict(sched)
    else:
        logger.info("Resetting submitted/scheduled info in etcd")
        sched_dict = {}
        ls.put_dict('/mon/observing/submitted', {})
    ls.put_dict('/mon/observing/schedule', sched_dict)


def put_dict(filename):
    """ Parses SDF and puts dict in etcd for later retrieval by data recorders
    """

    dd = sdf_to_dict(filename)
    session_mode_name = f'{dd['SESSION']['SESSION_ID']}_{dd['SESSION']['SESSION_MODE']}'
    if 'SESSION_DRX_BEAM' in dd['SESSION']:
        session_mode_name += dd['SESSION']['SESSION_DRX_BEAM']

    dd0 = ls.get_dict('/mon/observing/sdfdict')
    dd0.update({session_mode_name: dd})
    ls.put_dict('/mon/observing/sdfdict', dd0)

                
def put_submitted(rows):
    """ Takes submitted rows and sets values in etcd
    """

    dd = {}
    times = rows.index
    session_mode_name = rows.iloc[0].session_mode_name
    mode = session_mode_name.split('_')[1]
    dd[mode] = {session_mode_name: [times.min(), times.max()]}   # time range per session_mode_name per mode

    ls.put_dict('/mon/observing/submitted', dd)


def get_sched():
    """ Gets scheduled and active observations from etcd
    """

    scheduled = ls.get_dict('/mon/observing/schedule')
    active = ls.get_dict('/mon/observing/submitted')

    # use {} instead of None
    scheduled = scheduled if scheduled is not None else {}
    active = active if active is not None else {}

    return scheduled, active


def is_conflicted(sched):
    """ Check if sched is requesting a beam that is already scheduled or submitted
    """

    scheduled, active = get_sched()
    sched_dict = create_dict(sched)

    # iterate over sched_dict keys of unique observing modes to get values of (start, stop)
    for kk,vv in sched_dict.items():

        # if observing mode is scheduled, compare (start, stop)
        if kk in scheduled.keys():
            for trange in scheduled[kk].values():
                for (t0, t1) in vv.values():
                    if (t0 >= trange[0] and t0 <= trange[1]) or (t1 >= trange[0] and t1 <= trange[1]):
                        return True

        # if observing mode is active, compare (start, stop)
        if kk in active.keys():
            for trange in active[kk].values():
                for (t0, t1) in vv.values():
                    if (t0 >= trange[0] and t0 <= trange[1]) or (t1 >= trange[0] and t1 <= trange[1]):
                        return True

    return False


def print_sched(mode=None):
    """ Gets schedule from etcd and prints it
    """

    dd, dd2 = get_sched()
    mjd = time.Time.now().mjd

    logger.info(f"***Schedule (at MJD={mjd})***")
    if mode is None:
        if not len(dd):
            logger.info("Nothing scheduled")
        else:
            for kk, vv in dd.items():
                logger.info(f"\tMode {kk}:")
                for kk2,vv2 in vv.items():
                    logger.info(f"\tSession: {kk2}. Start, Stop: {vv2}")
                logger.info('\n')
        logger.info('\n')
        if len(dd2):
           logger.info("Last submission:")
           for kk, vv in dd2.items():
                logger.info(f"\tMode {kk}:")
                for kk2,vv2 in vv.items():
                    logger.info(f"\t\tSession: {kk2}. Start, Stop: {vv2}")
                logger.info('\n')
        logger.info('\n')
    else:
        if mode not in dd:
            logger.info(f"Mode {mode} not in schedule.")
        else:
            logger.info(f"Schedule for mode {mode} (at MJD={mjd})")
            for kk2,vv2 in dd[mode].items():
                logger.info(f"\tSession: {kk2}. Start, Stop: {vv2}")
            logger.info('\n')

        if mode not in dd2:
            logger.info(f"Mode {mode} not in submitted history.")
        else:
            for kk2,vv2 in dd2[mode].items():
                logger.info(f"\tSession: {kk2}. Start, Stop: {vv2}")


def sched_update(sched, mode='buffer'):
    """ Take a schedule or list of schedules, concatenate and sort them.
    Will either merge input list of scheds or get new sched from etcd set.
    If mode=='asap', then old session will not be removed.
    """

    if isinstance(sched, list):
        if mode == 'asap':
            sched = concat(sched)
        else:
            include = [DataFrame([])]
            for s0 in sched:
                if len(s0):
                    if s0.index[0] > time.Time.now().mjd:
                        include.append(s0)
                    else:
                        logger.warning(f"Removing session starting at {s0.index[0]}")
                        try:
                            obsstate.update_session(int(s0.session_id.iloc[0]), 'skipped')
                        except Exception as exc:
                            logger.warning(f"Could not update session status: {str(exc)}.")

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
    if mjd - time.Time.now().mjd < 2/(24*3600):
        rows = sched[sched.session_id == row.session_id]
        print(rows)
        fut = pool.apply_async(func=runrow, args=(rows,))
        put_submitted(rows)
        try:
            obsstate.update_session(int(row['session_id']), 'observing')
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
        if mjd - time.Time.now().mjd > 1/(24*3600):
            logger.info(f"Waiting until MJD {mjd}...")
            while mjd - time.Time.now().mjd > 1/(24*3600):
                sleep(0.49)
# no longer skipping late rows
#        elif mjd - time.Time.now().mjd < -10/(24*3600):
#            logger.warning(f"Skipping command at MJD {mjd}...")
#            continue
        else:
            logger.info(f"Submitting command:  {row.command}")

        try:
            exec(row.command)
        except Exception as exc:
            logger.warning(exc)

    # if loop completes, then set session to completed
    try:
        obsstate.update_session(int(row['session_id']), 'completed')
    except Exception as exc:
        logger.warning("Could not update session status.")

