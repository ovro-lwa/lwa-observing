from dsautils import dsa_store
from astropy import time
import logging
from dsautils import dsa_store
from pandas import concat, DataFrame

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


def put_sched(sched):
    """ Takes schedule dataframe and sets schedule in etcd
    """

    sched_dict = create_dict(sched)
    ls.put_dict('/mon/observing/schedule', sched_dict)


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
    """ Gets schedule from etcd
    """

    dd = ls.get_dict('/mon/observing/schedule')
    dd2 = ls.get_dict('/mon/observing/submitted')

    # use {} instead of None
    dd = dd if dd is not None else {}
    dd2 = dd2 if dd2 is not None else {}

    return dd, dd2


def check_sched(sched):
    """ Check if sched is requesting a beam that is already scheduled or submitted
    """

    dd, dd2 = get_sched()
    sched_dict = create_dict(sched)

    ok = True
    for kk,vv in sched_dict.items():
        if kk in dd.keys():
            for tr in dd[kk].values():
                _, (t0, t1) = vv.popitem()
                if (t0 > tr[0] and t0 < tr[1]) or (t1 > tr[0] and t1 < tr[1]):
                    ok = False
                    break
        if not ok:
            break
        if kk in dd2.keys():
            for tr in dd2[kk].values():
                _, (t0, t1) = vv.popitem()
                if (t0 > tr[0] and t0 < tr[1]) or (t1 > tr[0] and t1 < tr[1]):
                    ok = False
                    break
    return ok


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

