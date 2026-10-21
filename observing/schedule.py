from time import sleep
from pandas import concat, DataFrame
from astropy import time
import logging
from dsautils import dsa_store
from observing import obsstate, parsesdf

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


def _observation_end_mjd(obs):
    """ Return the end MJD of one SDF observation block, or None if it has no start.
    """

    try:
        start = int(obs['OBS_START_MJD']) + int(obs['OBS_START_MPM']) * 1e-3 / 86400.0
        return start + int(obs['OBS_DUR']) * 1e-3 / 86400.0
    except (KeyError, TypeError, ValueError):
        return None


def sdfdict_keys_to_keep(sdfdict, now_mjd, limit=10):
    """ Keys to retain in /mon/observing/sdfdict.

    Sessions that have not finished are always kept. The recorder sidecar is
    written when the voltage recording is commanded, which can be days after
    submit, and it reads this dict. Finished sessions are capped at `limit`
    per mode-beam, preferring the highest session id.
    """

    keep = set()
    finished = []
    for key, entry in sdfdict.items():
        ends = [_observation_end_mjd(obs) for obs in entry.get('OBSERVATIONS', {}).values()]
        ends = [end for end in ends if end is not None]
        if not ends or max(ends) >= now_mjd:
            keep.add(key)
        else:
            finished.append(key)

    by_mode = {}
    for key in finished:
        mode = key.split('_', 1)[1] if '_' in key else key
        by_mode.setdefault(mode, []).append(key)
    for keys in by_mode.values():
        keys.sort(key=lambda key: int(key.split('_', 1)[0]) if key.split('_', 1)[0].isdigit() else 0,
                  reverse=True)
        keep.update(keys[:limit])
    return keep


def put_dict(filename, limit=10):
    """ Parses SDF and puts dict in etcd for later retrieval by data recorders.
    limit is how many finished sessions to retain per mode-beam. Sessions that
    have not ended are not counted against that limit.
    """

    dd = parsesdf.sdf_to_dict(filename)
    session_mode_name = f"{dd['SESSION']['SESSION_ID']}_{dd['SESSION']['SESSION_MODE']}"
    if 'SESSION_DRX_BEAM' in dd['SESSION']:
        session_mode_name += str(dd['SESSION']['SESSION_DRX_BEAM'])

    dd0 = ls.get_dict('/mon/observing/sdfdict') or {}
    keep = sdfdict_keys_to_keep(dd0, time.Time.now().mjd, limit=limit)
    if len(keep) < len(dd0):
        logger.info(f'sdfdict reduced from {len(dd0)} to {len(keep)}.')
    dd0 = {k: v for k, v in dd0.items() if k in keep}
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


def find_conflicts(sched):
    """ Return descriptions of scheduled or submitted sessions that use the same mode/beam during sched
    """

    scheduled, active = get_sched()
    sched_dict = create_dict(sched)

    conflicts = []
    # iterate over sched_dict keys of unique observing modes to get values of (start, stop)
    for kk,vv in sched_dict.items():
        for state, existing in (('scheduled', scheduled), ('active', active)):
            if kk not in existing.keys():
                continue
            # if observing mode is scheduled or active, compare (start, stop)
            for name, trange in existing[kk].items():
                for (t0, t1) in vv.values():
                    if (t0 >= trange[0] and t0 <= trange[1]) or (t1 >= trange[0] and t1 <= trange[1]):
                        conflicts.append(f"{state} session {name} (MJD {trange[0]:.6f} to {trange[1]:.6f})")

    return conflicts


def is_conflicted(sched):
    """ Check if sched is requesting a beam that is already scheduled or submitted
    """

    conflicts = find_conflicts(sched)
    if conflicts:
        logger.warning(f"Conflicts with {', '.join(conflicts)}")
    return bool(conflicts)


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
                        late_s = (time.Time.now().mjd - s0.index[0]) * 24 * 3600
                        logger.warning(f"Removing session {s0.session_mode_name.iloc[0]}: first command at MJD "
                                       f"{s0.index[0]} was due {late_s:.0f} s ago (buffer mode drops started sessions)")
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

        t_start = time.Time.now().mjd
        late_s = (t_start - mjd) * 24 * 3600
        if late_s > 2:
            logger.warning(f"Session {row.session_mode_name}: running command {late_s:.1f} s late: {row.command}")
        else:
            logger.info(f"Submitting command:  {row.command}")

        try:
            exec(row.command)
        except Exception as exc:
            logger.exception(f"Session {row.session_mode_name}: command failed: {row.command}")

        elapsed_s = (time.Time.now().mjd - t_start) * 24 * 3600
        if elapsed_s > 5:
            logger.info(f"Session {row.session_mode_name}: command took {elapsed_s:.1f} s: {row.command}")

    # if loop completes, then set session to completed
    try:
        obsstate.update_session(int(row['session_id']), 'completed')
    except Exception as exc:
        logger.warning("Could not update session status.")

