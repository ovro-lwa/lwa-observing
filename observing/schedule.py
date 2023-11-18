from dsautils import dsa_store
from astropy import time
import sys
import logging
from dsautils import dsa_store

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
            times = sched[sched.session_mode_name == ss].index    
            dd[mode] = {ss: [times.min(), times.max()]}   # time range per session_mode_name per mode

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


