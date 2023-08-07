from dsautils import dsa_store
from astropy import time
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
            dd[mode] = {session: [times.min(), times.max()]}   # time range per session_mode_name per mode

    return dd


def put_sched(sched):
    """ Takes schedule dataframe and sets schedule in etcd
    """

    sched_dict = create_dict(sched)
    ls.put_dict('/mon/observing/schedule', sched_dict)


def get_sched():
    """ Gets schedule from etcd
    """

    dd = ls.get_dict('/mon/observing/schedule')

    return dd


def print_sched(mode=None):
    """ Gets schedule from etcd and prints it
    """

    dd = get_sched()
    mjd = time.Time.now().mjd

    if mode is None:
        print(f"Schedule (at MJD={mjd})")
        if not len(dd):
            print("\tNothing scheduled")
        else:
            for kk, vv in dd.items():
                print(f"Mode {kk}:")
                for kk2,vv2 in vv.items():
                    print(f"\tSession {kk2} (start, stop): {vv2}")
    else:
        if mode not in dd:
            print(f"Mode {mode} not in schedule.")
        else:
            print(f"Schedule for mode {mode} (at MJD={mjd})")
            for kk2,vv2 in dd[mode].items():
                print(f"\tSession {kk2} (start, stop): {vv2}")
