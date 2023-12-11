import pandas as pd
import numpy as np
from astropy.time import Time
from datetime import timedelta
from observing.classes import ObsType, EphemModes
import random
import os
import logging

logger = logging.getLogger('observing')


def create(out_name, sess_id=None, sess_mode=None, beam_num=None, cal_dir='/home/pipeline/caltables/latest', pi_id=None,
           pi_name=None, config_file=None, n_obs=1, obs_mode=None, obs_start=None, obs_dur=None, ra=None, dec=None,
           obj_name=None, int_time=None):
    """ Create a file out_name as an SDF
    """

    sdf_text = ''

    if sess_id is None:
        sess_id = random.randint(0, 1000)
        print(f"No Session ID provided. Setting random Session ID of {sess_id}")

    if sess_mode is None:
        sess_mode = input(f"Enter a session mode of {ObsType.__name__}")

    sess_mode = ObsType(sess_mode)
    
    if sess_mode.name not in ["FAST", "SLOW"]:
        if beam_num is None:
            inp = input("Provide a beam number:")
            beam_num = int(inp)
        if cal_dir is not None:
            assert os.path.exists(cal_dir), f"cal_dir ({cal_dir}) does not exist"
    else:
        beam_num = None
        cal_dir = None

    if pi_id is None:
        pi_id = random.randint(0, 1000)
        print(f"No PI ID provided. Setting random PI ID of {pi_id}")

    if pi_name is None:
        try:
            pi_name = os.environ["USER"]
        except:
            pi_name = "Observer"
            print("No PI Name provided. Setting the PI Name to Observer")

    if config_file is None:
        print("No configuration file specified. Assuming the standard path.")
        config_file = "/home/pipeline/proj/lwa-shell/mnc_python/config/lwa_config_calim.yaml"

    try:
        session_preamble = make_session_preamble(sess_id, sess_mode, pi_id, pi_name, beam_num, config_file, cal_dir)
        sdf_text += session_preamble
        print(session_preamble)
    except:
        raise Exception("Couldn't make the session preamble")
    
    obs_text = make_oneobs(1, sess_mode=sess_mode, obs_mode=obs_mode, obs_start=obs_start, obs_dur=obs_dur, ra=ra, dec=dec, obj_name=obj_name,
                           int_time=int_time)
    sdf_text += obs_text

    # after first, all others will prompt for input
    for obs_count in range(1, n_obs):
        obs_text = make_oneobs(obs_count, sess_mode=sess_mode)
        sdf_text += obs_text
        print(obs_text)
        print("Add another observation? (Y/N)")

    if os.path.exists(out_name):
        print(f"WARNING: {out_name} already exists. Overwriting.")
    else:
        print(f"Writing out to {out_name}")

    with open(out_name,'w') as f:
        f.write(sdf_text)



def make_oneobs(obs_count, sess_mode=None, obs_mode=None, obs_start=None, obs_dur=None, ra=None, dec=None, obj_name=None, int_time=None):
    """ Create string for one observation
    """

    print(f"Making observation {obs_count}")
    if obs_mode is None:
        if sess_mode.name in ['POWER', 'VOLT']:
            obs_mode = EphemModes('TRK_RADEC')
            logger.info("no obs_mode provided, assuming TRK_RADEC")
    elif obs_mode in ['TRK_JOV', 'TRK_SOL', 'TRK_LUN']:
        obs_mode = EphemModes(obs_mode)
        obj_name = obs_mode.name.lstrip('TRK_')
        ra = 0.
        dec = 0.

    if sess_mode.name in ['POWER', 'VOLT']:
        if ra is None and dec is None and obj_name is None:
            coords = input("Give target as RA DEC, in degrees (comma delimited) or a single object name (no commas):")
            try:
                objectspl = coords.split(',')
                if len(objectspl) == 2:
                    ra = float(ra)
                    dec = float(dec)
                elif len(objectspl) == 1:
                    obj_name = objectspl[0]
            except:
                raise ValueError("Couldn't parse coords")

    if obs_start is None:
        obs_start = input(f"Give the start time of the observation in isot format or as astropy.Time object")

    if isinstance(obs_start, Time):
        obs_start = obs_start.isot
    elif isinstance(obs_start, str):
        if obs_start.lower() == 'now':
            obs_start = Time.now().isot
        else:
            try:
                obs_start = Time(obs_start, format='isot').isot
            except:
                raise ValueError("Couldn't parse obs_start")

    if obs_dur is None:
        obs_dur = int(input(f"Give the duration of the observation in milliseconds:"))

    if int_time is None and sess_mode.name in ['POWER', 'VOLT']: 
        print(f"Give the integrations time of the observation in milliseconds")
        int_time = int(input())

    obs_text = make_obs_block(obs_count, obs_start, obs_dur, ra, dec, obj_name, int_time, obs_mode)
    return obs_text


def make_session_preamble(session_id, session_mode, pi_id = 0, pi_name:str = 'Observer', beam_num = None,
                          config_dir = '/home/pipeline/proj/lwa-shell/mnc_python/config/lwa_calim_config.yaml', cal_dir = None):
    """ Create preamble info required for a proper SDF
    """

    lines = 'PI_ID            {:02d}\n'.format(pi_id)
    lines += f'PI_NAME          {pi_name}\n\n'
    lines += 'PROJECT_ID       0\n'
    lines += f'SESSION_ID       {session_id}\n'
    lines += f'SESSION_MODE     {session_mode.name}\n'
    if beam_num != None:
        lines += f'SESSION_DRX_BEAM       {beam_num}\n'
    lines += f'CONFIG_FILE      {config_dir}\n'
    if cal_dir != None:
        lines += f'CAL_DIR          {cal_dir}\n'
    lines += '\n'

    return lines


def make_obs_block(obs_id, start_time:str, duration, ra = None, dec = None, obj_name = None, integration_time = 1, obs_mode = None):
    """ Create an observation block for the SDF
    """

    t = Time(start_time, format = 'isot')
    midnight = Time(int(t.mjd), format = 'mjd')
    mjd_start = int(midnight.value)
    mpm_dt = t - midnight
    mpm = int(mpm_dt.sec * 1e3)
    duration_lf = str(timedelta(milliseconds = duration))
    duration_arr = duration_lf.split(':')
    if len(duration_arr[0]) == 1:
        duration_lf = '0' + duration_lf

    lines =  f'OBS_ID          {obs_id}\n'
    if obj_name != None:
        lines += f'OBS_TARGET      {obj_name}\n'
    lines += f'OBS_START_MJD   {mjd_start}\n'
    lines += f'OBS_START_MPM   {mpm}\n'
    lines += f"OBS_START       UTC {start_time.replace('-',' ').replace('T',' ')}\n"
    lines += f"OBS_DUR         {duration}\n"
    lines += f"OBS_INT_TIME    {integration_time}\n"
    lines += f"OBS_DUR+        {duration_lf}\n"

    if obs_mode != None:
        lines += f"OBS_MODE        {obs_mode.name}\n"

    if ra is not None:
        lines += f"OBS_RA          %.9f\n" % (ra)
    if dec is not None:
        lines += f"OBS_DEC         %+.9f\n" % (dec)

    lines += "OBS_FREQ1       1161394218\n"
    lines += "OBS_FREQ1+      53.000000009 MHz\n"
    lines += "OBS_FREQ2       1599656187\n"
    lines += "OBS_FREQ2+      73.000000010 MHz\n"
    lines += "OBS_BW          7\n"
    lines += "OBS_BW+         19.600 MHz\n\n"

    return lines
