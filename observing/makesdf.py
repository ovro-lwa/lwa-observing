import pandas as pd
import numpy as np
from astropy.time import Time
from astropy import coordinates
from datetime import timedelta
from observing import obsstate, classes
import random
import os
import logging
import getpass

logger = logging.getLogger('observing')


def create(out_name, sess_id=None, sess_mode=None, beam_num=None, cal_dir='/home/pipeline/caltables/latest', do_cal=False,
           pi_id=None, pi_name=None, config_file=None, n_obs=1, obs_mode=None, obs_start=None, obs_dur=None, ra=None,
           dec=None, obj_name=None, int_time=None):
    """ Create a file out_name as an SDF
    """

    sdf_text = ''

    if sess_id is None:
        try:
            sess_id = obsstate.iterate_max_session_id()
        except:
            sess_id = random.randint(0, 10000)
            print(f"No Session ID provided and could not access obsstate. Setting random Session ID of {sess_id}")

    if sess_mode is None:
        sess_mode = input(f"Enter a session mode of {classes.ObsType.__name__}")

    sess_mode = classes.ObsType(sess_mode)
    
    if sess_mode.value not in ["FAST", "SLOW"]:
        if beam_num is None:
            inp = input("Provide a beam number:")
            beam_num = int(inp)
        if cal_dir is not None:
            assert os.path.exists(cal_dir), f"cal_dir ({cal_dir}) does not exist"
        if do_cal:
            assert os.path.exists(cal_dir), f"Cannot use do_cal if cal_dir ({cal_dir}) does not exist"
    else:
        beam_num = None
        cal_dir = None
        do_cal = False

    if pi_name is None:
        pi_name = getpass.getuser()

    if pi_id is None:
        try:
            pi_id = obsstate.check_and_create_pi(pi_name)
            logger.info(f"No PI ID provided. Getting new ID of {pi_id} for user {pi_name}")
        except:
            pi_id = random.randint(0, 10000)
            logger.warn(f"No PI ID provided and could not access obsstate. Setting random PI ID of {pi_id} for user {pi_name}")

    if config_file is None:
        print("No configuration file specified. Assuming the standard path.")
        config_file = "/home/pipeline/proj/lwa-shell/mnc_python/config/lwa_config_calim.yaml"

    try:
        session_preamble = make_session_preamble(sess_id, sess_mode, pi_id, pi_name, beam_num, config_file, cal_dir, do_cal)
        sdf_text += session_preamble
        print(session_preamble)
    except:
        raise Exception("Couldn't make the session preamble")
    
    obs_text = make_oneobs(1, sess_mode=sess_mode, obs_mode=obs_mode, obs_start=obs_start, obs_dur=obs_dur, ra=ra, dec=dec, obj_name=obj_name,
                           int_time=int_time)
    sdf_text += obs_text

    # after first, all others will prompt for input
    for obs_count in range(2, n_obs+1):
        obs_text = make_oneobs(obs_count, sess_mode=sess_mode, obs_mode=obs_mode, obs_dur=obs_dur, int_time=int_time)
        sdf_text += obs_text
        print(obs_text)
        print("Add another observation? (Y/N)")

    if os.path.exists(out_name):
        print(f"WARNING: {out_name} already exists. Overwriting.")
    else:
        print(f"Writing out to {out_name}")

    with open(out_name,'w') as f:
        f.write(sdf_text)



def make_oneobs(obs_count, sess_mode=None, obs_mode=None, obs_start=None, obs_dur=None, ra=None, dec=None, obj_name=None, int_time=None, az=None, alt=None):
    """ Create string for one observation
    """

    print(f"Making observation {obs_count}")
    if obs_mode is None:
        if sess_mode.value in ['POWER', 'VOLT']:
            obs_mode = classes.EphemModes('TRK_RADEC')
            logger.warn("no obs_mode provided, assuming TRK_RADEC")
    elif obs_mode in ['TRK_JOV', 'TRK_SOL', 'TRK_LUN']:
        obs_mode = classes.EphemModes(obs_mode)
        if 'SOL' in obs_mode.value:
            obj_name = 'Sun'
        elif 'LUN' in obs_mode.value:
            obj_name = 'Moon'
        elif 'JOV' in obs_mode.value:
            obj_name = 'Jupiter'
        ra = 0.
        dec = 0.
    elif obs_mode in ['AZALT']:
        logger.info("Using provided (RA, Dec) as (Az, Alt) for this obs_mode.")
        obs_mode = classes.EphemModes(obs_mode)
        az = ra
        alt = dec
        ra = None
        dec = None
    else:
        obs_mode = classes.EphemModes(obs_mode)

    if sess_mode.value in ['POWER', 'VOLT']:
        if (ra is None or dec is None) and az is None and alt is None:
            if obj_name is not None and isinstance(obj_name, str):
                try:
                    co = coordinates.SkyCoord.from_name(obj_name)
                    ra = co.ra.deg
                    dec = co.dec.deg
                except:
                    logger.warn(f"Could not parse {obj_name}. Not seting (RA, Dec) from that.")
            else:
                coords = input("Give target as RA DEC ('[deg], [deg]') or a single object name (no commas):")
                try:
                    objectspl = coords.split(',')
                    if len(objectspl) == 2:
                        ra = float(objectspl[0])
                        dec = float(objectspl[1])
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

    if int_time is None and sess_mode.value == 'POWER': 
        print(f"Give the integrations time of the observation in milliseconds")
        int_time = int(input())
    if sess_mode.name == 'VOLT':
        int_time = None

    if int_time is not None:
        assert int_time <= 1024, "Integration time must be less than 1024 ms"
    obs_text = make_obs_block(obs_count, obs_start, obs_dur, ra, dec, obj_name, int_time, obs_mode, az=az, alt=alt)
    return obs_text


def make_session_preamble(session_id, session_mode, pi_id = 0, pi_name:str = 'Observer', beam_num = None,
                          config_dir = '/home/pipeline/proj/lwa-shell/mnc_python/config/lwa_calim_config.yaml', cal_dir = None,
                          do_cal = False):
    """ Create preamble info required for a proper SDF
    """

    lines = f'PI_ID            {pi_id}\n'
    lines += f'PI_NAME          {pi_name}\n\n'
    lines += 'PROJECT_ID       0\n'
    lines += f'SESSION_ID       {session_id}\n'
    lines += f'SESSION_MODE     {session_mode.value}\n'
    if beam_num != None:
        lines += f'SESSION_DRX_BEAM       {beam_num}\n'
    lines += f'CONFIG_FILE      {config_dir}\n'
    if cal_dir != None:
        lines += f'CAL_DIR          {cal_dir}\n'
    lines += f'DO_CAL           {do_cal}\n'
    lines += '\n'

    return lines


def make_obs_block(obs_id, start_time:str, duration, ra = None, dec = None, obj_name = None, integration_time = 1, obs_mode = None, az = None, alt = None):
    """ Create an observation block for the SDF
    Note that RA for the function is in degrees, but the SDF standard uses hours (converted internally).
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
    lines += f'OBS_TARGET      {obj_name}\n'
    lines += f'OBS_START_MJD   {mjd_start}\n'
    lines += f'OBS_START_MPM   {mpm}\n'
    lines += f"OBS_START       UTC {start_time.replace('-',' ').replace('T',' ')}\n"
    lines += f"OBS_DUR         {int(duration)}\n"
    if integration_time != None:
        lines += f"OBS_INT_TIME    {integration_time}\n"
    lines += f"OBS_DUR+        {duration_lf}\n"

    if obs_mode != None:
        lines += f"OBS_MODE        {obs_mode.value}\n"

    if ra is not None:
        lines += f"OBS_RA          %.9f\n" % (ra / 15)  # SDF standard defines OBS_RA is in hours not degrees
    if dec is not None:
        lines += f"OBS_DEC         %+.9f\n" % (dec)

    if az is not None:
        lines += f"OBS_AZ          {az}\n"
    if alt is not None:
        lines += f"OBS_ALT         {alt}\n"

    lines += "OBS_FREQ1       1161394218\n"
    lines += "OBS_FREQ1+      53.000000009 MHz\n"
    lines += "OBS_FREQ2       1599656187\n"
    lines += "OBS_FREQ2+      73.000000010 MHz\n"
    lines += "OBS_BW          7\n"
    lines += "OBS_BW+         19.600 MHz\n"
    lines += "OBS_DRX_GAIN    6\n"

    return lines
