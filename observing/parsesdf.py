import pandas as pd
from observing.classes import ObsType, Session, Observation
from astropy.time import Time
import sys
import logging

logger = logging.getLogger('observing')


def make_sched(sdf_fn, mode='buffer'):
    """ Use SDF to create a schedule dataframe.
    mode can be 'buffer' (sets up before running at scheduled time) or 'asap' (runs sequence of commands immediately)
    """

    d = sdf_to_dict(sdf_fn)
    session, obs_list = make_obs_list(d)

    if session.obs_type is ObsType.power:
        sched = power_beam_obs(obs_list, session, mode=mode)
    if session.obs_type is ObsType.volt:
        sched = volt_beam_obs(obs_list, session, mode=mode)
    if session.obs_type is ObsType.fast:
        sched = fast_vis_obs(obs_list, session, mode=mode)
    if session.obs_type is ObsType.slow:
        sched = slow_vis_obs(obs_list, session, mode=mode)

    logger.info(f"Parsed {sdf_fn} into {len(sched)} commands.")

    return sched


def sdf_to_dict(filename:str):
    """
    
    :param filename: name of the sdf file
    :type filename: str
    :return: Dictionary of session
    :rtype: dict

    """
    # Start a dictionary
    d = {'SESSION':{},'OBSERVATIONS':{}}
    
    # Open file and read lines
    lines = open(filename).readlines()
    
    # Assert that each session has at least one observation
    n_obs = 1
    d['OBSERVATIONS']['OBSERVATION_'+str(n_obs)] = {}
    for l in lines:
        l = l.strip('\n')
        
        # skip empty lines:
        if len(l) != 0:
            # Assume first word of the line is a key and the rest is information pertaining to that key:
            l = l.split()
            key = l[0]
            info = l[1:]
            
            if len(info) == 1:
                info = info[0]
            
            if 'OBS' in key:
                # Assume that if a key has already been assigned in an observation, then you've moved onto the next observation:
                if key in list(d['OBSERVATIONS']['OBSERVATION_'+str(n_obs)].keys()):
                    n_obs += 1
                    d['OBSERVATIONS']['OBSERVATION_'+str(n_obs)] = {}
                d['OBSERVATIONS']['OBSERVATION_'+str(n_obs)][key] = info
            
            elif 'OBS' not in key:
                d['SESSION'][key] = info
                
    return d


def make_obs_list(inp:dict):
    """
    :param inp: dictionary of the session and observation info. Presumably made from the sdf_to_dict() function
    :type inp: dict
    :return: Objects of the session and associated observations
    :rtype: Session object, list of Observation objects

    """
    try:
        obs_type = inp['SESSION']['SESSION_MODE']
    except KeyError:
        logger.warning("Missing SESSION_MODE keyword, assuming VOLT")
        obs_type = ObsType.volt
    session_id = inp['SESSION']['SESSION_ID']
    config_file = inp['SESSION'].get('CONFIG_FILE', None)
        
    if obs_type == ObsType.power.value or obs_type == ObsType.volt.value:
        beam_num = int(inp['SESSION']['SESSION_DRX_BEAM'])
        do_cal = inp['SESSION'].get('DO_CAL', True)
        cal_dir = inp['SESSION'].get('CAL_DIR', None)
    elif obs_type != ObsType.power.value or obs_type != ObsType.volt.value:
        beam_num = None
        do_cal = False
        cal_dir = None
    
    session = Session(obs_type,session_id,config_file,cal_dir,do_cal,beam_num)    
    obs_list = []
    for i in inp['OBSERVATIONS']:
        obs_id = int(inp['OBSERVATIONS'][i]['OBS_ID'])
        obs_dur = int(inp['OBSERVATIONS'][i]['OBS_DUR'])
        obs_mjd = int(inp['OBSERVATIONS'][i]['OBS_START_MJD'])
        obs_mpm = int(inp['OBSERVATIONS'][i]['OBS_START_MPM'])
        # Should this be a MJD, MPM tuple instead?
        obs_start = obs_mjd + obs_mpm/1000/86400
        # define mode from target. default to that specified
        obs_mode = inp['OBSERVATIONS'][i]['OBS_MODE']

        obs = Observation(session, obs_id, obs_start, obs_dur, obs_mode)
        try:
            if obs_list[-1].obs_start > obs.obs_start:
                raise Exception('Current observation starts before previous observation')
        except:
            pass
            
        if session.obs_type == ObsType.power or session.obs_type == ObsType.volt:
            ra = inp['OBSERVATIONS'][i].get('OBS_RA', None)
            dec = inp['OBSERVATIONS'][i].get('OBS_DEC', None)
            int_time = inp['OBSERVATIONS'][i].get('OBS_INT_TIME', None)
            obj_name = inp['OBSERVATIONS'][i].get('OBS_TARGET', None)
            if obj_name is None and ra is None and dec is None:
                logger.warning("OBS_TARGET or OBS_RA/DEC must be defined")

            if session.obs_type == ObsType.volt:
                try:
                    bw = inp['OBSERVATIONS'][i]['OBS_BW']
                    freq1 = inp['OBSERVATIONS'][i]['OBS_STP_FREQ1[1]']
                    freq2 = inp['OBSERVATIONS'][i]['OBS_STP_FREQ2[1]']
                    gain = inp['OBSERVATIONS'][i]['OBS_DRX_GAIN']
                except:
                    raise Exception('voltage observation requires defining OBS_BW, OBS_STP_FREQ1[1], and OBS_STP_FREQ2[1]')
            else:
                bw, freq1, freq2, gain = None, None, None, None

            obs.set_beam_props(ra, dec, obj_name=obj_name, int_time=int_time, bw=bw, freq1=freq1, freq2=freq2, gain=gain)
            
        obs_list.append(obs)
    return session,obs_list


def fast_vis_obs(obs_list, session, mode="buffer"):
    """ Generate dataframe for fast visibility observing mode
    """

    if mode == "buffer":
        startbuffer = 20
    elif mode == "asap":
        startbuffer = 0.

    start = obs_list[0].obs_start
    ts = start - startbuffer/3600/24  # do the control command  before the start of the first observation
    cmd = f"con = control.Controller('{session.config_file}')"
    d = {ts:cmd}

    # handy name 
    session_mode_name = f"{session.session_id}_{session.obs_type.value}"
    if session.beam_num is not None:
        session_mode_name += f"{session.beam_num}"

    ts += 0.1/(24*3600)
    cmd = "con.configure_xengine(['drvf'])"
    d.update({ts:cmd})

    for obs in obs_list:
        end = obs.obs_start + obs.obs_dur/24/3600/1e3
        cmd = f"con.start_dr(['drvf'], t0 = {start})"
        d.update({start:cmd})
        cmd = f"con.stop_dr(['drvf'])"
        d.update({end:cmd})
    df = pd.DataFrame(d,index = ['command'])
    df = df.transpose()
    df.insert(1, column='session_id', value=session.session_id)
    df.insert(1, column='session_mode_name', value=session_mode_name)
    return df


def slow_vis_obs(obs_list, session, mode="buffer"):
    """ Generate dataframe for slow visibility observing mode
    """

    if mode == "buffer":
        startbuffer = 20
    elif mode == "asap":
        startbuffer = 0.

    start = obs_list[0].obs_start
    ts = start - startbuffer/3600/24  # do the control command  before the start of the first observation
    cmd = f"con = control.Controller('{session.config_file}')"
    d = {ts:cmd}

    # handy name 
    session_mode_name = f"{session.session_id}_{session.obs_type.value}"
    if session.beam_num is not None:
        session_mode_name += f"{session.beam_num}"

    ts += 0.1/(24*3600)
    cmd = "con.configure_xengine(['drvs'])"
    d.update({ts:cmd})

    for obs in obs_list:
        end = obs.obs_start + obs.obs_dur/24/3600/1e3
        cmd = f"con.start_dr(['drvs'], t0 = {start})"
        d.update({start:cmd})
        cmd = f"con.stop_dr(['drvs'])"
        d.update({end:cmd})
    df = pd.DataFrame(d,index = ['command'])
    df = df.transpose()
    df.insert(1, column='session_id', value=session.session_id)
    df.insert(1, column='session_mode_name', value=session_mode_name)
    return df


def power_beam_obs(obs_list, session, mode='buffer'):
    """ Generate dataframe for power beam observing mode
    """

    if mode == 'buffer':
        controller_buffer = 20
        configure_buffer = 20
        cal_buffer = 360
        pointing_buffer = 10
        recording_buffer = 5
    elif mode == 'asap':
        controller_buffer = 0.1
        configure_buffer = 0.1
        cal_buffer = 0.1
        pointing_buffer = 0.1
        recording_buffer = 0.1

    if session.do_cal == True:
        logger.debug("Scheduling for calibration time...")
        dt = (controller_buffer + configure_buffer + cal_buffer + pointing_buffer + recording_buffer)/3600/24
    else:
        logger.debug("Not scheduling for calibration time...")
        cal_buffer = 0
        dt = (controller_buffer + configure_buffer + pointing_buffer)/3600/24

    # handy name 
    session_mode_name = f"{session.session_id}_{session.obs_type.value}"
    if session.beam_num is not None:
        session_mode_name += f"{session.beam_num}"

    t0 = obs_list[0].obs_start
    ts = t0 - dt
    cmd = f"con = control.Controller('{session.config_file}')"
    d = {ts:cmd}
    # okay. originally, I was trying to avoid having two commands have the same timestamp to avoid confusing 
    # the scheduler. I'm deciding that should not be the perogative of the parser.
        
    if session.cal_directory is not None:
        # re-assign calibration directory if it is specified
        ts += 0.1/(24*3600)
        cmd = f"con.conf['xengines']['cal_directory'] = '{session.cal_directory}'"
        d.update({ts:cmd})

    # Configure for the beam
    ts += controller_buffer/24/3600 
    cmd = f"con.configure_xengine(['dr'+str({session.beam_num})], calibratebeams = {session.do_cal})"
    d.update({ts:cmd})

    if session.cal_directory is not None and session.do_cal == True:
        ts += cal_buffer/24/3600

    # Go through the list of observations
    for obs in obs_list:
        ts = obs.obs_start - (pointing_buffer + recording_buffer)/24/3600
        if mode == 'buffer':
            t0 = obs.obs_start
        elif mode == 'asap':
            t0 = "'now'"
        cmd = f"con.start_dr(recorders=['dr'+str({session.beam_num})], duration = {obs.obs_dur}, time_avg={obs.int_time}, t0 = {t0})"
        d.update({ts:cmd})

        ts += recording_buffer/24/3600
        if obs.dec is None:
            targetname = obs.obj_name
            cmd = f"con.control_bf(num = {session.beam_num}, targetname='{targetname}', track={obs.tracking}, duration={(obs.obs_dur+pointing_buffer)/1e3})"
        elif obs.dec is not None:
            cmd = f"con.control_bf(num = {session.beam_num}, coord = ({obs.ra/15},{obs.dec}), track={obs.tracking}, duration={(obs.obs_dur+pointing_buffer)/1e3})"
        d.update({ts:cmd})

    df = pd.DataFrame(d, index = ['command'])
    df = df.transpose()
    df.insert(1, column='session_id',value=session.session_id)
    df.insert(1, column='session_mode_name', value=session_mode_name)
    return df


def volt_beam_obs(obs_list, session, mode='buffer'):
    """ Generate dataframe for volteage beam observing mode
    """

    if mode == 'buffer':
        controller_buffer = 20
        configure_buffer = 20
        cal_buffer = 360
        pointing_buffer = 10
        recording_buffer = 5
    elif mode == 'asap':
        controller_buffer = 0.1
        configure_buffer = 0.1
        cal_buffer = 0.1
        pointing_buffer = 0.1
        recording_buffer = 0.1

    if session.do_cal:
        dt = (configure_buffer + cal_buffer + controller_buffer + pointing_buffer + recording_buffer)/3600/24
    elif not session.do_cal:
        cal_buffer = 0
        dt = (controller_buffer + configure_buffer + pointing_buffer)/3600/24

    # handy name 
    session_mode_name = f"{session.session_id}_{session.obs_type.value}"
    if session.beam_num is not None:
        session_mode_name += f"{session.beam_num}"

    assert session.beam_num == 1, "voltage beamforming currently only supported on beam 1"

    t0 = obs_list[0].obs_start
    ts = t0 - dt
    cmd = f"con = control.Controller('{session.config_file}')"
    d = {ts:cmd}
    # okay. originally, I was trying to avoid having two commands have the same timestamp to avoid confusing 
    # the scheduler. I'm deciding that should not be the perogative of the parser.
        
    if session.cal_directory is not None and session.do_cal == True:
        # re-assign calibration directory if it is specified
        cmd = f"con.conf(['xengine']['cal_directory'] = '{session.cal_directory}')"
        d.update({ts:cmd})

    # Configure for the beam
    ts += controller_buffer/24/3600 
    cmd = f"con.configure_xengine(['dr'+str({session.beam_num})], calibratebeams = {session.do_cal})"
    d.update({ts:cmd})
    ts += (configure_buffer + cal_buffer)/24/3600

    # Go through the list of observations
    for obs in obs_list:
        ts = obs.obs_start - (pointing_buffer + recording_buffer)/24/3600
        if mode == 'buffer':
            t0 = obs.obs_start
        elif mode == 'asap':
            t0 = "'now'"
        beam_gain = obs.gain
        if beam_gain is None or beam_gain == -1:
            beam_gain = 6
            logger.warning(f"OBS_DRX_GAIN is not defined, using a value of {beam_gain}")
            
        cmd = f"con.start_dr(recorders=['drt'+str({session.beam_num})], duration = {obs.obs_dur}, time_avg=0, t0={t0}, teng_f1={obs.freq1}, teng_f2={obs.freq2}, f0={obs.bw}, gain={beam_gain})"
        d.update({ts:cmd})

        ts += (recording_buffer)/24/3600
        if obs.dec is None:
            targetname = obs.obj_name
            cmd = f"con.control_bf(num = {session.beam_num}, targetname='{targetname}', track={obs.tracking}, duration = {(obs.obs_dur+pointing_buffer)/1e3})"
        elif obs.dec is not None:
            cmd = f"con.control_bf(num = {session.beam_num}, coord = ({obs.ra/15},{obs.dec}), track={obs.tracking}, duration = {(obs.obs_dur+pointing_buffer)/1e3})"
        d.update({ts:cmd})

    df = pd.DataFrame(d, index = ['command'])
    df = df.transpose()
    df.insert(1, column='session_id',value=session.session_id)
    df.insert(1, column='session_mode_name', value=session_mode_name)
    return df
