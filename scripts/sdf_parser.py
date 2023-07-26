#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 09:59:15 2023

@author: idavis
"""

from astropy.time import Time
import warnings
from enum import Enum
import pandas as pd

class ObsType(Enum):
    volt = 'VOLT'
    power = 'POWER'
    fast = 'FAST'

class Session:
    """
    Contains information about the controller settings for the session 
    """
    def __init__(self, obs_type, session_id, config_file = None, cal_directory = None, do_cal = True,beam_num = None):
        """
        
        :param obs_type: defines whether the session will be for a power beam, voltage beam, or fast vis
        :type obs_type: str
        :param config_file: path to the configuration file, defaults to '/home/pipeline/proj/lwa-shell/mnc_python/config/lwa_config_calim.yaml'
        :type config_file: str, optional
        :param cal_directory: path to the calibration directory. If None, then it is set as the calibration directory defined in the configuration file, defaults to None
        :type cal_directory: str, optional
        :param do_cal: If doing a beam observation, it defines whether to calibrate the beam at the beginning of the session, defaults to True
        :type do_cal: bool, optional
        :param beam_num: if doing a beam observation, this defines which beam to use, defaults to None
        :type beam_num: int, optional
        :return: DESCRIPTION
        :rtype: TYPE

        """
        self.session_id = session_id
        self.obs_type = ObsType(obs_type)
        if config_file is None:
            self.config_file = '/home/pipeline/proj/lwa-shell/mnc_python/config/lwa_config_calim.yaml'
        elif config_file is not None:
            self.config_file = config_file
        self.cal_directory = cal_directory
        self.beam_num = beam_num
        self.do_cal = bool(do_cal)
        self.__validate()
        return
    
    def __validate(self):
        if self.beam_num is not None:
            self.beam_num = int(self.beam_num)
            
        valid_beam_nums = range(1,17)
        if (self.obs_type is ObsType.power or self.obs_type is ObsType.volt) and self.beam_num not in valid_beam_nums:
            raise Exception("You must specify a valid beam number if you want to observe with a beam")
        return


class Observation:
    def __init__(self, session:Session, obs_id, obs_start_mjd,obs_start_mpm, obs_dur, obs_mode):
        self.session = session
        self.obs_id = obs_id
        self.obs_start_mjd = obs_start_mjd
        self.obs_start_mpm = obs_start_mpm
        self.obs_start = obs_start_mjd + obs_start_mpm/1e3/3600/24
        self.obs_dur = obs_dur
        assert(self.obs_dur > 0),'Duration cannot be negative'
        self.obs_mode = obs_mode
        return
    
    def set_beam_props(self,ra, dec = None, obj_name =None,int_time = None):
        # Check if the observing mode is one of the tracking modes:
        tracking_modes = ['TRK_RADEC','TRK_SOL', 'TRK_JOV','TRK_LUN']
        if self.obs_mode in tracking_modes:
            self.tracking = True

        # If the observing mode isn't one of the tracking modes, then set tracking to False
        elif self.obs_mode not in tracking_modes:
            self.tracking = False
        
        # Setting ra and dec values if the system isn't using one of the modes that require ephemerides: 
        ephem_modes = tracking_modes[1:]
        
        if self.obs_mode not in ephem_modes:
            # If the dec is None, then assume that the user wants to resolve to a target based on its name
            if dec is None and self.obs_mode:
                assert(obj_name is not None)
                self.obj_name = obj_name
                self.ra = None
                self.dec = None
            elif dec is not None:
                self.ra = float(ra)
                self.dec = float(dec)
                self.obj_name = obj_name

        elif self.obs_mode in ephem_modes:
            self.ra = None
            self.dec = None
            if self.obs_mode == 'TRK_SOL':
                self.obj_name = 'Sun'
            if self.obs_mode == 'TRK_JOV':
                self.obj_name = 'Jupiter'
            if self.obs_mode == 'TRK_LUN':
                self.obj_name = 'Moon'

        # Set integration time of the beam. If none, the default is 1ms:            
        if int_time is not None:
            self.int_time = int(int_time)
        elif int_time is None:
            self.int_time = 1
        return


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
    obs_type = inp['SESSION']['SESSION_MODE']
    session_id = inp['SESSION']['SESSION_ID']
    try:
        config_file = inp['SESSION']['CONFIG_FILE']
    except:
        warnings.warn('No config_file specified. Assuming the standard')
        config_file = None
        
    if obs_type == ObsType.power.value or obs_type == ObsType.volt.value:
        beam_num = int(inp['SESSION']['SESSION_DRX_BEAM'])
        try:
            do_cal = inp['SESSION']['DO_CAL']
        except:
            warnings.warn('Instructions to calibrate not specified. Assuming the beam should be calibrated')
            do_cal = True
            
        try:
            cal_dir = inp['SESSION']['CAL_DIR']
        except:
            warnings.warn('No cal directory specified. Assuming the one in the configuration file')
            cal_dir = None
            
    elif obs_type != ObsType.power.value or obs_type != ObsType.volt.value:
        beam_num = None
        cal_dir = None
        do_cal = False
    
    session = Session(obs_type,session_id,config_file,cal_dir,do_cal,beam_num)    
    obs_list = []
    for i in inp['OBSERVATIONS']:
        obs_id = int(inp['OBSERVATIONS'][i]['OBS_ID'])
        obs_dur = int(inp['OBSERVATIONS'][i]['OBS_DUR'])
        obs_start_mjd = int(inp['OBSERVATIONS'][i]['OBS_START_MJD'])
        obs_start_mpm = int(inp['OBSERVATIONS'][i]['OBS_START_MPM'])
        obs_mode = inp['OBSERVATIONS'][i]['OBS_MODE']
        obs = Observation(session, obs_id, obs_start_mjd, obs_start_mpm, obs_dur, obs_mode)
        try:
            if obs_list[-1].obs_start > obs.obs_start:
                raise Exception('Current observation starts before previous observation')
        except:
            pass
            
        if session.obs_type == ObsType.power or session.obs_type == ObsType.volt:
            try:
                ra = inp['OBSERVATIONS'][i]['OBS_RA']
            except:
                raise Exception('Need to give RA or name of object for a beam observation')
            try:
                dec = inp['OBSERVATIONS'][i]['OBS_DEC']
            except:
                warnings.warn('No declination given, assuming the RA input is the name of an object')
                dec = None
            try:
                int_time = inp['OBSERVATIONS'][i]['OBS_INT_TIME']
            except:
                int_time = None
                warnings.warn('No integration time given. Assuming 1 ms')
            try:
                obj_name = inp['OBSERVATIONS'][i]['OBS_TARGET']
            except:
                obj_name = None
            obs.set_beam_props(ra,dec,obj_name=obj_name,int_time= int_time)
            
        obs_list.append(obs)
    return session,obs_list


def main(sdf_fn):
    d = sdf_to_dict(sdf_fn)
    session, obs_list = make_obs_list(d)
    tn = Time.now().mjd
    t0 = obs_list[0].obs_start
    assert(t0 > tn),"The observations take place in the past"
    if session.obs_type is ObsType.power:
        df = power_beam_obs(obs_list,session)
    if session.obs_type is ObsType.volt:
        df = volt_beam_obs(obs_list,session)
    if session.obs_type is ObsType.fast:
        df = fast_vis_obs(obs_list,session)
        pass
    return df


def fast_vis_obs(obs_list, session, buffer = 20):
    start = obs_list[0].obs_start
    ts = obs.obs_start - buffer/3600/24 #do the control command  before the start of the first observation
    cmd = f"con = control.Controller({session.config_file})"
    d = {ts:cmd}

    con.configure_xengine(['drvf'])
    for obs in obs_list:
        end = obs.obs_start + duration/24/3600/1e3
        cmd = f"con.start_dr(['drvf'], t0 = {start})"
        d.update({start:cmd})
        cmd = f"con.stop_dr(['drvf'])"
        d.update({end:cmd})
    df = pd.DataFrame(d,index = ['command'])
    df = df.transpose()
    df.insert(1,column = 'session_id',value = session.session_id)
    return df

def power_beam_obs(obs_list,session,controller_buffer = 20, configure_buffer = 20, cal_buffer = 600, pointing_buffer = 10,recording_buffer = 5):

    if session.do_cal:
        dt = (configure_buffer + cal_buffer + controller_buffer + pointing_buffer + recording_buffer)/3600/24
    elif not session.do_cal:
        cal_buffer = 0
        dt = (controller_buffer + configure_buffer + pointing_buffer)/3600/24

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
    cmd = f"con.configure_xengine(['dr'+str({session.beam_num})], calibrate_beams = {session.do_cal})"
    d.update({ts:cmd})
    ts += (configure_buffer + cal_buffer)/24/3600

    # Go through the list of observations
    for obs in obs_list:
        ts = obs.obs_start - (pointing_buffer - recording_buffer)/24/3600
        if obs.dec is None:
            cmd = f"con.control_bf(num = {session.beam_num}, targetname = {obs.obj_name}, track={obs.tracking})"
        elif obs.dec is not None:
            cmd = f"con.control_bf(num = {session.beam_num}, ra = {obs.ra}, dec = {obs.dec}, track={obs.tracking})"
        d.update({ts:cmd})

        cmd = f"con.start_dr(recorders=['dr'+str({session.beam_num})], duration = {obs.obs_dur}, time_avg={obs.int_time},t0 = {obs.obs_start})"
        ts += pointing_buffer + pointing_buffer/24/3600
        d.update({ts:cmd})

    df = pd.DataFrame(d, index = ['command'])
    df = df.transpose()
    df.insert(1,column = 'session_id',value = session.session_id)
    return df

def volt_beam_obs(obs):
    return

