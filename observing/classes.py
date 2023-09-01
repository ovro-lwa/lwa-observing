from enum import Enum

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
    def __init__(self, session:Session, obs_id, obs_start, obs_dur, obs_mode):
        self.session = session
        self.obs_id = obs_id
        self.obs_start = obs_start
        self.obs_start_mjd = int(obs_start)
        self.obs_start_mpm = int((obs_start - int(obs_start)) * 24*3600*1e3)
        self.obs_dur = obs_dur
        assert(self.obs_dur > 0),'Duration cannot be negative'
        self.obs_mode = obs_mode
    
    def set_beam_props(self, ra, dec=None, obj_name=None, int_time=None, bw=None, freq1=None, freq2=None):

        # overload obs_mode for some targets
        if obj_name.lower() == 'sun':
            self.obs_mode = 'TRK_SOL'
        elif obj_name.lower() == 'jupiter':
            self.obs_mode = 'TRK_JOV'
        elif obj_name.lower() == 'moon':
            self.obs_mode = 'TRK_LUN'

        # Check if the observing mode is one of the tracking modes:
        tracking_modes = ['TRK_RADEC', 'TRK_SOL', 'TRK_JOV','TRK_LUN']
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

        else:
            self.ra = None
            self.dec = None
            self.obj_name = obj_name

        # Set integration time of the beam. If none, the default is 1ms:            
        if int_time is not None:
            self.int_time = int(int_time)
        elif int_time is None:
            self.int_time = 1
            
        if self.session.obs_type is ObsType.volt:
            self.bw = bw
            self.freq1 = freq1
            self.freq2 = freq2

        return
