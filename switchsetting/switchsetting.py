import enum
import os
import logging
import subprocess

from astropy.coordinates import SkyCoord, EarthLocation, ICRS, AltAz
from astropy.coordinates import solar_system_ephemeris, EarthLocation
from astropy.coordinates import get_body
from astropy.time import Time, TimeDelta
from astropy import units as u
import numpy as np

from mnc.control import settings

OVRO_LWA_LOCATION = EarthLocation(lat=37.2398 * u.deg, lon=-118.282 * u.deg, height=1216 * u.m)

TAU_BOO = SkyCoord('13h47m15.74s', '+17deg27m24.9s', frame=ICRS)

ALT_THRESHOLD = 5 * u.deg

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('__name__')
logger.setLevel(logging.DEBUG)

class Setting(enum.Enum):
    NIGHT = '/home/pipeline/proj/lwa-shell/mnc_python/data/20230721-settingsAll-night.mat'
    DAY = '/home/pipeline/proj/lwa-shell/mnc_python/data/20230721-settingsAll-day.mat'

def manual_override() -> bool:
    settingslog = '/home/pipeline/proj/lwa-shell/mnc_python/data/arxAndF-settings.log'
    with open(settingslog) as f:
        lines = f.readlines()
    for l in reversed(lines):
        sp = l.split('\t')
        # check for empty line
        if len(sp) <= 1:
            continue
        if len(sp) != 5:
            logger.warn(f'Cannot parse last lines of {settingslog}. Did the format change?')
            raise ValueError('Cannot parse settings log.')
        if 'yuping' == sp[2].strip().rstrip().lower():
            return False
        else:
            return True


def enforce_setting(setting: Setting):
    logger.info(f'Updating setting to {setting.value}.')
    settings.update(setting.value)

if __name__ == '__main__':
    logger.debug('Waking up.')
    if manual_override():
        logger.info('Manual override. No setting switch.')
        exit(0)

    altaz_frame = AltAz(location=OVRO_LWA_LOCATION)
    # Calculate Altitude for the next 16 hours at 10 min cadence.
    times = Time.now() + TimeDelta((10 * np.arange(16 * 60 // 10)) * u.minute)
    with solar_system_ephemeris.set('builtin'):
        sun = get_body('sun', times, OVRO_LWA_LOCATION)
    sun_coords = sun.transform_to(altaz_frame)
    current_sun_alt = sun_coords.alt[0].to(u.deg)
    if current_sun_alt < ALT_THRESHOLD:
        enforce_setting(Setting.NIGHT)
    else:
        enforce_setting(Setting.DAY)

    # Figure out when the Sun's altitude cross the threshold next.
    next_time = next(ts for ts, a in zip(times, sun_coords.alt.to(u.deg)) if
                     (a - ALT_THRESHOLD).value * (current_sun_alt - ALT_THRESHOLD).value < 0) 

    seconds_till_next = int((next_time - Time.now()).to(u.second).value)
    if seconds_till_next < 0:
        logger.error(f'Got seconds till next run {seconds_till_next} < 0.')
        seconds_till_next = 10
    logger.debug(f'Next run is {seconds_till_next} s away.')
    res = subprocess.check_output(
        ['/usr/bin/systemd-run', '--user', f'--on-active={seconds_till_next}',
            '--unit switchsetting-onetime',
            '/opt/devel/pipeline/envs/deployment/bin/python',
            os.path.abspath(__file__)],
        stderr=subprocess.STDOUT)
    logger.debug(res)