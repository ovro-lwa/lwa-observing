#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 09:59:15 2023

@author: idavis
"""

import sys
from time import sleep
from astropy.time import Time
from observing.classes import ObsType
from observing import parsesdf
from mnc import control


def getdf(sdf_fn):
    d = parsesdf.sdf_to_dict(sdf_fn)
    session, obs_list = parsesdf.make_obs_list(d)

    tn = Time.now().mjd
    t0 = obs_list[0].obs_start

    if session.obs_type is ObsType.power:
        df = parsesdf.power_beam_obs(obs_list, session)
    if session.obs_type is ObsType.volt:
        df = parsesdf.volt_beam_obs(obs_list, session)
    if session.obs_type is ObsType.fast:
        df = parsesdf.fast_vis_obs(obs_list, session)
        pass

    return df


if __name__ == "__main__":
    """ Run commands parsed from SDF.
    """

    df = getdf(sys.argv[1])
    df.sort_index(inplace=True)
    for (mjd, row) in df.iterrows():
        if Time.now().mjd >= mjd:
            exec(row.command)
        else:
            wait = mjd - Time.now().mjd
            print(f"Need to wait {wait*24*3600} seconds to run next command...")
            sleep(wait*24*3600)
