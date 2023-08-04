#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 09:59:15 2023

@author: idavis
"""

from astropy.time import Time
from observing.classes import ObsType
from observing import parsesdf

def main(sdf_fn):
    d = parsesdf.sdf_to_dict(sdf_fn)
    session, obs_list = parsesdf.make_obs_list(d)

    tn = Time.now().mjd
    t0 = obs_list[0].obs_start
    assert(t0 > tn),"The observations take place in the past"

    if session.obs_type is ObsType.power:
        df = parsesdf.power_beam_obs(obs_list, session)
    if session.obs_type is ObsType.volt:
        df = parsesdf.volt_beam_obs(obs_list, session)
    if session.obs_type is ObsType.fast:
        df = parsesdf.fast_vis_obs(obs_list, session)
        pass

    return df
