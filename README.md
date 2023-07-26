# lwa-observing

Code to use and run observations with the OVRO-LWA.

## Requirements

- mnc_python

## Goals

1. Set up observations in advance -- We want to be able to submit an observation up to 24 hours in advance.
2. Control commensal observations -- Different observations (data recorders, beam control) should be schedulable and controllable independently. This requires separate threads/processes for beam control.
3. Control of all subsystems -- The scheduler should be able to submit commands to f-engine, x-engine, and data recorder subsystems (i.e., execute mnc-python functions).
4. Visibility -- Users should be able to see what has been scheduled up to 24 hours in advance. Any user can submit, but submission should check on whether requested observations collide or interfere with other observations.
5. Calibrated beams -- Beamforming observations should be schedulable such that they apply good calibration for the time of observation (note: this means making and applying new tables across day/night boundaries).
6. Automated scheduling -- It should be possible for an automated process (a script in a loop) to decide to submit an observation. An important application of this is ASAP beamformed observations of FRBs detected by DSA-110.

