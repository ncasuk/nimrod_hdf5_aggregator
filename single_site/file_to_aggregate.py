"""
Author: Joshua M Hampton
Date Created: 24 July 2020
Institution: National Centre for Atmospheric Science
             University of Leeds
Contact: joshua.hampton@ncas.ac.uk
         j.m.hampton@leeds.ac.uk
         joshua.hampton@live.co.uk

Take incoming, single elevation scan from Met Office Radar 
in ODIM_H5 format, places into daily aggregate file.
"""

import h5py
import numpy as np
from time import sleep
import ast
import sys

all_odim_quantities = {'TH':     {'unit':'dBZ',        'description':'Logged horizontally-polarized total (uncorrected) reflectivity factor'},
                       'TV':     {'unit':'dBZ',        'description':'Logged vertically-polarized total (uncorrected) reflectivity factor'},
                       'DBZH':   {'unit':'dBZ',        'description':'Logged horizontally-polarized (corrected) reflectivity factor'},
                       'DBZV':   {'unit':'dBZ',        'description':'Logged vertically-polarized (corrected) reflectivity factor'},
                       'ZDR':    {'unit':'dBZ',        'description':'Logged differetial reflectivity'},
                       'RHOHV':  {'unit':'',           'description':'Correlation between DBZH and DBZV'},
                       'LDR':    {'unit':'dB',         'description':'Linear depolarization ratio'},
                       'PHIDP':  {'unit':'degrees',    'description':'Differential Phase'},
                       'KDP':    {'unit':'degrees/km', 'description':'Specific Differential Phase'},
                       'SQI':    {'unit':'',           'description':'Signal quality index'},
                       'SQIH':   {'unit':'',           'description':'Signal quality index - horizontally-polarized'},
                       'SQIV':   {'unit':'',           'description':'Signal quality index - vertically-polarized'},
                       'SNRH':   {'unit':'',           'description':'Normalized signal-to-noise ratio - horizontally-polarized'},
                       'SNRV':   {'unit':'',           'description':'Normalized signal-to-noise ratio - vertically-polarized'},
                       'CCORH':  {'unit':'dB',         'description':'Clutter correction - horizontally-polarized'},
                       'CCORV':  {'unit':'dB',         'description':'Clutter correction - vertically-polarized'},
                       'RATE':   {'unit':'mm/h',       'description':'Rain rate'},
                       'URATE':  {'unit':'mm/h',       'description':'Uncorrected rain rate'},
                       'HI':     {'unit':'dB',         'description':'Hail intensity'},
                       'HP':     {'unit':'percent',    'description':'Hail probability'},
                       'ACCRR':  {'unit':'mm',         'description':'Accumulated precipitation'},
                       'HGHT':   {'unit':'km',         'description':'Height (of echotops)'},
                       'VIL':    {'unit':'kg/m^2',     'description':'Vertical Integrated Liquid water'},
                       'VRAD':   {'unit':'m/s',        'description':'Radial velocity'},
                       'VRADH':  {'unit':'m/s',        'description':'Radial velocity - horizontally-polarized. Radial winds towards the radar are negative, while radial winds away from the radar are positive (PANT)'},
                       'VRADV':  {'unit':'m/s',        'description':'Radial velocity - vertically-polarized. Radial winds towards the radar are negative, while radial winds away from the radar are positive (PANT)'},
                       'VRADDH': {'unit':'m/s',        'description':'Dealiased horizontally-polarized radial velocity'},
                       'VRADDV': {'unit':'m/s',        'description':'Dealiased vertically-polarized radial velocity'},
                       'WRAD':   {'unit':'m/s',        'description':'Spectral width of radial velocity'},
                       'WRADH':  {'unit':'m/s',        'description':'Spectral width of radial velocity - horizontally-polarized'},
                       'WRADV':  {'unit':'m/s',        'description':'Spectral width of radial velocity - vertically-polarized'},
                       'UWND':   {'unit':'m/s',        'description':'Component of wind in x-direction'},
                       'VWND':   {'unit':'m/s',        'description':'Component of wind in y-dirction'},
                       'RSHR':   {'unit':'m/s km',     'description':'Radial shear'},
                       'ASHR':   {'unit':'m/s km',     'description':'Azimuthal shear'},
                       'CSHR':   {'unit':'m/s km',     'description':'Range-azimuthal shear'},
                       'ESHR':   {'unit':'m/s km',     'description':'Elevation shear'},
                       'OSHR':   {'unit':'m/s km',     'description':'Range-elevation shear'},
                       'HSHR':   {'unit':'m/s km',     'description':'Horizontal shear'},
                       'VSHR':   {'unit':'m/s km',     'description':'Vertical shear'},
                       'TSHR':   {'unit':'m/s km',     'description':'Three-dimensional shear'},
                       'BRDR':   {'unit':'',           'description':'1 denotes a border where data from two or more radars meet in composites, otherwise 0'},
                       'QIND':   {'unit':'',           'description':'Spatially analyzed quality indicator, according to OPERA II, normalized to between 0 (poorest quality) to 1 (best quality)'},
                       'CLASS':  {'unit':'',           'description':'Indicates that data are classified and that the classes are specified accrding to the assocated legend object which must be present'}}

inputfile = sys.argv[1]
output_file = sys.argv[2]

# read input file
inputfile_f = h5py.File(inputfile, 'r')

# sp or lp and file time
pulse_type = inputfile.split('.')[-2][-2:]
file_time = inputfile_f['what'].attrs['time'][:4] #inputfile.split('/')[-1].split('_')[0][-4:]

# data might be written to original file late. To see if a scan should belong in the previous volume,
# for data arriving in the first `minutes_late` of a scan time compare elevation to those scans in previous volume
minutes_late = 1
minutes_late_remainder = minutes_late - 1  # i.e. 110500 to 110559 will be checked against 1100

# find what groups we want to put the data in
check_previous = False
if pulse_type == 'lp':
    if 'ldr' in inputfile.split('/')[-1]:
        pulse_group = 'ldr'
        time_group = file_time
    else:
        pulse_group = 'lp'
        time_group = str(int(file_time) - (int(file_time) % 5))
        check_previous = (int(file_time) % 5 == minutes_late_remainder)  
        prev_time_group = str(int(time_group) - 5)
        if int(prev_time_group[-2:]) >= 60:
            prev_time_group = f'{prev_time_group[:-2]}{int(prev_time_group[-2:])-40}'
elif pulse_type == 'sp':
    pulse_group = 'sp'
    time_group = str(int(file_time) - (int(file_time) % 10))
    check_previous = (int(file_time) % 10 == minutes_late_remainder)  
    prev_time_group = str(int(time_group) - 10)
    if int(prev_time_group[-2:]) >= 60:
        prev_time_group = f'{prev_time_group[:-2]}{int(prev_time_group[-2:])-40}'
else:
    msg = f'Unrecognized pulse type - {pulse_type}'
    raise ValueError(msg)

if len(time_group) == 1:
    time_group = f'000{time_group}'
elif len(time_group) == 2:
    time_group = f'00{time_group}'
elif len(time_group) == 3:
    time_group = f'0{time_group}'
    
if pulse_group in ['lp','sp']:
    if int(prev_time_group) >= 0:
        if len(prev_time_group) == 1:
            prev_time_group = f'000{prev_time_group}' 
        elif len(prev_time_group) == 2:
            prev_time_group = f'00{prev_time_group}'
        elif len(prev_time_group) == 3:
            prev_time_group = f'0{prev_time_group}'
    else:
        prev_time_group = '0000'
        check_previous = False

print(f"Incoming File: {inputfile}")
print(f"pulse type: {pulse_type}")
#print(f"file time: {file_time.decode()}")
print(f"file time: {file_time}")
print(f"time group: {time_group}")
if pulse_group in ['lp','sp']:
    print(f"previous time group: {prev_time_group}")
    print(f"checking previous time group: {check_previous}")
print(f"output file: {output_file}")
    
day_f = h5py.File(f"{output_file}", "a")

# create groups if they don't already exist
if pulse_group not in day_f.keys():
    day_f.create_group(pulse_group)
if time_group not in day_f[pulse_group].keys():
    day_f[pulse_group].create_group(time_group)
    for wwh in ['what', 'where', 'how']:
        day_f[pulse_group][time_group].create_group(wwh)
        day_f[pulse_group][time_group].attrs['Conventions'] = "ODIM_H5/V2_1"
        day_f[pulse_group][time_group]['what'].attrs['object'] = 'PVOL'
        day_f[pulse_group][time_group]['what'].attrs['source'] = inputfile_f['what'].attrs['source'] 
if 'variables' not in day_f.keys():
    day_f.create_group('variables')

# get info currently in aggregate file that might need to be updated
if 'quantities' in day_f.attrs.keys():
    quantities = day_f.attrs['quantities']
else:
    quantities = []
if 'startdate' in day_f.attrs.keys():
    curr_startdate = int(day_f.attrs['startdate'])
else:
    curr_startdate = 1e35
if 'enddate' in day_f.attrs.keys():
    curr_enddate = int(day_f.attrs['enddate'])
else:
    curr_enddate = 0
if 'starttime' in day_f.attrs.keys():
    curr_starttime = int(day_f.attrs['starttime'])
else:
    curr_starttime = 1e35
if 'endtime' in day_f.attrs.keys():
    curr_endtime = int(day_f.attrs['endtime'])
else:
    curr_endtime = 0
    
# if necessary, check to see if the elevation of this scan appears in the previous volume, and adjust time_group if needed
if check_previous:
    prev_elevations = day_f[pulse_group][prev_time_group]['where'].attrs['elangles']
    curr_elevation = inputfile_f['dataset1']['where'].attrs['elangle']
    if curr_elevation not in prev_elevations:
        time_group = prev_time_group
        print(f'time group changed to {time_group}')


# count to find the next dataset number
dataset_number = 1
for key in day_f[pulse_group][time_group].keys():
    if 'dataset' in key:
        dataset_number += 1
day_f[pulse_group][time_group].create_group(f'dataset{dataset_number}')

# save original file name as attribute in new file
day_f[pulse_group][time_group][f'dataset{dataset_number}'].attrs['original_filename'] = inputfile.split('/')[-1]

# copy dataset contents to file
for group in inputfile_f['dataset1'].keys():
    inputfile_f.copy(f'dataset1/{group}', day_f[pulse_group][time_group][f'dataset{dataset_number}'])

# update FILE start and end time and quantities
for key in inputfile_f.keys():
    if 'dataset' in key:
        #find start/end date/time for file
        if 'startdate' in inputfile_f[key]['what'].attrs.keys():
            dataset_startdate = int(inputfile_f[key]['what'].attrs['startdate'])
        if 'starttime' in inputfile_f[key]['what'].attrs.keys():
            dataset_starttime = int(inputfile_f[key]['what'].attrs['starttime'])
        if (dataset_startdate < curr_startdate) or ((dataset_startdate == curr_startdate) and (dataset_starttime < curr_starttime)):
            day_f.attrs['startdate'] = inputfile_f[key]['what'].attrs['startdate']
            day_f.attrs['starttime'] = inputfile_f[key]['what'].attrs['starttime']
            
        if 'enddate' in inputfile_f[key]['what'].attrs.keys():
            dataset_enddate = int(inputfile_f[key]['what'].attrs['enddate'])
        if 'endtime' in inputfile_f[key]['what'].attrs.keys():
            dataset_endtime = int(inputfile_f[key]['what'].attrs['endtime'])
        if (dataset_enddate > curr_enddate) or ((dataset_enddate == curr_enddate) and (dataset_endtime > curr_endtime)):
            day_f.attrs['enddate'] = inputfile_f[key]['what'].attrs['enddate']
            day_f.attrs['endtime'] = inputfile_f[key]['what'].attrs['endtime']
            
        #find quantities
        for subkey in inputfile_f[key].keys():
            if 'data' in subkey:
                new_quantity = inputfile_f[key][subkey]['what'].attrs['quantity'].decode()
                #print(f'new_quantity: {new_quantity}')
                if new_quantity not in quantities:
                    print(f'quantity type: {type(quantities)}')
                    try:
                        day_f['variables'].create_group(new_quantity)
                    except:
                        print(day_f['variables'].keys())
                        print(new_quantity)
                        print(quantities)
                        raise
                    if new_quantity in all_odim_quantities.keys():
                        for info in ['unit','description']:
                            day_f['variables'][new_quantity].attrs[info] = all_odim_quantities[new_quantity][info]
                    if type(quantities) == str:
                        quantities = [quantities, new_quantity]
                        print(type(quantities))
                        quantities = ','.join(quantities)
                    elif type(quantities) == list:
                        quantities.append(new_quantity)
                        quantities = ','.join(quantities)
                    else:
                        msg = f'Unexpected type for quantities: {type(quantities)}'
                        raise TypeError(msg)
    

day_f.attrs['quantities'] = quantities


# FILE: lat/lon/height location of radar (assume the radar doesn't move during the day)
if 'lon' not in day_f.attrs.keys():
    day_f.attrs['lon'] = inputfile_f['where'].attrs['lon']
if 'lat' not in day_f.attrs.keys():
    day_f.attrs['lat'] = inputfile_f['where'].attrs['lat']
if 'height' not in day_f.attrs.keys():
    day_f.attrs['height'] = inputfile_f['where'].attrs['height']
    

# VOLUME SPECIFIC, ODIM: copy some attributes over if not present (these ones should be the same for all sp, all lp) 
if 'lon' not in day_f[pulse_group][time_group]['where'].attrs.keys():
    day_f[pulse_group][time_group]['where'].attrs['lon'] = inputfile_f['where'].attrs['lon']
    day_f[pulse_group][time_group]['where'].attrs['lat'] = inputfile_f['where'].attrs['lat']
    day_f[pulse_group][time_group]['where'].attrs['height'] = inputfile_f['where'].attrs['height']
    day_f[pulse_group][time_group]['where'].attrs['source_local_grid_easting'] = inputfile_f['where'].attrs['source_local_grid_easting']
    day_f[pulse_group][time_group]['where'].attrs['source_local_grid_northing'] = inputfile_f['where'].attrs['source_local_grid_northing'] 
    day_f[pulse_group][time_group]['where'].attrs['elangles'] = inputfile_f['dataset1']['where'].attrs['elangle']
    day_f[pulse_group][time_group]['where'].attrs['elangles_map'] = str({f'dataset{dataset_number}': inputfile_f['dataset1']['where'].attrs['elangle']})
    print(f"elangles_map: {day_f[pulse_group][time_group]['where'].attrs['elangles_map']}")
            
    day_f[pulse_group][time_group]['what'].attrs['source_local_site_number'] = inputfile_f['what'].attrs['source_local_site_number']
    day_f[pulse_group][time_group]['what'].attrs['source_wmo_site_number'] = inputfile_f['what'].attrs['source_wmo_site_number']
            
    day_f[pulse_group][time_group]['how'].attrs['beamwidth'] = inputfile_f['how'].attrs['beamwidth']
    day_f[pulse_group][time_group]['how'].attrs['radconstH'] = inputfile_f['how'].attrs['radconstH']
    day_f[pulse_group][time_group]['how'].attrs['radconstV'] = inputfile_f['how'].attrs['radconstV']

else:
    # copy elangle from scan to higher level metadata
    day_f[pulse_group][time_group]['where'].attrs['elangles'] = np.append(day_f[pulse_group][time_group]['where'].attrs['elangles'], inputfile_f['dataset1']['where'].attrs['elangle'])
    current_elangles = ast.literal_eval(day_f[pulse_group][time_group]['where'].attrs['elangles_map'])
    current_elangles[f'dataset{dataset_number}'] = inputfile_f['dataset1']['where'].attrs['elangle']
    day_f[pulse_group][time_group]['where'].attrs['elangles_map'] = str(current_elangles)
    print(f"elangles_map: {day_f[pulse_group][time_group]['where'].attrs['elangles_map']}")
    
    
# VOLUME SPECIFIC, ODIM: set the start date and time for each volume. This needs to be checked with every new incoming file
if 'time' in day_f[pulse_group][time_group]['what'].attrs.keys():
    if inputfile_f['what'].attrs['time'] < day_f[pulse_group][time_group]['what'].attrs['time']:
        day_f[pulse_group][time_group]['what'].attrs['time'] = inputfile_f['what'].attrs['time']
        day_f[pulse_group][time_group]['what'].attrs['date'] = inputfile_f['what'].attrs['date']
else:
    day_f[pulse_group][time_group]['what'].attrs['time'] = inputfile_f['what'].attrs['time']
    day_f[pulse_group][time_group]['what'].attrs['date'] = inputfile_f['what'].attrs['date']

inputfile_f.close()

# close the file
day_f.close()

# I think sometimes the code moves on too quickly, so we get to the next opening of file 
# before we've properly closed it. Putting in this short sleep should help.
sleep(2)
print(' ')