import h5py
import numpy as np
import datetime
from time import sleep
import ast
import sys

def file_to_aggregate(input_file, output_file):
    # if any quantities have ' in them, there will be trouble
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

    infile_f = h5py.File(input_file, 'r')
    time = int(infile_f['what'].attrs['time'][:4])
    seconds = int(infile_f['what'].attrs['time'][4:])

    #if (seconds != 0) or ((time % 5) != 0):
    #    msg = f"Unexpected time, not minute multiple of 5 - {infile_f['what'].attrs['time']}"
    #    raise ValueError(msg)
        
    if time < 10:
        str_time = f'000{time}'
    elif time < 100:
        str_time = f'00{time}'
    elif time < 1000:
        str_time = f'0{time}'
    else:
        str_time = str(time)

    outfile_f = h5py.File(output_file, 'a')
    outfile_f.create_group(str_time)    ######## Need to add a check really, if a file arrives with the same time as a previous one, currently this will error
                                        ######## Question is, should it error and quit, or should it check for changes/updates? Maybe a flag is needed,
                                        ######## for example (sys.argv[3]?), which if present then the previous group for this time should be overwritten.

    if 'Conventions' not in outfile_f.attrs.keys():
        outfile_f.attrs['Conventions'] = 'ODIM v of files, ncas-odims/v1_0'

    #if 'quantities' not in outfile_f.keys():
    #    outfile_f.create_group('quantities')
        
    if 'what' not in outfile_f.keys():
        outfile_f.create_group('what')
        outfile_f['what'].attrs['title'] = 'Title'
        outfile_f['what'].attrs['product'] = 'PROD'
        outfile_f['what'].attrs['object'] = 'OBJ'
        outfile_f['what'].attrs['comment'] = 'This is what the file is all about.'
        
    if 'where' not in outfile_f.keys():
        outfile_f.create_group('where')
        outfile_f['where'].attrs['bbox_comment'] = 'Bounding box values given as enclosing bounding box for all data within this aggregate file'
        
    if 'how' not in outfile_f.keys():
        outfile_f.create_group('how')
        outfile_f['how'].attrs['created'] = datetime.datetime.now().strftime('%Y%m%dT%H%M%S')
        outfile_f['how'].attrs['creator_name'] = 'Met Office'
        outfile_f['how'].attrs['creator_email'] = 'enquiries@metoffice.gov.uk'
        outfile_f['how'].attrs['history'] = 'In fourteen hundred and ninety two Columbus sailed the ocean blue'
        outfile_f['how'].attrs['institution'] = 'Met Office'
        outfile_f['how'].attrs['licence'] = 'http://artefacts.ceda.ac.uk/licences/specific_licences/nerc-met-office_agreement.pdf'
        outfile_f['how'].attrs['processing_software'] = 'Some super-duper code'
        outfile_f['how'].attrs['processing_software_version'] = '0.1.0'
        outfile_f['how'].attrs['project_principle_investigator'] = 'Met Office'
        outfile_f['how'].attrs['project_principle_investigator_contact'] = 'enquiries@metoffice.gov.uk'
        outfile_f['how'].attrs['references'] = 'Some sort of document that describes life and all we know'
        

    for group in infile_f.keys():
        infile_f.copy(f'{group}', outfile_f[str_time])
        
        
    outfile_f[str_time].attrs['original_filename'] = input_file.split('/')[-1]


    #####################################################################################
    #####################################################################################
    #####################################################################################
    #####################################################################################
    #####################################################################################
    if 'quantity' in outfile_f['what'].attrs.keys():
        quantities = outfile_f['what'].attrs['quantity'][1:-1]  # [1:-1] to remove {}
    else:
        #quantities = []
        quantities = ''
        
    only_quantities = quantities.split("':{'")
    for i, q in enumerate(only_quantities):
        only_quantities[i] = q.split("'")[-1]  # if any quantities have ' in them, this will break

        
    if 'node_details' in outfile_f['how'].attrs.keys():
        node_details = outfile_f['how'].attrs['node_details'][1:-1]  # [1:-1] to remove {}
    else:
        node_details = ''

        
    if 'startdate' in outfile_f['what'].attrs.keys():
        curr_startdate = int(outfile_f['what'].attrs['startdate'])
    else:
        curr_startdate = 1e35
        
    if 'enddate' in outfile_f['what'].attrs.keys():
        curr_enddate = int(outfile_f['what'].attrs['enddate'])
    else:
        curr_enddate = 0
        
    if 'starttime' in outfile_f['what'].attrs.keys():
        curr_starttime = int(outfile_f['what'].attrs['starttime'])
    else:
        curr_starttime = 1e35
        
    if 'endtime' in outfile_f['what'].attrs.keys():
        curr_endtime = int(outfile_f['what'].attrs['endtime'])
    else:
        curr_endtime = 0

        
    #print(quantities)
    #print(type(quantities))

    for key in infile_f.keys():
        if 'dataset' in key:
            #find start/end date/time
            if 'startdate' in infile_f[key]['what'].attrs.keys():
                dataset_startdate = int(infile_f[key]['what'].attrs['startdate'])
            if 'starttime' in infile_f[key]['what'].attrs.keys():
                dataset_starttime = int(infile_f[key]['what'].attrs['starttime'])
            if (dataset_startdate < curr_startdate) or ((dataset_startdate == curr_startdate) and (dataset_starttime < curr_starttime)):
                outfile_f['what'].attrs['startdate'] = infile_f[key]['what'].attrs['startdate']
                outfile_f['what'].attrs['starttime'] = infile_f[key]['what'].attrs['starttime']
                
            if 'enddate' in infile_f[key]['what'].attrs.keys():
                dataset_enddate = int(infile_f[key]['what'].attrs['enddate'])
            if 'endtime' in infile_f[key]['what'].attrs.keys():
                dataset_endtime = int(infile_f[key]['what'].attrs['endtime'])
            if (dataset_enddate > curr_enddate) or ((dataset_enddate == curr_enddate) and (dataset_endtime > curr_endtime)):
                outfile_f['what'].attrs['enddate'] = infile_f[key]['what'].attrs['enddate']
                outfile_f['what'].attrs['endtime'] = infile_f[key]['what'].attrs['endtime']

    #####################################################################################
    #####################################################################################
    #####################################################################################
    #####################################################################################
    #####################################################################################            
            #find quantities
            for subkey in infile_f[key].keys():
                if 'data' in subkey:
                    #new_quantity = infile_f[key][subkey]['what'].attrs['quantity']
                    new_quantity = infile_f[key][subkey]['what'].attrs['quantity'].decode()
                    #print(f'new_quantity: {new_quantity}')
                    '''
                    if new_quantity not in quantities:
                        outfile_f['quantities'].create_group(new_quantity)
                        if new_quantity in all_odim_quantities.keys():
                            for info in ['unit','description']:
                                outfile_f['quantities'][new_quantity].attrs[info] = all_odim_quantities[new_quantity][info]
                        if type(quantities) == str:
                            quantities = [quantities, new_quantity]
                        elif type(quantities) == list:
                            quantities.append(new_quantity)
                        else:
                            msg = f'Unexpected type for quantities: {type(quantities)}'
                            raise TypeError(msg)
                    '''
                    if new_quantity not in only_quantities:
                        if new_quantity in all_odim_quantities.keys():
                            quantities += f"'{new_quantity}':{{'unit':{all_odim_quantities[new_quantity]['unit']},'description':'{all_odim_quantities[new_quantity]['description']}'}},"
                        else:
                            quantities += f"'{new_quantity}':{{'unit':'unknown','description':'unknown'}},"
                        
                        
    '''
    #print(quantities)
    if type(quantities) == list:
        quantities = ','.join(quantities)

    outfile_f.attrs['quantities'] = quantities
    '''
    if quantities[-1] == ',':
        quantities = quantities[:-1]
    quantities = f'{{{quantities}}}'

    outfile_f['what'].attrs['quantity'] = quantities



    infile_sources = infile_f['what'].attrs['source']
    infile_sources_str = infile_sources.decode()

    #print(infile_sources)
    #print(infile_sources_str)

    if 'source' not in outfile_f['what'].attrs.keys():
        #print('[ ]')
        outfile_f['what'].attrs['source'] = infile_sources_str
    else:
        current_sources = outfile_f['what'].attrs['source']
        #print(current_sources)
        current_sources_list = current_sources.split(',')
        #print(current_sources_list)
        infile_sources_list = infile_sources_str.split(',')
        #print(infile_sources_list)
        for infile_source in infile_sources_list:
            if infile_source not in current_sources_list:
                current_sources_list.append(infile_source)
        #print(current_sources_list)
        current_sources_str = ','.join(current_sources_list)
        #print(current_sources_str)
        outfile_f['what'].attrs['source'] = current_sources_str



    infile_nodes = infile_f['how'].attrs['nodes']
    infile_nodes_str = infile_nodes.decode()
    infile_nodes_list = infile_nodes_str.split(',')

    #print(infile_nodes)
    #print(infile_nodes_str)

    #####################################################################################
    #####################################################################################
    #####################################################################################
    #####################################################################################
    #####################################################################################
    #####################################################################################
    #####################################################################################
    '''
    if 'all_nodes' not in outfile_f.attrs.keys():
        #print('[ ]')
        outfile_f.attrs['all_nodes'] = infile_nodes_str
    else:
        current_nodes = outfile_f.attrs['all_nodes']
        #print(current_nodes)
        current_nodes_list = current_nodes.split(',')
        #print(current_nodes_list)
        infile_nodes_list = infile_nodes_str.split(',')
        #print(infile_nodes_list)
        for infile_node in infile_nodes_list:
            if infile_node not in current_nodes_list:
                current_nodes_list.append(infile_node)
        #print(current_nodes_list)
        current_nodes_str = ','.join(current_nodes_list)
        #print(current_nodes_str)
        outfile_f.attrs['all_nodes'] = current_nodes_str
        
    quantities += f"'{new_quantity}':{{'unit':{all_odim_quantities[new_quantity]['unit']},'description':{all_odim_quantities[new_quantity]['description']}}},"
    '''

    for infile_node in infile_nodes_list:
        if infile_node not in node_details:
            node_details += f"'{infile_node}':{{'identifier_set':{{'identifier_type':'id'}},'site_name':'site_name','lat':'lat', 'lon':'lon', 'height':'antenna height AMSL'}},"
            #outfile_f['how'].attrs['node_details'][infile_node] = {'identifier_set':{'identifier_type':'id'},'site_name':'site_name','lat':'lat', 'lon':'lon', 'height':'antenna height AMSL'}

    if node_details[-1] == ',':
        node_details = node_details[:-1]
    node_details = f'{{{node_details}}}'

    outfile_f['how'].attrs['node_details'] = node_details



    '''    
    if 'LL_lat' not in outfile_f['where'].attrs.keys() and 'LL_lat' in infile_f['where'].attrs.keys():
        outfile_f['where'].attrs['LL_lat'] = infile_f['where'].attrs['LL_lat']    
    if 'LL_lon' not in outfile_f['where'].attrs.keys() and 'LL_lon' in infile_f['where'].attrs.keys():
        outfile_f['where'].attrs['LL_lon'] = infile_f['where'].attrs['LL_lon']    
    if 'LR_lat' not in outfile_f['where'].attrs.keys() and 'LR_lat' in infile_f['where'].attrs.keys():
        outfile_f['where'].attrs['LR_lat'] = infile_f['where'].attrs['LR_lat']    
    if 'LR_lon' not in outfile_f['where'].attrs.keys() and 'LR_lon' in infile_f['where'].attrs.keys():
        outfile_f['where'].attrs['LR_lon'] = infile_f['where'].attrs['LR_lon']    
    if 'UL_lat' not in outfile_f['where'].attrs.keys() and 'UL_lat' in infile_f['where'].attrs.keys():
        outfile_f['where'].attrs['UL_lat'] = infile_f['where'].attrs['UL_lat']    
    if 'UL_lon' not in outfile_f['where'].attrs.keys() and 'UL_lon' in infile_f['where'].attrs.keys():
        outfile_f['where'].attrs['UL_lon'] = infile_f['where'].attrs['UL_lon']    
    if 'UR_lat' not in outfile_f['where'].attrs.keys() and 'UR_lat' in infile_f['where'].attrs.keys():
        outfile_f['where'].attrs['UR_lat'] = infile_f['where'].attrs['UR_lat']    
    if 'UR_lon' not in outfile_f['where'].attrs.keys() and 'UR_lon' in infile_f['where'].attrs.keys():
        outfile_f['where'].attrs['UR_lon'] = infile_f['where'].attrs['UR_lon']
    '''

    # for each corner, check if already in outfile, and if so check if more extreme
    # NOTE: assumption longitude is in range -180 to 180, not 0 to 360
    if 'LL_lat' in infile_f['where'].attrs.keys():
        if ('LL_lat' not in outfile_f['where'].attrs.keys()) or (float(infile_f['where'].attrs['LL_lat']) < float(outfile_f['where'].attrs['LL_lat'])):
            outfile_f['where'].attrs['LL_lat'] = infile_f['where'].attrs['LL_lat']
    if 'LL_lon' in infile_f['where'].attrs.keys():
        if ('LL_lon' not in outfile_f['where'].attrs.keys()) or (float(infile_f['where'].attrs['LL_lon']) < float(outfile_f['where'].attrs['LL_lon'])):
            outfile_f['where'].attrs['LL_lon'] = infile_f['where'].attrs['LL_lon']

    if 'LR_lat' in infile_f['where'].attrs.keys():
        if ('LR_lat' not in outfile_f['where'].attrs.keys()) or (float(infile_f['where'].attrs['LR_lat']) < float(outfile_f['where'].attrs['LR_lat'])):
            outfile_f['where'].attrs['LR_lat'] = infile_f['where'].attrs['LR_lat']
    if 'LR_lon' in infile_f['where'].attrs.keys():
        if ('LR_lon' not in outfile_f['where'].attrs.keys()) or (float(infile_f['where'].attrs['LR_lon']) > float(outfile_f['where'].attrs['LR_lon'])):
            outfile_f['where'].attrs['LR_lon'] = infile_f['where'].attrs['LR_lon']
            
    if 'UL_lat' in infile_f['where'].attrs.keys():
        if ('UL_lat' not in outfile_f['where'].attrs.keys()) or (float(infile_f['where'].attrs['UL_lat']) > float(outfile_f['where'].attrs['UL_lat'])):
            outfile_f['where'].attrs['UL_lat'] = infile_f['where'].attrs['UL_lat']
    if 'UL_lon' in infile_f['where'].attrs.keys():
        if ('UL_lon' not in outfile_f['where'].attrs.keys()) or (float(infile_f['where'].attrs['UL_lon']) < float(outfile_f['where'].attrs['UL_lon'])):
            outfile_f['where'].attrs['UL_lon'] = infile_f['where'].attrs['UL_lon']

    if 'UR_lat' in infile_f['where'].attrs.keys():
        if ('UR_lat' not in outfile_f['where'].attrs.keys()) or (float(infile_f['where'].attrs['UR_lat']) > float(outfile_f['where'].attrs['UR_lat'])):
            outfile_f['where'].attrs['UR_lat'] = infile_f['where'].attrs['UR_lat']
    if 'UR_lon' in infile_f['where'].attrs.keys():
        if ('UR_lon' not in outfile_f['where'].attrs.keys()) or (float(infile_f['where'].attrs['UR_lon']) > float(outfile_f['where'].attrs['UR_lon'])):
            outfile_f['where'].attrs['UR_lon'] = infile_f['where'].attrs['UR_lon']
        
    infile_f.close()
    outfile_f['how'].attrs['last_revised_date'] = datetime.datetime.now().strftime('%Y%m%dT%H%M%S')
    outfile_f.close()
    del infile_f
    del outfile_f
    sleep(1)
    print('\n')