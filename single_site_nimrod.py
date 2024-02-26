from composite import combined_aggregates
from single_site import file_to_aggregate
import os
import h5py
import re
import shutil
import tempfile

# 202007170002_polar_pl_radar20b3_augzdr_lp


fileDir = 'C:\\Users\\fdq63749\\Desktop\\nimrod_hdf5\\nimrod_hdf5_aggregator\\singlesitedata'
processingDir = 'C:\\Users\\fdq63749\\Desktop\\quarantine\\'
expression = '(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})(?P<hour>[0-9]{2})(?P<minute>[0-9]{2})_polar_pl_radar\d+b\d+_augzdr_lp\.h5$'
pattern = '(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})(?P<hour>[0-9]{2})(?P<minute>[0-9]{2})_polar_pl_radar\d+b\d+_augzdr_lp\.h5$'
dummyArchive = 'C:\\Users\\fdq63749\\Desktop\\badc\\ukmo-nimrod-hdf5\\single_site\\uk-1km'


dummy = True

yearly_files = {}
for root, dirs, files in os.walk(fileDir):
    for file in files:
        if re.match(expression, file):
            year = file[:8]
            if year not in yearly_files:
                yearly_files[year] = []

            yearly_files[year].append(file)

# TODO: rename variable name when done
            
# set difference?




for date, files in yearly_files.items():
    files_to_remove = []
    temp_filename_out = os.path.join(os.path.dirname(processingDir), 'temp_' + date + '.h5')
    composite_file_name = os.path.join(os.path.dirname(processingDir), date + '_polar_pl_radar.h5')
    readyToIngest_path = composite_file_name.replace('quarantine', 'readyToIngest')
    dummyArchive_path = dummyArchive + '\\' + date + '_polar_pl_radar'
    temporary_aggregate_name = os.path.dirname(composite_file_name) + '\\_localcopy' + os.path.basename(composite_file_name)

    if os.path.exists(temporary_aggregate_name):
        with h5py.File(temporary_aggregate_name, 'r') as f:
            groups = f.keys()
            for file in files:
                time = file[8:12]
                if time in groups:
                        pass
                else:
                    file_to_aggregate.single_site_aggregation(file, temp_filename_out)
                    files_to_remove.append(file)
    else:
        for file in files:
            file = 'C:\\Users\\fdq63749\\Desktop\\nimrod_hdf5\\nimrod_hdf5_aggregator\\singlesitedata\\' + file
            file_to_aggregate.single_site_aggregation(file, temp_filename_out)
            files_to_remove.append(file)

    if os.path.exists(composite_file_name):
        try:
            shutil.copy2(composite_file_name,  temporary_aggregate_name)
        except Exception as e:
            print(f"Error copying {composite_file_name} to {temporary_aggregate_name}: {e}")
    elif os.path.exists(readyToIngest_path):
        shutil.copy2(readyToIngest_path, temporary_aggregate_name)
    elif os.path.exists(dummyArchive_path):
        shutil.copy2(dummyArchive_path, temporary_aggregate_name)

    combined_aggregates.combined(temp_filename_out, composite_file_name)
    if not os.path.exists(temporary_aggregate_name):
        shutil.copy2(composite_file_name, temporary_aggregate_name)
    if not dummy:     
        for file in files_to_remove:
            os.remove(file)
        

