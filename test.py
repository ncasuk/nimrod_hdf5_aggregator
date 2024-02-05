from composite import file_to_aggregate_composite, combined_aggregates
import os
import h5py
import re
import shutil
import tempfile


fileDir = 'C:\\Users\\fdq63749\\Desktop\\nimrod_hdf5\\nimrod_hdf5_aggregator\\'
processingDir = 'C:\\Users\\fdq63749\\Desktop\\quarantine\\'
expression = '(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})(?P<hour>[0-9]{2})(?P<minute>[0-9]{2})_ODIM_ng_radar_rainrate_composite_1km_UK.h5$'
pattern = '(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})_ODIM_ng_radar_rainrate_composite_1km_UK.h5$'
dummyArchive = 'C:\\Users\\fdq63749\\Desktop\\badc\\ukmo-nimrod-hdf5\\composite\\uk-1km'


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

for date, files in yearly_files.items():
    files_to_remove = []
    temp_filename_out = os.path.join(os.path.dirname(processingDir), 'temp_' + date + '.h5')
    composite_file_name = os.path.join(os.path.dirname(processingDir), date + '_ODIM_ng_radar_rainrate_composite_1km_UK.h5')
    readyToIngest_path = composite_file_name.replace('quarantine', 'readyToIngest')
    dummyArchive_path = dummyArchive + '\\' + date + '_ODIM_ng_radar_rainrate_composite_1km_UK'
    for file in files:
        time = file[8:12]
        temporary_aggregate_name = os.path.dirname(composite_file_name) + '\\_localcopy' + os.path.basename(composite_file_name)
        if os.path.exists(temporary_aggregate_name):
            with h5py.File(temporary_aggregate_name, 'r') as f:
                if time in f:
                    pass
                else:
                    file_to_aggregate_composite.file_to_aggregate(file, temp_filename_out)
                    files_to_remove.append(file)
        else:
            file_to_aggregate_composite.file_to_aggregate(file, temp_filename_out)
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
    # with h5py.File(temporary_aggregate_name, 'a') as temp_file, h5py.File(composite_file_name, 'r') as comp_file:
    #     common_groups = set(temp_file.keys()).intersection(comp_file.keys())
    #     print(common_groups)
    #     for group in common_groups:
    #         del temp_file[group]
    #     print(temp_file.keys())

    combined_aggregates.combined(temp_filename_out, composite_file_name)
    if not os.path.exists(temporary_aggregate_name):
        shutil.copy2(composite_file_name, temporary_aggregate_name)
    if not dummy:     
        for file in files_to_remove:
            os.remove(file)
        

