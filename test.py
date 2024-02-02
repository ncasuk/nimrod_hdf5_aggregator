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

yearly_files = {}
for root, dirs, files in os.walk(fileDir):
    for file in files:
        if re.match(expression, file):
            year = file[:8]
            if year not in yearly_files:
                yearly_files[year] = []

            yearly_files[year].append(file)

# TODO: rename variable name when done

for year, files in yearly_files.items():
    temp_filename_out = os.path.join(os.path.dirname(processingDir), 'temp_' + year + '.h5')
    for file in files:
        file_to_aggregate_composite.file_to_aggregate(file, temp_filename_out)
        # os.remove(file)
    composite_file_name = os.path.join(os.path.dirname(processingDir), year + '_ODIM_ng_radar_rainrate_composite_1km_UK.h5')
    readyToIngest_path = composite_file_name.replace('quarantine', 'readyToIngest')
    dummyArchive_path = dummyArchive + '\\' + year + '_ODIM_ng_radar_rainrate_composite_1km_UK'
    if os.path.exists(composite_file_name):
        combined_aggregates.combined(temp_filename_out, composite_file_name, "C:\\Users\\fdq63749\\Desktop\\quarantine\\test.h5")
    elif os.path.exists(readyToIngest_path):
        combined_aggregates.combined(temp_filename_out, readyToIngest_path)
    elif os.path.exists(dummyArchive_path):
        combined_aggregates.combined(temp_filename_out, dummyArchive_path)
    else:
        combined_aggregates.combined(temp_filename_out, composite_file_name, "C:\\Users\\fdq63749\\Desktop\\quarantine\\test.h5")

# for year, files in yearly_files.items():
#     for file in files:
#         year_date_time = file[:8]
#         composite_file_name = processingDir + year_date_time + '_ODIM_ng_radar_rainrate_composite_1km_UK.h5'
#         readyToIngest_path = composite_file_name.replace('quarantine', 'readyToIngest')
#         dummyArchive_path = dummyArchive + '\\' + year_date_time + '_ODIM_ng_radar_rainrate_composite_1km_UK.h5'
#         if os.path.exists(composite_file_name):
#             temp_filename_out = os.path.join(os.path.dirname(composite_file_name), 'temp_' + os.path.basename(composite_file_name))
#             shutil.copy2(composite_file_name, temp_filename_out)        
#             file_to_aggregate_composite.file_to_aggregate(file, composite_file_name)
#             os.remove(file)
#         elif os.path.exists(readyToIngest_path):
#             temp_filename_out = os.path.join(os.path.dirname(readyToIngest_path), 'temp_' + os.path.basename(composite_file_name))
#             shutil.copy2(readyToIngest_path, temp_filename_out)
#             file_to_aggregate_composite.file_to_aggregate(file, readyToIngest_path)
#             os.remove(file)
#         elif os.path.exists(dummyArchive_path):
#             temp_filename_out = os.path.join(os.path.dirname(dummyArchive_path), 'temp_' + os.path.basename(composite_file_name))
#             shutil.copy2(dummyArchive_path, temp_filename_out)
#             file_to_aggregate_composite.file_to_aggregate(file, dummyArchive_path)
#             os.remove(file)
#         else:
#             file_to_aggregate_composite.file_to_aggregate(file, composite_file_name)
#             os.remove(file)



# for year, files in yearly_files:
#     for h5file in files:
#         file = fileDir + h5file
#         year_date_time = h5file[:8]
#         compositefile = processingDir + year_date_time + '_ODIM_ng_radar_rainrate_composite_1km_UK.h5'

#         # Get a temporary output filename
#         temp_filename_out = os.path.join(os.path.dirname(compositefile), 'temp_' + os.path.basename(compositefile))

#         if os.path.exists(compositefile):
#             # Copy the composite file to the temporary output filename
#             file_to_aggregate_composite.file_to_aggregate(file, compositefile)
#             shutil.copy2(compositefile, temp_filename_out)
#         else:
#             readyToIngest_path = compositefile.replace('quarantine', 'readyToIngest')
#             if os.path.exists(readyToIngest_path):
#                 # Copy the file from the 'readyToIngest' area to the temporary output filename
#                 shutil.copy2(readyToIngest_path, temp_filename_out)
#                 file_to_aggregate_composite.file_to_aggregate(file, readyToIngest_path)
#             else:
#                 dummyArchive_path = dummyArchive + '\\' + year_date_time + '_ODIM_ng_radar_rainrate_composite_1km_UK.h5'
#                 if os.path.exists(dummyArchive_path):
#                     # Copy the file from the dummy archive to the temporary output filename
#                     shutil.copy2(dummyArchive_path, temp_filename_out)
#                     file_to_aggregate_composite.file_to_aggregate(file, dummyArchive_path)
#         # Continue with your processing... may have misunderstood WHERE to do this, go back over with graham but does what we want
