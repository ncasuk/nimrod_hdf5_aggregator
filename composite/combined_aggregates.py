import h5py
import os



def combined(f1, f2, output_file):

    # if not os.path.exists(f2):
    #     with h5py.File(f2, 'w') as combined_file:
    #         for group in f1.keys():
    #             print(group)
    #             f1.copy(group, combined_file)
    
    # Check if the input files exist)
    print(os.path.exists(f1), os.path.exists(f2))
    if not os.path.exists(f1) or not os.path.exists(f2):
        print("One or both input files do not exist.")
        return

    # Open the input files
    with h5py.File(f1, 'r') as file1, h5py.File(f2, 'r') as file2:
        print('Input h5 files opened to read')

        # Create a new .h5 file to store the combined data
        with h5py.File(output_file, 'w') as combined_file:
            # Copy the datasets from the input files to the combined file
            for file in [file1, file2]:
                for group in file.keys():
                   if group not in combined_file:
                    file.copy(group, combined_file)
                # If the group already exists, copy over the attributes if they don't exist already
                else:
                    for attr_name, attr_value in file[group].attrs.items():
                        combined_file[group].attrs[attr_name] = attr_value
                #    for attr_name, attr_value in file[group].attrs.items():
                #         print(f"Attribute name: {attr_name}, Attribute value: {attr_value}")
            print('done')

# Call the function with your file paths
# combined("C:\\Users\\fdq63749\\Desktop\\quarantine\\dd.h5", "C:\\Users\\fdq63749\\Desktop\\quarantine\\temp_20200810.h5", "C:\\Users\\fdq63749\\Desktop\\quarantine\\test.h5")

