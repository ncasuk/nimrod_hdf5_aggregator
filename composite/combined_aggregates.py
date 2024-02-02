import h5py
import os


def combined(f1, f2):

    if not os.path.exists(f2):
        with h5py.File(f1, 'r') as file1, h5py.File(f2, 'w') as file2:
            for group in file1.keys():
                print(group)
                file1.copy(group, file2)
        os.remove(f1)
        return
    
    # Check if the input files exist)
    if not os.path.exists(f1) or not os.path.exists(f2):
        print("One or both input files do not exist.")
        return

    # Open the input files
    with h5py.File(f1, 'r') as file1, h5py.File(f2, 'a') as file2:

        # Copy the datasets from file1 to file2
        for group in file1.keys():
            if group not in file2:
                file1.copy(group, file2)
            # If the group already exists, copy over the attributes if they don't exist already
            else:
                for attr_name, attr_value in file1[group].attrs.items():
                    file2[group].attrs[attr_name] = attr_value
    os.remove(f1)
    return

# Call the function with your file paths
# combined("C:\\Users\\fdq63749\\Desktop\\quarantine\\dd.h5", "C:\\Users\\fdq63749\\Desktop\\quarantine\\temp_20200810.h5", "C:\\Users\\fdq63749\\Desktop\\quarantine\\test.h5")

