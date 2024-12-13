import os
import pandas as pd

# Path to the directory containing the .pkl files
directory_path = "artifacts"  # Replace with your actual path

# Iterate through all files in the directory
for filename in os.listdir(directory_path):
    if filename.endswith(".pkl"):
        filepath = os.path.join(directory_path, filename)
        
        # Try to load the .pkl file
        try:
            # Read the .pkl file
            data = pd.read_pickle(filepath)
            
            # Ensure it's a DataFrame before converting
            if isinstance(data, pd.DataFrame):
                csv_filename = filename.replace(".pkl", ".csv")
                csv_filepath = os.path.join(directory_path, csv_filename)
                
                # Save the DataFrame to a .csv file
                data.to_csv(csv_filepath, index=False)
                print(f"Converted {filename} to {csv_filename}")
            else:
                print(f"Skipping {filename}: Not a DataFrame")
        except Exception as e:
            print(f"Error processing {filename}: {e}")
