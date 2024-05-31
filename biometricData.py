import os
import pandas as pd
from datetime import datetime, timedelta

# Column Names
columns = ['ID', 'Name', 'DateTime', 'CheckType', 'Column4']

def process_file(file_path):
    # Read the data from the .dat file with specified column names and whitespace delimiter
    data = pd.read_csv(file_path, names=columns, delimiter='\t')
    
    # Convert 'DateTime' column to datetime format
    data['DateTime'] = pd.to_datetime(data['DateTime'])
    
    # Get date and time components
    data['Date'] = data['DateTime'].dt.date
    data['Time'] = data['DateTime'].dt.time
    
    # Group by ID, Name, and Date
    grouped_data = data.groupby(['ID', 'Name', 'Date'])
    
    # Define a custom aggregation function to handle missing "Time Out" values and identify the last check type
    def custom_aggregation(group):
        check_in = group['Time'].iloc[0]  # First check-in time
        check_out = group['Time'].iloc[-1] if len(group) > 1 else pd.NaT  # Last check-out time or NaN if missing
        
        # Check if both check-in and check-out times exist
        if not pd.isnull(check_in) and not pd.isnull(check_out):
            # Calculate time difference between check-in and check-out times
            time_diff = datetime.combine(datetime.min, check_out) - datetime.combine(datetime.min, check_in)
            
            # Check if time difference is at least 3 hours and 45 minutes
            if time_diff >= timedelta(hours=3, minutes=45):
                # Get the last observed check type for the day
                last_check_type = group['CheckType'].iloc[-1]
                return pd.Series([check_in, check_out, last_check_type], index=['CheckIn', 'CheckOut', 'LastCheckType'])
        
        return pd.Series([pd.NaT, pd.NaT, pd.NaT], index=['CheckIn', 'CheckOut', 'LastCheckType'])  # Return NaN if condition not met (Walay data mugawas)
    
    # Apply the custom aggregation function to each group
    check_in_out_data = grouped_data.apply(custom_aggregation).reset_index()
    
    return check_in_out_data, data

def main():
    # Prompt the user to input the folder path
    folder_path = input("Enter the folder path: ")
    
    # Extract the name of the current folder
    folder_name = os.path.basename(folder_path)
    
    # Create an empty DataFrame to store the raw data
    raw_data = pd.DataFrame(columns=columns)
    processed_data = pd.DataFrame(columns=['ID', 'Name', 'Date', 'CheckIn', 'CheckOut', 'LastCheckType'])

    # Iterate over each file in the folder
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        
        # Check the file size (Maybe add the function for checking of special characters here as well?)
        if os.path.isfile(file_path) and os.path.getsize(file_path) >= 1024:  # Check if file size is at least 1 KB
            print("Processing file:", file_name)
            processed, raw = process_file(file_path)
            processed_data = pd.concat([processed_data, processed], ignore_index=True)
            raw_data = pd.concat([raw_data, raw], ignore_index=True)

    # Sort the raw data by ID, Name, and DateTime
    raw_data.sort_values(by=['ID', 'Name', 'DateTime'], inplace=True)

    # Save the data to an Excel file outside the folder path
    parent_folder_path = os.path.abspath(os.path.join(folder_path, os.pardir))
    output_file_path = os.path.join(parent_folder_path, f'{folder_name}_data.xlsx')
    with pd.ExcelWriter(output_file_path) as writer:
        processed_data.to_excel(writer, index=False, sheet_name='Processed Data')
        raw_data.to_excel(writer, index=False, sheet_name='Raw Data')

    print(f"Data saved to Excel successfully at {output_file_path}.")

if __name__ == "__main__":
    main()
