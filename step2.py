import os
year = "2021"
scrubber_asn = "13335" # Scrubber asn
path = "/data/shared_dir/ddos_scrubber/as"+scrubber_asn+"/"+year+"/"

# Program to find origin, provider, prefix, its length and version from raw data generated using bgpreader as
# bgpreader -t ribs -w 1485907200,1485907200 -A _32787_ >> /home/shyam/jupy/ddos_scrubber/data/raw_as32787_01_feb_2017.txt
# Use in aruba
# Use python multicore programming feature
import pandas as pd
import csv
from concurrent.futures import ProcessPoolExecutor
import os
import re

print("\nStep 6: Finding customers doing AS path prepending.")
# Function to check and validate the 'as_path' removing AS set or other strings in AS path
def is_valid_as_path(row):
    try:
        # Try converting the as_path to a list of integers
        parts = list(map(int, row['as_path'].split()))
        return True  # Return True if valid
    except ValueError:
        return False  # Return False if invalid


# Define a function to determine path prepending and new_provider
def determine_path_prepending_and_new_provider(row):

    # Ensure as_path is a string and split into a list of ASNs
#     print("AS path is %s" %row['as_path'])
    try:
        as_path = list(map(int, str(row['as_path']).split()))

    except ValueError:
        # Catch ValueError, print error, and skip the invalid path
        return None

    provider = None # Default value set
    if len(as_path) < 2:
        provider = None  # Not enough ASNs in path to determine provider

    origin_as = as_path[-1]  # The last ASN is the origin ASN

    path_prepending = 1 if as_path.count(origin_as) > 1 else 0

#     provider = as_path[-2]  # If all checks fail, return the second ASN by default

    # Check for sequentially repeated ASNs
    repeated_asn = None
    for i in range(len(as_path) - 1, 0, -1):
        if as_path[i-1] == as_path[i]:
            repeated_asn = as_path[i]
        else:
            # If we find an ASN that is not the same as the repeated ASN,
            # and we have found a repeated ASN, return the one before the repeated ASN
            if repeated_asn is not None:
                provider = as_path[i-1]  # This is the upstream provider
            break

    return pd.Series([path_prepending, provider])

pattern = r"unique_optimized_provider_not_as.*\.csv$"  # Match files with format unique_optimized_raw_as32787_*.csv

# Loop through all the .csv files
for filename in os.listdir(path):

    # Read only processed files
    if re.search(pattern, filename):
        day = filename.split('_')[5] # Get day from file name
        mon = filename.split('_')[6] # Get month name from file name
        year = filename.split('_')[-1].split('.')[0]

        date = year + "-" + mon + "-" + day
        df = pd.read_csv(path + "unique_optimized_provider_not_as"+scrubber_asn+"_01_"+mon+"_"+year+".csv")
        # Filter the DataFrame to keep only valid rows
        df = df[df.apply(is_valid_as_path, axis=1)]
        print("For file %s" %filename)

        if len(df) > 0:
            # Apply the function to each row
            df[['path_prepending', 'new_provider']] = df.apply(determine_path_prepending_and_new_provider, axis=1)
            # Print the updated DataFrame to verify results
            df.to_csv(path + "unique_optimized_provider_not_as"+scrubber_asn+"_01_"+mon+"_"+year+"_v2.csv")

            # Find the records of AS path prepending
            condition = (df['path_prepending'] == 1) & (df['new_provider'] == int(scrubber_asn))
            provider_scrubber_as_path_prepend = df.loc[condition]
            output_file = path + "unique_optimized_provider_as" + scrubber_asn +"_path_prepend_01_"+mon+"_"+year+".csv"
            provider_scrubber_as_path_prepend.to_csv(output_file)
            print("%s number of records have AS path prepended and contain AS%s as a provider(not a second last hop in AS path though).\n" %(len(provider_scrubber_as_path_prepend), scrubber_asn))
            print("Records are stored in the file %s.\n" %(output_file))
            print("\n Done for file")
            # Remove original file that contains duplicate records
            os.remove(path + "unique_optimized_provider_not_as"+scrubber_asn+"_01_"+mon+"_"+year+".csv")
print("Step 6: AS path prepending check completed.\n")
