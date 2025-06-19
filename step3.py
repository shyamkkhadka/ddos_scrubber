import os
year = "2021"
scrubber_asn = "13335" # Scrubber asn
path = "/data/shared_dir/ddos_scrubber/as"+scrubber_asn+"/"+year+"/"


print("Step 7: Finding records that were not originated by the scrubber and contain scrubber as the second last hop.")

# Program to find siblings of an ASN based on CAIDA AS2Org data
# Read line from 95721 from the file /h # format:aut|changed|aut_name|org_id|opaque_id|source
import gzip
import json

# Function to find siblings of a given ASN in a .jsonl.gz file, starting from a specific line
def find_siblings(asn_to_find):
    # Create a dictionary to group ASNs by organizationId
    org_id_map = {}

    file_path = path + '20241001.as-org2info.jsonl.gz'

    start_line = 95721

    # Open and read the .jsonl.gz file
    with gzip.open(file_path, 'rt') as f:  # 'rt' is for reading text mode
        for current_line, line in enumerate(f):
            if current_line < start_line:
                continue  # Skip lines until reaching the desired start_line

            record = json.loads(line)  # Parse each line as JSON

            asn = record['asn']
            organization_id = record['organizationId']

            if organization_id not in org_id_map:
                org_id_map[organization_id] = []
            org_id_map[organization_id].append(asn)

    # Find the organizationId of the given ASN
    org_id_of_asn = None
    for org_id, asns in org_id_map.items():
        if asn_to_find in asns:
            org_id_of_asn = org_id
            break

    # If ASN is not found, return an empty list
    if org_id_of_asn is None:
        return []

    # Return all ASNs that share the same organizationId, excluding the given ASN
    siblings = [asn for asn in org_id_map[org_id_of_asn] if asn != asn_to_find]
    return siblings

# Test the function with a .jsonl.gz file, starting from line 10
# asn_to_find = "23752"
# siblings = find_siblings(asn_to_find)
# print(f"Siblings of ASN {asn_to_find}: {siblings}")

import pandas as pd
import re
import os

from concurrent.futures import ProcessPoolExecutor

# Function to determine sibling ASes and new provider
def determine_sibling_ases_and_new_provider(row):
    # Ensure as_path is a string and split into a list of ASNs
    as_path = list(map(int, str(row['as_path']).split()))

    # If as_path is too short to determine provider, return default values
    if len(as_path) < 2:
        return (0, None)  # No siblings, no provider

    origin_as = as_path[-1]  # The last ASN is the origin ASN
    provider = as_path[-2]  # Default provider is the second-to-last ASN

    # Default sibling count
    siblings = 0

    # Retrieve sibling list from an API (or mock it here for testing)
    try:
        sibling_list = find_siblings(str(origin_as))
    except Exception as e:
        print(f"Error retrieving siblings: {e}")
        return (0, None)  # No siblings, no provider in case of API failure

    # Traverse the AS path to identify a non-sibling provider
    for i in range(len(as_path) - 1, 0, -1):
        if str(as_path[i-1]) not in sibling_list:
            provider = as_path[i-1]
            break

    # Determine if there are any siblings in the AS path
    as_path_str = list(map(str, as_path))  # Convert AS path to strings for comparison
    siblings = 1 if len(set(as_path_str) & set(sibling_list)) > 0 else 0

    return (siblings, provider)  # Return as tuple

# Function to process a chunk of the DataFrame
def process_chunk(chunk):
    # Apply the function and collect results
    results = chunk.apply(determine_sibling_ases_and_new_provider, axis=1)

    # Convert results to DataFrame with correct columns
    results_df = pd.DataFrame(results.tolist(), columns=['siblings', 'new_provider_sibling_check'], index=chunk.index)

    return results_df


# Loop through the input files
pattern = r"unique_optimized_provider_not_as"+scrubber_asn+".*\_v2.csv$"  # Match files with format raw_as32787_*.txt

for filename in os.listdir(path):
     # Read only processed files
    if re.search(pattern, filename):
        day = filename.split('_')[5] # Get day from file name
        mon = filename.split('_')[6] # Get month name from file name
        year = filename.split('_')[7]
        df = pd.read_csv(path + "unique_optimized_provider_not_as"+scrubber_asn+"_"+day+"_"+mon+"_"+year+"_v2.csv")
        df = df.loc[(df['path_prepending'] == 0 ) & (df['origin_as'] != int(scrubber_asn))]
        print(f"{len(df)} records found with AS path prepending = 0 and provider not AS {scrubber_asn} for filename {filename}.\n")

        if len(df) > 0: # Only if there are rows without origin prepending its path.
            # Define chunk size and split data into chunks for parallel processing
            chunk_size = 1000  # Adjust based on available CPU cores and memory
            chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

            # Set the number of workers (processors)
            num_workers = 100  # Adjust this number based on your CPU

            # Use ProcessPoolExecutor for parallel processing
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                # Map the function over each chunk and gather results
                results = list(executor.map(process_chunk, chunks))

            # Combine processed chunks into a single DataFrame with reset index
            processed_chunks = pd.concat(results)

            # Assign processed data to the original DataFrame columns
            df[['siblings', 'new_provider_sibling_check']] = processed_chunks

            # Print final output or save to file
            df.to_csv(path + "unique_optimized_provider_not_as"+scrubber_asn+"_"+day+"_"+mon+"_"+year+"_v3.csv", index=False)
print("Step 7: Completed checking siblings of origin AS.")

