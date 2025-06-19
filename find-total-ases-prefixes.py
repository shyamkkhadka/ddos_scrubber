# Program to find origin, provider, prefix, its length and version from raw data generated using bgpreader as 
# bgpreader -t ribs -w 1704067200,1704067200 -A _32787_ >> raw_as13335_01_jan_2024.txt
# Use python multicore programming feature
year = "2024"
day = "01"
path = "/data/shared_dir/ddos_scrubber/"

# months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
months = ["jun", "jul", "aug", "sep", "oct", "nov", "dec"]

import pandas as pd
import csv
from concurrent.futures import ProcessPoolExecutor

def process_chunk(lines):
    """Process a chunk of lines and extract prefix, AS path, and origin AS."""
    chunk_data = []
    for line in lines:
        fields = line.strip().split('|')
        if len(fields) > 12 and fields[1] == "R":
            try:
               # Extract prefix, AS path, and origin AS
                prefix = fields[9]
                as_path = fields[11].split()


                # Find provider here checking AS repetetitions and the same organization owning multiple ASes
#                 provider = find_immediate_provider(as_path)
                
                provider = as_path[-2] if len(as_path) > 1 else None  # The ASN before the origin AS
                origin_as = as_path[-1] if as_path else None
                
                pfx_len = int(prefix.split('/')[1]) if '/' in prefix else None
                ip_version = "IPv6" if ':' in prefix else "IPv4"

                # Append data to the chunk
                chunk_data.append([prefix, ' '.join(as_path), origin_as, provider, pfx_len, ip_version])
            
            except IndexError:
                continue
    return chunk_data

def process_route_data_parallel(file_path, output_file, num_workers=100, chunk_size=100000):
    with open(file_path, 'r') as file:
        lines = []
        
        # Initialize CSV output
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['prefix', 'as_path', 'origin_as', 'provider', 'pfx_len', 'ip_version'])

        # Process the file in chunks with multiple workers
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            for line in file:
                lines.append(line)
                
                # When lines reach chunk size, process them
                if len(lines) >= chunk_size:
                    futures.append(executor.submit(process_chunk, lines))
                    lines = []

            # Process any remaining lines after the loop
            if lines:
                futures.append(executor.submit(process_chunk, lines))

            # Collect results from all futures and write to CSV
            for future in futures:
                chunk_data = future.result()
                if chunk_data:
                    df = pd.DataFrame(chunk_data, columns=['prefix', 'as_path', 'origin_as', 'provider', 'pfx_len', 'ip_version'])
                    df.to_csv(output_file, mode='a', header=False, index=False)

    print(f"Data successfully saved to {output_file}")

for mon in months:
	
	# Example usage
	file_path = path + 'raw_'+day+'_'+ mon+'_'+year+'.txt'
	output_file = path + 'optimized_raw_'+day+'_'+mon+'_'+year+'.csv'
	process_route_data_parallel(file_path, output_file)

	df = pd.read_csv(path + 'optimized_raw_'+day+'_'+mon+'_'+year+'.csv', low_memory=False)
	 
	# Remove duplicate rows in dataframe
	df = df.drop_duplicates()

	# Write unique rows into a file
	df.to_csv(path + 'unique_optimized_raw_'+day+'_'+mon+'_'+year+'.csv', index=False)
	print("Completed.")

	# Remove records containing AS set on an AS path
	print("Records before %s." %len(df)) 

	print("Removing AS set in an AS path")
	# Find rows containing '{}' (set origins)
	mask = df['as_path'].str.contains(r'\{.*\}', na=False)
	rows_with_origin_set = df[mask]

	# Count rows with set origin
	count_set_origin = rows_with_origin_set.shape[0]

	# Remove rows with set origin from the DataFrame
	df_cleaned = df[~mask]

	# Remove rows with set origin from the DataFrame
	df_cleaned = df[~mask]

	df_cleaned.to_csv(path + 'unique_optimized_raw_'+day+'_'+mon+'_'+year+'.csv', index = False)
	print("Number of rows with set origin:", count_set_origin)
	print("DataFrame after removal:")
	print("Records after %s." %len(df_cleaned)) 
	print("%s number of records were removed that contains AS set in AS paths." %(len(df) - len(df_cleaned)))

	# Split the AS path by spaces and then flatten the list
	asns = df_cleaned['as_path'].str.split().explode().unique()


	headers = ["year", "mon", "day", "no_customer_ases"] 
	all_record_ases = [[year, mon, day, len(asns)]]
	filename_ases = path + "final_ases_2017_2024.csv"

	with open(filename_ases, 'a') as f:
	    write = csv.writer(f)
	    write.writerow(headers)
	    write.writerows(all_record_ases)




	# Find total prefixes routed on that date
	# Split the prefixes by spaces and then flatten the list
	prefixes = df_cleaned['prefix'].explode().unique()

	headers = ["year", "mon", "day", "no_prefixes"] 
	all_record_prefixes = [[year, mon, day, len(prefixes)]]
	filename_prefixes = path + "final_prefixes_2017_2024.csv"

	with open(filename_ases, 'a') as f:
	    write = csv.writer(f)
	    write.writerow(headers)
	    write.writerows(all_record_prefixes)
	print("No. of prefixes written.")
	print("Done for month %s" %mon)
