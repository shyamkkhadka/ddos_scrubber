# Fidn total protected ASes combining all the 3 patterns
import pandas as pd
import csv
import os

year = "2024"
day = "01"

months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]

# List your scrubber ASNs here
#scrubber_asns = ["32787", "13335", "19551", "198949", "19905"] 
scrubber_asns = ["13335"]

for mon in months:
    print(f"Processing month: {mon}")
    
    for scrubber_asn in scrubber_asns:
        path = "/data/shared_dir/ddos_scrubber/as"+scrubber_asn+"/"+year+"/"
        all_record_prefixes = []  # Stores number of customer prefixes for a month

        # Load confirmed customers
        confirmed_customers1 = pd.read_csv(path + f"confirmed_customers_as{scrubber_asn}_{day}_{mon}_{year}.csv")
        confirmed_customers1_prefixes = confirmed_customers1['prefix'].unique()
        
        if os.path.exists(path + "unique_optimized_provider_as" + scrubber_asn + "_path_prepend_"+day+"_" + mon + "_" + year + ".csv"):
            path_prepending = pd.read_csv(path + "unique_optimized_provider_as" + scrubber_asn + "_path_prepend_"+day+"_"+  mon + "_" + year + ".csv")
            path_prepending_unique_prefixes = path_prepending['prefix'].unique()
            confirmed_customers2_prefixes = path_prepending_unique_prefixes
        else:
            confirmed_customers2_prefixes = []
            
        if os.path.exists(path + "unique_optimized_provider_not_as" + scrubber_asn + "_"+ day +"_" + mon + "_" + year + "_v3.csv"):
            df = pd.read_csv(
                path + "unique_optimized_provider_not_as" + scrubber_asn + "_"+day+"_" + mon + "_" + year + "_v3.csv")
            sibling_path = df.loc[(df['siblings'] == 1) & (df['new_provider_sibling_check'] == int(scrubber_asn))]
            sibling_path_unique_prefixes = sibling_path['prefix'].unique()
            confirmed_customers3_prefixes = sibling_path_unique_prefixes

        else:
            confirmed_customers3_prefixes = []

        # Combine all unique origin ASes
        confirmed_customers_prefixes = set(confirmed_customers1_prefixes) | set(confirmed_customers2_prefixes) | set(confirmed_customers3_prefixes)  # Union

        print(f"Customers 1: {len(confirmed_customers1_prefixes)}, 2: {len(confirmed_customers2_prefixes)}, 3: {len(confirmed_customers3_prefixes)}, final {len(confirmed_customers_prefixes)}")
        # Write to CSV
        output_path = f"customers_prefixes_scrubber_{scrubber_asn}_{day}_{mon}_{year}.csv"
        #os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(path + output_path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["as"])  # Header
            for prefixes in confirmed_customers_prefixes:
                writer.writerow([prefixes])

        print(f"Done for scrubber {scrubber_asn} in {mon}")
print("Completed all months.")
