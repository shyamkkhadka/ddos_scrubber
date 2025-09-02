# Fidn total protected ASes combining all the 3 patterns
import pandas as pd
import csv
import os

year = "2020"
day = "01"

months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
#months = ["dec"]

# List your scrubber ASNs here
#scrubber_asns = ["32787", "19551", "198949", "19905"] 
scrubber_asns = ["13335"]

for mon in months:
    print(f"Processing month: {mon}")
    
    for scrubber_asn in scrubber_asns:
        path = "/data/shared_dir/ddos_scrubber/as"+scrubber_asn+"/"+year+"/"
        all_record_ases = []  # Stores number of customer ases for a month

        # Load confirmed customers
        confirmed_customers1 = pd.read_csv(path + f"confirmed_customers_as{scrubber_asn}_{day}_{mon}_{year}.csv")
        confirmed_customers1_ases = confirmed_customers1['origin_as'].unique()
        
        if os.path.exists(path + "unique_optimized_provider_as" + scrubber_asn + "_path_prepend_"+day+"_" + mon + "_" + year + ".csv"):
            path_prepending = pd.read_csv(path + "unique_optimized_provider_as" + scrubber_asn + "_path_prepend_"+day+"_"+  mon + "_" + year + ".csv")
            path_prepending_unique_origin_ases = path_prepending['origin_as'].unique()
            confirmed_customers2_ases = path_prepending_unique_origin_ases
        else:
            confirmed_customers2_ases = []
            
        if os.path.exists(path + "unique_optimized_provider_not_as" + scrubber_asn + "_"+ day +"_" + mon + "_" + year + "_v3.csv"):
            df = pd.read_csv(
                path + "unique_optimized_provider_not_as" + scrubber_asn + "_"+day+"_" + mon + "_" + year + "_v3.csv")
            sibling_path = df.loc[(df['siblings'] == 1) & (df['new_provider_sibling_check'] == int(scrubber_asn))]
            sibling_path_unique_origin_ases = sibling_path['origin_as'].unique()
            confirmed_customers3_ases = sibling_path_unique_origin_ases

        else:
            confirmed_customers3_ases = []

        # Combine all unique origin ASes
        confirmed_customers_ases = set(confirmed_customers1_ases) | set(confirmed_customers2_ases) | set(confirmed_customers3_ases)  # Union

        print(f"Customers 1: {len(confirmed_customers1_ases)}, 2: {len(confirmed_customers2_ases)}, 3: {len(confirmed_customers3_ases)}, final {len(confirmed_customers_ases)}")
        # Write to CSV
        output_path = f"customers_ases_scrubber_{scrubber_asn}_{day}_{mon}_{year}.csv"
        #os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(path + output_path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["as"])  # Header
            for ases in confirmed_customers_ases:
                writer.writerow([ases])

        print(f"Done for scrubber {scrubber_asn} in {mon}")
print("Completed all months.")
