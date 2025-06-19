import pandas as pd
import requests
import io
year = "2020"
day = "01"
mon = "jan"

# OpenINTEL open data URL, full directory available at: https://openintel.nl/download/, https://openintel.nl/data/forward-dns/top-lists/
openintel_url = 'https://object.openintel.nl/openintel-public/fdns/basis=toplist/source=umbrella/year=2020/month=01/day=01/part-00000-d61ac111-3e04-4a55-81fe-94d0f455353c-c000.gz.parquet'

# Download the file into memory
response = requests.get(openintel_url)
response.raise_for_status()  # Raise an error if the download fails
# Read the parquet file into a DataFrame
df = pd.read_parquet(io.BytesIO(response.content), engine="pyarrow")
# Process the OpenINTEL data
umbrella_a_df = df[df['response_type'] == 'A'] # Filter on DNS response type A
umbrella_a_ip4_df = umbrella_a_df['ip4_address']  # Get the column we want

umbrella_a_ip4_nparray = umbrella_a_ip4_df.unique()  # Only grab unique values

umbrella_a_df['domain'] = umbrella_a_df['response_name'].str.rstrip('.')

umbrella_domain_a_df = umbrella_a_df[['domain', 'ip4_address']].copy()
unique_umbrella_domain_a_df = umbrella_domain_a_df.drop_duplicates()
 # Sometimes duplications are found.
# Print count
print('Total IP4 addresses found: %s and unique addresses are %s' %((len(umbrella_a_ip4_df), len(umbrella_a_ip4_nparray))))
unique_umbrella_domain_a_df.to_csv("openintel_umbrella_domain_a_record_"+day+"_"+mon+"_"+year+".csv", index=False)


# Find ranks of umbrella 100k domains with their A records (IP address/es) and save it into a file.
# After TMA
import tarfile
import csv
import pandas as pd
import io

# 1. Read umbrella_domain_a_df that has domain name and ranks: openintel_domain_a_record
# 2. Join it with umbrella_domains_with_rank
# File umbrella_domains_with_rank_<year>.csv is downloaded from DACS object storage
# traces/Feeds/<year>/umbrella/umbrella_<year>-12-31.tar.gz

# Path to your .tar.gz file
archive_path = 'umbrella_domains_with_rank_01_jan_2020.csv.zip'

# Open the archive
# Assign column names directly while reading
df1_orig = pd.read_csv(archive_path, names=["rank", "domain"], header=None)

df1 = df1_orig[0:100000] # Choose only 100k based on https://attentioninsight.com/what-is-umbrella-rank-and-its-value/
df2 = pd.read_csv("openintel_umbrella_domain_a_record_"+day+"_"+mon+"_"+year+".csv")
df3 = df1.merge(df2, how='left', on='domain')
df3.to_csv("openintel_umbrella_resolved_with_rank_100k_"+day+"_"+mon+"_"+year+".csv", index=False)

# Find the number of matched IP addresses 

# Compare the ip addresses with the protected prefixes using pytricia loop
import pandas as pd
import ipaddress
import pytricia


def build_prefix_trees(prefix_list):
    """Builds two Patricia Tries: one for IPv4 and one for IPv6."""
    pt_v4 = pytricia.PyTricia()
    pt_v6 = pytricia.PyTricia()

    for prefix in prefix_list:
        network = ipaddress.ip_network(prefix)
        if network.version == 4:

            pt_v4[prefix] = True
        else:
            pt_v6[prefix] = True

    return pt_v4, pt_v6


def count_covered_ips(ip_list, pt_v4, pt_v6):
    """Counts how many IPs are covered by the prefixes in the Patricia Tries."""
    count = 0
    for ip in ip_list:
        ip_obj = ipaddress.ip_address(ip)
        if ip_obj.version == 4 and pt_v4.get(ip):
            count += 1
        elif ip_obj.version == 6 and pt_v6.get(ip):
            count += 1

    return count


def get_covered_ips(ip_list, pt_v4, pt_v6):
    """Returns the list of IPs covered by prefixes in the Patricia Trie."""
    covered_ips = []
    for ip in ip_list:
        ip_obj = ipaddress.ip_address(ip)
        if ip_obj.version == 4 and pt_v4.get(ip):
            covered_ips.append(ip)
        elif ip_obj.version == 6 and pt_v6.get(ip):
            covered_ips.append(ip)
#     covered_ips = [ip for ip in ip_list if prefix_tree.get(ip)]
    return covered_ips



# Protected prefixes lists using all five scrubberscovered_count
df = pd.read_csv("customers_prefixes_scrubber_all_"+day+"_"+mon+"_"+year+".csv") 
protected_prefixes = df["prefix"].tolist()

# Openintel list of IP addresses
df = pd.read_csv("openintel_umbrella_resolved_with_rank_100k_"+day+"_"+mon+"_"+year+".csv") 
df_cleaned = df.dropna()

umbrella_ip_addresses = df_cleaned["ip4_address"].unique()

# Build separate prefix trees for IPv4 and IPv6
pt_v4, pt_v6 = build_prefix_trees(protected_prefixes)

# Find the number of IPs covered
covered_count = count_covered_ips(umbrella_ip_addresses, pt_v4, pt_v6)

# Find the IPs covered
covered_ips = get_covered_ips(umbrella_ip_addresses, pt_v4, pt_v6)


# Save covered ips in a .txt file.
with open("openintel_umbrella_scrubber_covered_ip_100k_"+day+"_"+mon+"_"+year+".txt", "w", encoding="utf-8") as f:
    for ip in covered_ips:
        f.write(f"{ip}\n")
print(f"Number of IPs protected by the five scrubber's protected prefixes is: {len(set(covered_ips))} and IPs saved in openintel_umbrella_scrubber_covered_ip_"+day+"_"+mon+"_"+year+".txt")


import csv

covered_ips = []
# Parse massdns results
with open("openintel_umbrella_scrubber_covered_ip_100k_"+day+"_"+mon+"_"+year+".txt", "r", encoding="utf-8") as f:
    for line in f:
        ip = line.strip("\n")
        covered_ips.append(ip)
        
# Convert it into a dataframe with column name ipv4_address
df1 = pd.DataFrame(covered_ips, columns=['ip4_address'])

df2 = pd.read_csv("openintel_umbrella_resolved_with_rank_100k_"+day+"_"+mon+"_"+year+".csv")

# df3 = df2.merge(df1, how='inner', on='ip4_address')
        
    
result = df1.merge(df2, on='ip4_address', how='inner')
result.to_csv("openintel_umbrella_ip_domains_ranks_100k_"+day+"_"+mon+"_"+year+".csv", index=False)

print("%s number of domains are protected in %s %s %s. \n" %(len(result["domain"].unique()), day, mon, year))



