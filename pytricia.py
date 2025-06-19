Steps to rub in aruba
# Find the number of matched IP addresses 

# Compare the ip addresses with the protected prefixes using pytricia loop
import pandas as pd
import ipaddress
import pytricia


year = "2020"
mon = "jun"
day = "01"


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
df = pd.read_csv("openintel_alexa_resolved_with_rank_100k_"+day+"_"+mon+"_"+year+".csv") 
df_cleaned = df.dropna()

alexa_ip_addresses = df_cleaned["ip4_address"].unique()

# Build separate prefix trees for IPv4 and IPv6
pt_v4, pt_v6 = build_prefix_trees(protected_prefixes)

# Find the number of IPs covered
covered_count = count_covered_ips(alexa_ip_addresses, pt_v4, pt_v6)

# Find the IPs covered
covered_ips = get_covered_ips(alexa_ip_addresses, pt_v4, pt_v6)


# Save covered ips in a .txt file.
with open("openintel_alexa_scrubber_covered_ip_100k_"+day+"_"+mon+"_"+year+".txt", "w", encoding="utf-8") as f:
    for ip in covered_ips:
        f.write(f"{ip}\n")
print(f"Number of IPs protected by the five scrubber's protected prefixes in {day} {mon} {year} is: {len(covered_ips)} and IPs saved in openintel_alexa_scrubber_covered_ip_100k_"+year+".txt")
