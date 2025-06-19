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

def process_route_data_parallel(file_path, output_file, num_workers=8, chunk_size=100000):
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

pattern = r"raw_as"+scrubber_asn+".*\.txt$"  # Match files with format raw_as32787_*.txt

print("Step 1: Saving raw bgpreader data in .txt format to csv file.\n")

# Loop through the txt files that contain RIB snapshots of every month
for filename in os.listdir(path):
     # Read only processed files
    if re.search(pattern, filename):
        mon = filename.split('_')[3] # Get month name from file name
        year = filename.split('_')[-1].split('.')[0]
        file_path = path + filename
        output_file = path + 'optimized_raw_as'+scrubber_asn+'_01_'+mon+'_'+year+'.csv'
        process_route_data_parallel(file_path, output_file)
print("Step 1 completed.")

print("Step 2: Removing duplicates.")
# Read csv file and remove duplicate rows
pattern = r"optimized_raw_as"+scrubber_asn+".*\.csv$"  # Match files with format raw_as32787_*.txt
       
# Loop through the txt files that contain RIB snapshots of every month  
for filename in os.listdir(path):
     # Read only processed files
    if re.search(pattern, filename):
        mon = filename.split('_')[4] # Get month name from file name
#         print("Mon is ", mon)
        #year = filename.split('_')[-1].split('.')[0]
        year = filename.split('_')[-1].split('.')[0]
 
        df = pd.read_csv(path + 'optimized_raw_as'+scrubber_asn+'_01_'+mon+'_'+year+'.csv', low_memory=False)
       # df = pd.read_csv(path + filename)
 

        # Remove duplicate rows in dataframe
        df = df.drop_duplicates()

        # Write unique rows into a file
        df.to_csv(path + 'unique_optimized_raw_as'+scrubber_asn+'_01_'+mon+'_'+year+'.csv', index=False)
        
        # Remove original file that contains duplicate records
        os.remove(path + 'optimized_raw_as'+scrubber_asn+'_01_'+mon+'_'+year+'.csv')

print("Step 2 completed.")  

print("Step 3: Removing private ASNs.")
# Generate a set of private ASNs
private_asns = set(range(64512, 65535 + 1))

# Define a function to check if any ASN in the AS path is private
def has_private_asn(as_path):
    try:
        # Convert the AS path to a list of integers
        as_numbers = map(int, as_path.split())
        return any(asn in private_asns for asn in as_numbers)
    except ValueError:
        # Handle cases where the AS path is invalid
        return False


pattern = r"unique_optimized_raw_as"+scrubber_asn+".*\.csv$"  # Match files with format raw_as32787_*.txt

# Loop through the txt files that contain RIB snapshots of every month
for filename in os.listdir(path):
     # Read only processed files
    if re.search(pattern, filename):
        day = filename.split('_')[4] # Get day from file name
        mon = filename.split('_')[5] # Get month name from file name
        year = filename.split('_')[-1].split('.')[0]

        date = year + "-" + mon + "-" + day

        df = pd.read_csv(path + 'unique_optimized_raw_as'+scrubber_asn+'_01_'+mon+'_'+year+'.csv', low_memory=False)


        # Apply the function to filter rows
        mask = df['as_path'].apply(has_private_asn)

        # Filter out rows with private ASNs
        df_without_private_asns = df[~mask]

        # Write rows after removing private ASNs into a file
        df_without_private_asns.to_csv(path + 'unique_optimized_raw_as'+scrubber_asn+'_01_'+mon+'_'+year+'.csv', index = False)

print("Step 3 completed.")

print("Step 4: Removing AS sets.")
pattern = r"unique_optimized_raw_as"+scrubber_asn+".*\.csv$"  # Match files with format raw_as32787_*.txt

# Loop through the txt files that contain RIB snapshots of every month
for filename in os.listdir(path):
     # Read only processed files
    if re.search(pattern, filename):
        day = filename.split('_')[4] # Get day from file name
        mon = filename.split('_')[5] # Get month name from file name
        year = filename.split('_')[-1].split('.')[0]
        date = year + "-" + mon + "-" + day

        df = pd.read_csv(path + 'unique_optimized_raw_as'+scrubber_asn+'_01_'+mon+'_'+year+'.csv', low_memory=False)

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

        df_cleaned.to_csv(path + 'unique_optimized_raw_as13335_01_jan_2024.csv', index = False)
        print("Number of rows with set origin:", count_set_origin)
        print("DataFrame after removal:")
        print("Records after %s." %len(df_cleaned))
        print("%s number of records were removed that contains AS set in AS paths." %(len(df) - len(df_cleaned)))


print("Step 4 completed.")

# Pyrank API
# Method to get AS rank of all ASes from CAIDA AS rank API
# This part of the code is used from https://github.com/bgpkit/pyasrank
import json
import logging
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ASRANK_ENDPOINT = "https://api.asrank.caida.org/v2/graphql"


def ts_to_date_str(ts):
    """
    Convert timestamp to a date. This is used for ASRank API which only takes
    date strings with no time as parameters.api
    """
    return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")


class AsRank:
    """
    Utilities for using ASRank services
    """

    def __init__(self, max_ts=""):
        self.data_ts = None

        # various caches to avoid duplicate queries
        self.cache = None
        self.cone_cache = None
        self.neighbors_cache = None
        self.siblings_cache = None
        self.organization_cache = None

        self.queries_sent = 0

        self.session = None
        self._initialize_session()

        self.init_cache(max_ts)

    def _initialize_session(self):
        self.session = requests.Session()
        retries = Retry(total=5,
                        backoff_factor=1,
                        status_forcelist=[500, 502, 503, 504])
        self.session.mount(ASRANK_ENDPOINT, HTTPAdapter(max_retries=retries))

    def _close_session(self):
        if self.session:
            self.session.close()

    def _send_request(self, query):
        """
        send requests to ASRank endpoint
        :param query:
        :return:
        """

        r = self.session.post(url=ASRANK_ENDPOINT, json={'query': query})
        r.raise_for_status()
        self.queries_sent += 1
        return r

    def init_cache(self, ts):
        """
        Initialize the ASRank cache for the timestamp ts
        :param ts:
        :return:
        """
        self.cache = {}
        self.cone_cache = {}
        self.neighbors_cache = {}
        self.siblings_cache = {}
        self.organization_cache = {}
        self.queries_sent = 0
        if isinstance(ts, int):
            ts = ts_to_date_str(ts)

        ####
        # Try to cache datasets available before the given ts
        ####
        graphql_query = """
            {
              datasets(dateStart:"2000-01-01", dateEnd:"%s", sort:"-date", first:1){
                edges {
                  node {
                    date
                  }
                }
              }
            }
        """ % ts
        r = self._send_request(graphql_query)

        edges = r.json()['data']['datasets']['edges']
        if edges:
            self.data_ts = edges[0]["node"]["date"]
            return

        # if code reaches here, we have not found any datasets before ts. we should now try to find one after ts.
        # this is the best effort results
        logging.warning("cannot find dataset before date %s, looking for the closest one after it now" % ts)

        graphql_query = """
            {
              datasets(dateStart:"%s", sort:"date", first:1){
                edges {
                  node {
                    date
                  }
                }
              }
            }
        """ % ts
        r = self._send_request(graphql_query)
        edges = r.json()['data']['datasets']['edges']
        if edges:
            self.data_ts = edges[0]["node"]["date"]
            logging.warning("found closest dataset date to be %s" % self.data_ts)
            return
        else:
            raise ValueError("no datasets from ASRank available to use for tagging")

    def _query_asrank_for_asns(self, asns, chunk_size=100):
        asns = [str(asn) for asn in asns]
        asns_needed = [asn for asn in asns if asn not in self.cache]
        if not asns_needed:
            return

        # https://stackoverflow.com/a/312464/768793
        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        for asns in chunks(asns_needed, chunk_size):

            graphql_query = """
                {
                  asns(asns: %s, dateStart: "%s", dateEnd: "%s", first:%d, sort:"-date") {
                    edges {
                      node {
                        date
                        asn
                        asnName
                        rank
                        organization{
                          country{
                            iso
                            name
                          }
                          orgName
                          orgId
                        } asnDegree {
                          provider
                          peer
                          customer
                          total
                          transit
                          sibling
                        }
                      }
                    }
                  }
                }
            """ % (json.dumps(asns), self.data_ts, self.data_ts, len(asns))
            r = self._send_request(graphql_query)
            try:
                for node in r.json()['data']['asns']['edges']:
                    data = node['node']
                    if data['asn'] not in self.cache:
                        if "asnDegree" in data:
                            degree = data["asnDegree"]
                            degree["provider"] = degree["provider"] or 0
                            degree["customer"] = degree["customer"] or 0
                            degree["peer"] = degree["peer"] or 0
                            degree["sibling"] = degree["sibling"] or 0
                            data["asnDegree"] = degree
                        self.cache[data['asn']] = data
                for asn in asns:
                    if asn not in self.cache:
                        self.cache[asn] = None
            except KeyError as e:
                logging.error("Error in node: {}".format(r.json()))
                logging.error("Request: {}".format(graphql_query))
                raise e

    ##########
    # AS_ORG #
    ##########

    def are_siblings(self, asn1, asn2):
        """
        Check if two ASes are sibling ASes, i.e. belonging to the same organization
        :param asn1: first asn
        :param asn2: second asn
        :return: True if asn1 and asn2 belongs to the same organization
        """
        self._query_asrank_for_asns([asn1, asn2])
        if any([self.cache[asn] is None for asn in [asn1, asn2]]):
            return False
        try:
            return self.cache[asn1]["organization"]["orgId"] == self.cache[asn2]["organization"]["orgId"]
        except TypeError:
            # we have None for some of the values
            return False

    def get_organization(self, asn):
        """
        Keys:
        - country
        - orgName
        - orgId

        Example return value:
        {'country': {'iso': 'US', 'name': 'United States'}, 'orgName': 'Google LLC', 'orgId': 'f7b8c6de69'}

        :param asn:
        :return:
        """
        self._query_asrank_for_asns([asn])
        if self.cache[asn] is None:
            return None
        return self.cache[asn]["organization"]



    ###########
    # AS_RANK #
    ###########

    def get_degree(self, asn):
        """
        Get relationship summary for asn, including number of customers, providers, peers, etc.

        Example return dictionary:
        {
            "provider": 0,
            "peer": 31,
            "customer": 1355,
            "total": 1386,
            "transit": 1318,
            "sibling": 25
        }
        :param asn:
        :return:
        """
        self._query_asrank_for_asns([asn])
        if self.cache[asn] is None:
            return None

        return self.cache[asn]["asnDegree"]

    def is_sole_provider(self, asn_pro, asn_cust):
        """
        Verifies if asn_pro and asn_cust are in a customer provider relationship
        and asn_pro is the sole upstream of asn_cust (no other providers nor peers
        are available to asn_cust).

        This function is ported from dataconcierge.ASRank.check_single_upstream. The name of which is confusing, thus
        renamed to is_sole_provider.

        :param asn_pro: provider ASn (string)
        :param asn_cust: ASn in customer cone (string)
        :return: True or False
        """
        asn_cust_degree = self.get_degree(asn_cust)
        if asn_cust_degree is None:
            # missing data for asn_cust
            return False
        if asn_cust_degree["provider"] == 1 and asn_cust_degree["peer"] == 0 and \
                self.get_relationship(asn_pro, asn_cust) == "p-c":
            # asn_cust has one provider, no peer, and the provider is asn_pro
            return True
        return False

    def get_relationship(self, asn0, asn1):
        """
        Get the AS relationship between asn0 and asn1.

        asn0 is asn1's:
        - provider: "p-c"
        - customer: "c-p"
        - peer: "p-p"
        - other: None

        :param asn0:
        :param asn1:
        :return:
        """
        graphql_query = """
            {
              asnLink(asn0:"%s", asn1:"%s", date:"%s"){
              relationship
              }
            }
        """ % (asn0, asn1, self.data_ts)
        r = self._send_request(graphql_query)
        if r.json()["data"]["asnLink"] is None:
            return None
        rel = r.json()["data"]["asnLink"].get("relationship", "")

        if rel == "provider":
            # asn1 is the provider of asn0
            return "c-p"

        if rel == "customer":
            # asn1 is the customer of asn0
            return "p-c"

        if rel == "peer":
            # asn1 is the peer of asn0
            return "p-p"

        return None

    def in_customer_cone(self, asn0, asn1):
        """
        Check if asn0 is in the customer cone of asn1
        :param asn0:
        :param asn1:
        :return:
        """
        if asn1 in self.cone_cache:
            return asn0 in self.cone_cache[asn1]

        graphql_query = """
        {
          asnCone(asn:"%s", date:"%s"){
            asns {
              edges {
                node {
                  asn
                }
              }
            }
          }
        }
        """ % (asn1, self.data_ts)
        r = self._send_request(graphql_query)
        data = r.json()["data"]["asnCone"]
        if data is None:
            return False
        asns_in_cone = {node["node"]["asn"] for node in data["asns"]["edges"]}
        self.cone_cache[asn1] = asns_in_cone
        return asn0 in asns_in_cone

    def cache_asrank_chunk(self, asns: list, chunk_size: int):
        """
        Query asrank info in chunk to boost individual asrank queries performance later.

        :param asns:
        :param chunk_size:
        :return:
        """
        self._query_asrank_for_asns(asns, chunk_size)

    def get_all_siblings_list(self, asns, chunk_size=100):
        self._query_asrank_for_asns(asns, chunk_size)
        res = {}
        for asn in asns:
            res[asn] = self.get_all_siblings(asn)
        return res

    def get_all_siblings(self, asn, skip_asrank_call=False):
        """
        get all siblings for an ASN
        :param asn: AS number to query for all siblings
        :param skip_asrank_call: skip asrank call if already done
        :return: a tuple of (TOTAL_COUNT, ASNs)
        """
        # FIXME: pagination does not work here. Example ASN5313.
        asn = str(asn)
        if asn in self.siblings_cache:
            return self.siblings_cache[asn]

        if not skip_asrank_call:
            self._query_asrank_for_asns([asn])

        if asn not in self.cache or self.cache[asn] is None:
            return 0, []
        asrank_info = self.cache[asn]
        if "organization" not in asrank_info or asrank_info["organization"] is None:
            return 0, []

        org_id = self.cache[asn]["organization"]["orgId"]

        if org_id in self.organization_cache:
            data = self.organization_cache[org_id]
        else:
            graphql_query = """
            {
            organization(orgId:"%s"){
              orgId,
              orgName,
              members{
                numberAsns,
                numberAsnsSeen,
                asns{totalCount,edges{node{asn,asnName}}}
              }
            }}
            """ % org_id
            r = self._send_request(graphql_query)
            data = r.json()["data"]["organization"]
            self.organization_cache[org_id] = data

        if data is None:
            return 0, []

        total_cnt = data["members"]["asns"]["totalCount"]
        siblings = set()
        for sibling_data in data["members"]["asns"]["edges"]:
            siblings.add(sibling_data["node"]["asn"])
        if asn in siblings:
            siblings.remove(asn)
            total_cnt -= 1

        # NOTE: this assert can be wrong when number of siblings needs pagination
        # assert len(siblings) == total_cnt - 1

        siblings = list(siblings)
        self.neighbors_cache[asn] = (total_cnt, siblings)
        return total_cnt, siblings

import os
import pandas as pd
import csv
import re

path = "/data/shared_dir/ddos_scrubber/as"+scrubber_asn+"/"+year+"/"
print("Step 5: Finding records that were not originated by the scrubber and contain scrubber as the second last hop.")

pattern = r"unique_optimized_raw_as.*\.csv$"  # Match files with format unique_optimized_raw_as32787_*.csv

import time

# Loop through all the .csv files
for filename in os.listdir(path):

    # Read only processed files
    if re.search(pattern, filename):
        day = filename.split('_')[4] # Get day from file name
        mon = filename.split('_')[5] # Get month name from file name
        year = filename.split('_')[-1].split('.')[0]
        date = year + "-" + mon + "-" + day

        df = pd.read_csv(path + filename, low_memory=False)

        # Find number of unique prefixes that do not contain scrubber as well as its siblings as origin
        # Siblings are found using ASRank API
        print("Calling AS Rank API.")
        as_rank = AsRank(date)
        print("Wait until 30 seconds.")
        time.sleep(30) # Added because AS rank API threw error sending many requests in a few seconds.

        siblings = as_rank.get_all_siblings(scrubber_asn)[1]
        siblings.append(scrubber_asn)
        #siblings.append("209242") # Remove it for other except cloudflare.

        # Convert string list to int list
        siblings_int = [int(x) for x in siblings]

        condition = (~df['origin_as'].isin (siblings_int))
        df2 = df.loc[condition]

        # Saving records that the scrubber originates.
        origin_scrubber_df = df[df["origin_as"] == int(scrubber_asn)]
        origin_scrubber_df.to_csv(path + 'unique_optimized_raw_origin_scrubber_as'+scrubber_asn+'_01_'+mon+'_'+year+'.csv')
        
        # Prefixes containing scrubber asn as the second last hop AS in AS path
        provider_scrubber_records = df2.loc[df['provider'] == int(scrubber_asn)]

        # Save confirmed_customers1 to a csv file
        provider_scrubber_records.to_csv(path+"confirmed_customers_as"+scrubber_asn+"_"+day+"_"+mon+"_"+year+".csv", index=False)
        # Unique origin ASes are actually CONFIRMED customers that have their second last hop AS as AS198949
        unique_origin_ases = provider_scrubber_records['origin_as'].unique()
        print("%s number of unique customer ASNs of AS%s on %s" %(len(unique_origin_ases), scrubber_asn, mon))

        unique_prefixes = provider_scrubber_records['prefix'].unique()
        print("%s number of unique prefixes that contain AS%s as provider on %s %s.\n" %(len(unique_prefixes), scrubber_asn, day, mon))

        print("Finding cases where second last hop is not the scrubber ASN.")
        provider_not_scrubber = df2.loc[df['provider'] != int(scrubber_asn)]
        provider_not_scrubber.to_csv(path + "unique_optimized_provider_not_as"+scrubber_asn+"_01_"+mon+"_"+year+".csv", index=False)
        print("%s number of prefixes do not contain AS%s as a provider (second last hop in AS path).\n" %(len(provider_not_scrubber), scrubber_asn))
#         del as_rank # Destroy object

print("Step 5 completed.")

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

