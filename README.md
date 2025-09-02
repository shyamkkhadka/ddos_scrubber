# Artifacts of the paper accepted in 21st International Conference on Network and Service Management (CNSM 2025)
Paper: A first look at the adoption of BGP-based DDoS scrubbing services: A 5-year longitudinal analysis.  
## File descriptions
- process-bgp-data-step1.py: The first step of the code that processes a raw BGP data downloaded using bgpreader. It converts raw BGP data into csv file. This file represents steps 1 to 6 of the methodology and pattern distribution explained in the paper.
- openintel-tranco-2024-100k.ipynb: Checks how many protected prefixes host Tranco 1M list of domains.
- openintel-alexa-2024-100k.ipynb: Checks how many protected prefixes host Alexa 1M list of domains.
- protected-ases-analysis.ipynb: Maps protected ASes to their registered countries and the top finance and insurance companies of the top 8 countries.
- protected-ases-analysis.py: Determines total protected ASes for each scrubber after combining three patterns.
- protected-prefixes-analysis.py: Determines total protected prefixes for each scrubber after combining three patterns.
- find-total-ases-prefixes.py: Determines the total prefixes and ases as seen in the BGP data monthly.
