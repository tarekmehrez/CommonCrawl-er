# CommonCrawl-er
Fetching pages from common-crawl, according to given domains

Domains should be specified in domains.txt
fetch_data.py gets all indexed pages by common-crawl for the mentioned domains.
The variable index_list in fetch_data.py contains all common-crawl index up to date.

By running fetch_data.py, indexed WARC files for each domain are then saved as follows:
data/[domain_name]/[index_id]/file.warc

Hint: Code is currently parallelized on 20 cores by default. Cores must be changed according to machine's capabilities.
