import requests
import argparse
import time
import json
import StringIO
import gzip
import csv
import codecs
import html2text
import collections
import os
import sys
import logging

reload(sys)
sys.setdefaultencoding('utf8')

class CommonCrawlFetcher:


	def __init__(self, domain):

		logging.basicConfig(filename='logging.log', level=logging.DEBUG,format='%(asctime)s : %(levelname)s : %(message)s')
		self._logger = logging.getLogger(__name__)

		self._domain = domain
		self._index_list = [
								"2013-20",
								"2013-48",

								"2014-10",
								"2014-15",
								"2014-23",
								"2014-35",
								"2014-41",
								"2014-42",
								"2014-49",
								"2014-52",

								"2015-06",
								"2015-11",
								"2015-14",
								"2015-18",
								"2015-22",
								"2015-27",
								"2015-32",
								"2015-35",
								"2015-40",
								"2015-48",
								"2016-07",	]

		self._hits = self._search_domain()

		self._curr_dir = 'data' + '/' + str(self._domain)
		self._create_dirs()


	def _search_domain(self):

		d = collections.OrderedDict()
		record_list = d.fromkeys(self._index_list, [])


		self._logger.info("Fetching for target domain: %s" % self._domain)

		for index in record_list:

			self._logger.info("Fetching index %s" % index)

			cc_url  = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
			cc_url += "url=%s&matchType=domain&output=json" % self._domain

			response = requests.get(cc_url)
			total_records = 0
			if response.status_code == 200:

				records = response.content.splitlines()

				for record in records:
					record_list[index].append(json.loads(record))

				self._logger.info("Added %d results." % len(records))
				total_records += len(records)

		self._logger.info("Found a total of %d hits." % len(record_list))
		self._logger.info("Total records: %d" % total_records)

		return record_list


	def _download_page(self, record):

		offset, length = int(record['offset']), int(record['length'])
		offset_end = offset + length - 1

		prefix = 'https://aws-publicdatasets.s3.amazonaws.com/'
		resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})

		raw_data = StringIO.StringIO(resp.content)
		f = gzip.GzipFile(fileobj=raw_data)

		data = f.read()

		response = ""

		if len(data):
			return data

	def _create_dirs(self):

		if not os.path.exists('data'):
			os.makedirs('data')

		for index in self._hits:
			curr = self._curr_dir +'/'+ str(index)
			if not os.path.exists(curr):
				os.makedirs(curr)

	def run(self):
		self._logger.info('Now Fetching WARC files')
		for index, records in self._hits.iteritems():

			for idx, record in enumerate(records):

				content = self._download_page(record)
				curr_file = self._curr_dir + '/' + str(index) + "/" + str(idx) + ".warc"

				with open(curr_file, "w") as text_file:
					text_file.write(content)


with open ('domains.txt', 'rb') as f:
	domains = f.read().split('\n')

for domain in domains:
	fetcher = CommonCrawlFetcher(domain)
	fetcher.run()

