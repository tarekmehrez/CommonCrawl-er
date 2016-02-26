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

reload(sys)
sys.setdefaultencoding('utf8')

class CommonCrawlFetcher:


	def __init__(self, domain):

		self._domain = domain
		self._index_list = [	"2013-20",
								"2013-48",

								# "2014-10",
								# "2014-15",
								# "2014-23",
								# "2014-35",
								# "2014-41",
								# "2014-42",
								# "2014-49",
								# "2014-52",

								# "2015-06",
								# "2015-11",
								# "2015-14",
								# "2015-18",
								# "2015-22",
								# "2015-27",
								# "2015-32",
								# "2015-35",
								"2015-40",	]

		self._hits = self._search_domain()
		self._create_dirs()

	def _search_domain(self):

		d = collections.OrderedDict()
		record_list = d.fromkeys(self._index_list, [])


		print "[*] Trying target domain: %s" % self._domain

		for index in record_list:

			print "[*] Trying index %s" % index

			cc_url  = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
			cc_url += "url=%s&matchType=domain&output=json" % self._domain

			response = requests.get(cc_url)

			if response.status_code == 200:

				records = response.content.splitlines()

				for record in records:
					record_list[index].append(json.loads(record))

				print "[*] Added %d results." % len(records)


		print "[*] Found a total of %d hits." % len(record_list)

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
			try:
				warc, header, response = data.strip().split('\r\n\r\n', 2)
			except:
				pass

		return html2text.html2text(response)

	def _create_dirs(self):

		if not os.path.exists('data'):
			os.makedirs('data')

		for index in self._hits:
			curr_dir  = 'data' + '/' + str(index)
			if not os.path.exists(curr_dir):
				os.makedirs(curr_dir)

	def run(self):

		for index, records in self._hits.iteritems():

			for idx, record in enumerate(records):

				print index, record
				content = self._download_page(record)
				curr_file = 'data' + '/' + str(index) + '/' + str(self._domain) + '-' + str(idx)

				with open(curr_file, "w") as text_file:
					text_file.write(content)

				# print "[*] Retrieved %d bytes for %s" % (len(content),record['url'])


fetcher = CommonCrawlFetcher('cnn.com')
fetcher.run()

