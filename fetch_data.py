import requests
import json
import StringIO
import gzip
import html2text
import collections
import os
import sys
import logging
import re

from multiprocessing.dummy import Pool as ThreadPool

reload(sys)
sys.setdefaultencoding('utf8')

global logger
logging.basicConfig(filename='logging.log', level=logging.INFO,format='%(asctime)s : %(levelname)s : %(message)s')
logger = logging.getLogger(__name__)


def fetch_records(domain, index):

	logger.info("Fetching for target domain: %s" % domain)


	logger.info("Fetching index %s" % index)

	cc_url  = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
	cc_url 	+= "url=%s&matchType=domain&output=json" % domain

	response = requests.get(cc_url)

	record_list = []
	keys = ['offset', 'length', 'filename']

	if response.status_code == 200:

		records = response.content.splitlines()
		for record in records:
			curr_record_dict = json.loads(record)
			new_record_dict = {}

			for key in keys:
				new_record_dict[key] = curr_record_dict[key]

			record_list.append(new_record_dict)

	return record_list
	logger.info("for domain: %s and index: %s found a total of %d hits." % (domain,index,len(record_list)))


def download_page(record):

	offset, length = int(record['offset']), int(record['length'])
	offset_end = offset + length - 1

	prefix = 'https://aws-publicdatasets.s3.amazonaws.com/'
	resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})

	raw_data = StringIO.StringIO(resp.content)
	f = gzip.GzipFile(fileobj=raw_data)

	data = f.read()
	return data

def clean_warc(input):

	content = input.split('\n')


	date = content[2].split(':')[1].strip()
	url = content[9].split(':',1)[1].strip()

	html = content[39:]

	html = '\n'.join(html).strip().decode('utf8')

	h = html2text.HTML2Text()
	h.ignore_links = True
	text = h.handle(html)
	text = re.sub('[^A-Za-z0-9\.]+', ' ', text)

	return text, date, url

def run(indices):

	for index in indices:

		meta_data = ''
		index_dir = 'data/' + index
		os.makedirs(index_dir)

		csv_file = open(index_dir +'/meta_data.csv', 'w')

		for domain in domains:


			domain_dir = index_dir +'/'+ str(domain)
			os.makedirs(domain_dir)

			records_list = fetch_records(domain, index)

			for idx, record in enumerate(records_list):


				warc_data = download_page(record)
				text_data, date, url = clean_warc(warc_data)

				csv_file.write('%s,%s,%s\n' % (domain, url, date))
				with open(domain_dir + '/' + str(idx)  + '.text', "w") as text_file:
					text_file.write(text_data)
		csv_file.close()

global domains

with open ('domains.txt', 'rb') as f:
	domains = f.read().split('\n')
with open ('indices.txt', 'rb') as f:
	indices = f.read().split('\n')

pool = ThreadPool(20)
results = pool.map(run, indices)

