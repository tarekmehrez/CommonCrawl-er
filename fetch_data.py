import StringIO
import gzip
import html2text
import json
import logging
import os
import re
import requests
import sys
import shutil

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

	result = '%s\n%s\n%s' % (text, date, url)
	return result

def run(item):

	domain = item[0]
	index = item[1]
	dir = 'data/%s-%s' %(domain, index)
	records_list = fetch_records(domain, index)
	for idx, record in enumerate(records_list):
		warc_data = download_page(record)
		text_file = clean_warc(warc_data)
		with open('%s/%s.text' % (dir, str(idx)), 'wb') as f:
			f.write(text_file)

def create_dirs(data):
	if os.path.exists('data'):
		shutil.rmtree('data')
	os.makedirs('data')
	for d in data:
		os.makedirs('data/%s-%s' % (d[0],d[1]))


data_arr = []

with open ('domains.txt', 'rb') as f:
	domains = f.read().split('\n')
with open ('indices.txt', 'rb') as f:
	indices = f.read().split('\n')

for d in domains:
	for i in indices:
		data_arr.append([d,i])

create_dirs(data_arr)

pool = ThreadPool(20)
esults = pool.map(run, data_arr)

