import nltk
import html2text
import sys
import BeautifulSoup
import re


path = 'data/theguardian.com/2015-40/1.warc'

with open(path, 'rb') as f:
	content = f.read().split('\n')



date = content[2].split(':')[1].strip()
url = content[9].split(':',1)[1].strip()

html = content[39:]

html = '\n'.join(html).strip().decode('utf8')

h = html2text.HTML2Text()
h.ignore_links = True
text = h.handle(html)



text = re.sub('[^A-Za-z0-9\.]+', ' ', text)



print text

print date,url


