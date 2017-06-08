"""
To Run:
 FIREBASE_API_KEY=<the api key> \
 FIREBASE_DOMAIN=<the domain> \
 FIREBASE_DB_URL=<the db url> \
 scrapy runspider scrape.py
"""


import scrapy
import zipfile
from StringIO import StringIO
from bs4 import BeautifulSoup
import re
from slugify import slugify
from datetime import datetime
import time
from firebase import firebase
from cleanup import cleanup



def to_camelcase(str):

    slug = slugify(str)
    split = slug.lower().split('-')
    split = [s.capitalize() if i > 0 else s for i, s in enumerate(split)]
    return ''.join(split)

patterns = {
    'incident_number': '(?P<incident_number>^\d{2}-\d{1,6})\s.+',
    'incident_time': '\d{2}-\d{1,6}\s(?P<incident_time>\d{4})',
    'incident_title': '\d{2}-\d{1,6}\s\d{4}(?P<incident_title>.+)',
    'incident_date': '^For Date: (?P<date>.+)',
}


def incident_parser(text, incident_date):

    text = text.strip(' \t\n\r')

    keywords = [
        'Locati.+',
        'ID',
        'Narrative',
        'Refer To Incident',
        'Refer To Summons',
        'Vicinity of',
    ]

    lines = text.split('\n')

    def reducer(final, value):

        pattern = '(?P<key>%s):(?P<value>.{0,1000})' % '|'.join(keywords)

        result = re.search(pattern, value, flags=0)

        length = len(final)

        if result or length is 0:
            final.append(value)
        else:
            last_value = final[-1]
            final[-1] = last_value + value.replace('\n', '')

        return final

    cleaned = reduce(reducer, lines, [])

    cleaned = '\n'.join(cleaned)

    try:
        incident_date = re.search('(?P<incident_date>\d{2}/\d{2}/\d{4})', incident_date).group('incident_date')
    except:
        pass

    data = {
        'incident_number': '',
        'date': incident_date,
        'original': text,
        'category': '',
        'num_citations': 0,
        'num_written_warnings': 0,
        'num_verbal_warnings': 0,
        'num_unspecified_warnings': 0,
        'num_penalty_types': 0,
        'verbal_warning': False,
        'written_warning': False,
        'unspecified_warning': False,
        'citation': False,
        'multiple': False,
        'Narrative': '',
    }

    try:
        data['incident_number'] = re.search(patterns['incident_number'], cleaned, flags=0).group('incident_number')
    except:
        data['incident_number'] = 'unable to parse'

    try:
        data['incident_time'] = re.search(patterns['incident_time'], cleaned, flags=0).group('incident_time')
    except:
        data['incident_time'] = 'unable to parse'

    try:
        data['incident_title'] = re.search(patterns['incident_title'], cleaned, flags=0).group('incident_title').strip()
    except:
        data['incident_title'] = 'unable to parse'

    try:
        date_string = "%s %s" % (data['date'], data['incident_time'])
        d = datetime.strptime(date_string, '%m/%d/%Y %H%M')
        data['isotime'] = d.isoformat()
    except:
        data['isotime'] = 'unable to parse'

    try:
        date_string = "%s %s" % (data['date'], data['incident_time'])
        dt = datetime.strptime(date_string, '%m/%d/%Y %H%M')
        data['timestamp'] = time.mktime(dt.timetuple())
    except:
        data['timestamp'] = 'unable to parse'

    for word in keywords:
        pattern = '(?P<key>%s):(?P<value>.{0,1000})' % word

        try:
            results = re.findall(pattern, cleaned, flags=0)
            if results:
                data[results[0][0]] = '. '.join([result[1] for result in results]).strip()
        except:
            pass

    try:
        if re.search('verbal', data['Narrative'], re.IGNORECASE) and not re.search('argument', data['Narrative'], re.IGNORECASE):
            data['verbal_warning'] = True
            data['num_verbal_warnings'] = 1

        if re.search('written', data['Narrative'], re.IGNORECASE):
            data['written_warning'] = True
            data['num_written_warnings'] = 1

        if re.search('warn', data['Narrative'], re.IGNORECASE) and not data['written_warning'] and not data['verbal_warning']:
            data['unspecified_warning'] = True
            data['num_unspecified_warnings'] = 1

        if re.search('citation', data['Narrative'], re.IGNORECASE):
            data['citation'] = True
            data['num_citations'] = 1

    except:
        print data

    data['num_penalty_types'] = len([True for l in ['verbal_warning',
                                                            'written_warning',
                                                            'unspecified_warning',
                                                            'citation'] if data[l]])

    return data, cleaned, text


current_date = ''


class PoliceReportSpider(scrapy.Spider):
    name = 'Police Report Spider'
    start_urls = ['http://melrosepolice.net/police-logs/']
    weeks_to_scrape = 1

    def __init__(self, *args, **kwargs):
        super(PoliceReportSpider, self).__init__(*args, **kwargs)

        self.db = firebase.database()

    def parse(self, response):

        if 'docx' in response.url:
            docx = StringIO(response.body)
            zip = zipfile.ZipFile(docx)
            text = zip.read('word/document.xml')
            soup = BeautifulSoup(text)
            els = [el.text.encode('ascii', errors='ignore') for el in soup.find_all('w:t')]

            def reducer(final, value):

                incident_number_result = re.search(patterns['incident_number'], value, flags=0)

                if incident_number_result:
                    final.append('-------')

                date_result = re.search(patterns['incident_date'], value, flags=0)

                if date_result:
                    final.append('-------')

                if re.search('Melrose Police Department Page', value, flags=0):
                    final.append('-------')

                final.append(value)

                return final

            els = reduce(reducer, els, [])

            els = '\n'.join(els)

            def mapper(item):
                global current_date

                try:
                    current_date = re.search(patterns['incident_date'],
                                             item.strip(' \t\n\r'), flags=0).group('date')
                except:
                    pass

                parsed, cleaned, raw = incident_parser(item, current_date)

                return parsed

            els = map(mapper, els.split('-------'))

            for i, el in enumerate(els):

                cleaned_dict = {}

                for k, v in el.iteritems():
                    cleaned_dict[to_camelcase(k)] = v

                if (cleaned_dict['incidentNumber'] == 'unable to parse'):
                    continue

                numerical_id = cleaned_dict['incidentNumber'].replace('-', '')

                report = self.db.child('reports').child(numerical_id)

                if not report.get().val():
                    print "Saving %s" % cleaned_dict['incidentNumber']
                    self.db.child('reports').child(numerical_id).update(cleaned_dict)
                else:
                    print "%s already exists" % cleaned_dict['incidentNumber']

        links = response.css('a::attr(href)').extract()

        links = [link for link in links if 'docx' in link]

        for link in links[:self.weeks_to_scrape]:
            yield scrapy.Request(response.urljoin(link), callback=self.parse)

        cleanup()
