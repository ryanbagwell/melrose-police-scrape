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
import time
from firebase import firebase
from cleanup import cleanup
from dateutil.parser import parse
from datetime import datetime, timedelta

errors = []

delimiter = '-------'

def log_error(description, error, original):
    msg = "%s: %s\n%s" % (description, error, original)
    errors.append(msg)


def to_camelcase(str):

    slug = slugify(str)
    split = slug.lower().split('-')
    split = [s.capitalize() if i > 0 else s for i, s in enumerate(split)]
    return ''.join(split)

patterns = {
    'incident_number': '(?P<incident_number>^1\d-\d{1,6})\s.+',
    'incident_time': '\d{2}-\d{1,6}\s(?P<incident_time>\d{4})',
    'incident_title': '\d{2}-\d{1,6}\s\d{4}(?P<incident_title>.+)',
    'incident_date': 'Date: (?P<date>\d{1,2}/\d{1,2}/\d{4})',
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
    except Exception as e:
        log_error("Error pasing incident_date", e, text)

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
    except Exception as e:
        log_error("Error parsing incident_number field", e, text)
        data['incident_number'] = 'unable to parse'

    try:
        data['incident_time'] = re.search(patterns['incident_time'], cleaned, flags=0).group('incident_time')
    except Exception as e:
        log_error("Error parsing incident_time field", e, text)
        data['incident_time'] = 'unable to parse'

    try:
        data['incident_title'] = re.search(patterns['incident_title'], cleaned, flags=0).group('incident_title').strip()
    except Exception as e:
        log_error("Error parsing incident_title field", e, text)
        data['incident_title'] = 'unable to parse'

    try:
        date_string = "%s %s" % (data['date'], data['incident_time'])
        d = datetime.strptime(date_string, '%m/%d/%Y %H%M')
        data['isotime'] = d.isoformat()
    except Exception as e:
        log_error("Error parsing isotime field", e, text)
        data['isotime'] = 'unable to parse'

    try:
        date_string = "%s %s" % (data['date'], data['incident_time'])
        dt = datetime.strptime(date_string, '%m/%d/%Y %H%M')
        data['timestamp'] = time.mktime(dt.timetuple())
    except Exception as e:
        log_error("Error parsing timestamp field", e, text)
        data['timestamp'] = 'unable to parse'

    for word in keywords:
        pattern = '(?P<key>%s):(?P<value>.{0,1000})' % word

        try:
            results = re.findall(pattern, cleaned, flags=0)
            if results:
                data[results[0][0]] = '. '.join([result[1] for result in results]).strip()
        except Exception as e:
            log_error("Error parsing %s field" % word, e, text)

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

    except Exception as e:
        log_error("Error parsing citation fields", e, text)

    data['num_penalty_types'] = len([True for l in ['verbal_warning',
                                                            'written_warning',
                                                            'unspecified_warning',
                                                            'citation'] if data[l]])

    return data, cleaned, text


current_date = ''


class PoliceReportSpider(scrapy.Spider):
    name = 'Police Report Spider'
    start_urls = ['http://melrosepolice.net/police-logs/']
    weeks_to_scrape = 10

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

                incident_number_result = re.search(patterns['incident_number'],
                                                   value, flags=0)

                if incident_number_result:
                    final.append(delimiter)

                date_result = re.search(patterns['incident_date'],
                                        value, flags=0)

                if date_result:
                    final.append(delimiter)

                final.append(value.strip())

                return final

            els = reduce(reducer, els, [])

            """
            Check to make sure we have 7 date delimiters

            """
            def date_reducer(final, value):

                date_result = re.search(patterns['incident_date'],
                                        value, flags=0)

                if date_result:
                    final.append(date_result.group('date'))

                return final

            dates = reduce(date_reducer, els, [])

            """
            If we didn't find 7 date delimiters, try to fix it
            """
            if len(dates) is not 7:

                """
                Check if the first date label was missing from the document.
                It should be in the second array value.

                """
                date_result = re.search(patterns['incident_date'],
                                        els[1], flags=0)

                if not date_result and len(dates) == 6:
                    last = parse(dates[-1])
                    first = last - timedelta(days=6)
                    els = [delimiter, 'For Date: %s' % first.strftime('%m/%d/%Y')] + els

                else:

                    """
                    If one of the other dates is missing,
                    raise an exception because I don't think it will be
                    possible to figure out where the other dates
                    belong
                    """
                    raise Exception("Only %s dates exist in this document (%s). I'm stopping here." % (len(dates), ', '.join(dates)))

            els = '\n'.join(els)

            def mapper(item):
                global current_date

                try:
                    current_date = re.search(patterns['incident_date'],
                                             item.strip('\t\n\r'), flags=0).group('date')
                    return None
                except Exception as e:
                    pass
                    # log_error("Error parsing current date", e, item)

                parsed, cleaned, raw = incident_parser(item, current_date)

                return parsed

            els = map(mapper, els.split('-------'))

            existing_reports = firebase.database().child('reports').shallow().get().each()

            for i, el in enumerate(els):

                if el is None:
                    continue

                cleaned_dict = {}

                for k, v in el.iteritems():
                    cleaned_dict[to_camelcase(k)] = v

                if (cleaned_dict['incidentNumber'] == 'unable to parse'):
                    continue

                numerical_id = cleaned_dict['incidentNumber'].replace('-', '')

                if numerical_id not in existing_reports:
                    print "Saving %s" % cleaned_dict['incidentNumber']
                    self.db.child('reports').child(numerical_id).update(cleaned_dict)
                else:
                    print "%s already exists" % cleaned_dict['incidentNumber']

        links = response.css('a::attr(href)').extract()

        links = [link for link in links if 'docx' in link]

        for link in links[:self.weeks_to_scrape]:
            yield scrapy.Request(response.urljoin(link), callback=self.parse)

        if errors:
            print "Couldn't parse %s incidents" % len(errors)

            for error in errors:
                print error
                print '\n'

        cleanup()
