"""
A script to clean up some old code and normalize
various fields
"""

from firebase import firebase
from datetime import datetime
import time
from dispositions import COMMON_DISPOSITIONS
import re


def cleanup():

    reports = firebase.database().child('reports').get().each()

    for report in reports:
        val = report.val()

        keys = val.keys()

        updates = {}


        #
        #   fix old keys
        #
        if '-' in report.key():
            print "Deleting %s" % report.key()
            firebase.database().child('reports').child(report.key()).remove()
            continue

        #
        #   If a key starts with anything other than 1, somewthing went wront
        #
        if report.key()[0] != '1':
            print "Deleting %s" % report.key()
            firebase.database().child('reports').child(report.key()).remove()
            continue

        #
        #   If a date gets messed up somehow, just delete it
        #
        if not val['date']:
            print "Deleting %s" % report.key()
            firebase.database().child('reports').child(report.key()).remove()
            continue

        #
        #   create a common location property
        #
        if 'finalLocation' not in keys:

            for word in ['locationAddress', 'vicinityOf', 'location']:

                if word in val.keys():
                    final_location = val[word]
                    updates['finalLocation'] = final_location
                    break

        #
        #   clean up the final location property
        #
        if 'finalLocation' in keys:

            #
            #   First, remove the prefix that's between brackets
            #
            cleaned_location = re.sub(r'\[.*\]', '', val['finalLocation']).strip()

            #
            #   Now try to remove the name of the address and start with a number,
            #   for cases like "Milano Center - 20 Main St"
            #
            starting_with_number = re.search(r'\d.*', cleaned_location)

            replacement = starting_with_number.group(0) if starting_with_number is not None else cleaned_location

            #
            #   Try and remove apartment numbers
            #
            replacement = re.sub(r'Apt\. #\d+', '', replacement).strip()

            if replacement != val['finalLocation']:
                updates['finalLocation'] = replacement


        #
        #   Remove bad data from the incident title
        #
        if 'incidentTitle' in val.keys() and 'Location/Address' in val['incidentTitle']:
            try:
                cleaned_title = re.search('^.*(?=(Location/Address))', val['incidentTitle'], flags=0).group(0).strip()
                updates['incidentTitle'] = cleaned_title
            except:
                pass

        #
        #   categorize directed patrols
        #
        if 'incidentTitle' in val.keys() and not val['category']:

            if 'directed' in val['incidentTitle'].lower():

                updates['category'] = 'Directed Patrol'

            elif 'motor vehicle stop' in val['incidentTitle'].lower():

                updates['category'] = 'Motor Vehicle Stop'

            elif 'medical emergency' in val['incidentTitle'].lower():

                updates['category'] = 'Medical Emergency'

        #
        #   create various date formats
        #
        if 'isotime' not in keys or val['isotime'] is None:

            try:
                date_string = "%s %s" % (val['date'], val['incidentTime'])
                d = datetime.strptime(date_string, '%m/%d/%Y %H%M')
                updates['isotime'] = d.isoformat()
            except:
                pass

        try:
            if 'timestamp' not in val.keys():
                date_string = "%s %s" % (val['date'], val['incidentTime'])
                dt = datetime.strptime(date_string, '%m/%d/%Y %H%M')
                updates['timestamp'] = time.mktime(dt.timetuple())
        except:
            pass

        try:
            if 'disposition' not in val.keys() or val['disposition'] == '':
                patt = '|'.join(COMMON_DISPOSITIONS)
                disposition = re.search(patt, val['incidentTitle'], flags=0).group(0).strip()
                updates['disposition'] = disposition
        except:
            print "Couldn't find disposition for '%s'" % val['incidentTitle']

        if 'incidentName' not in val.keys() or val['incidentName'] == '':
            patt = '|'.join(COMMON_DISPOSITIONS)
            disposition = re.search(patt, val['incidentTitle'], flags=0).group(0).strip()
            name = val['incidentTitle'].replace(disposition, '').replace('*', '').strip()
            updates['incidentName'] = name

        #
        #   Remove bad data from the incident name
        #
        if 'incidentName' in val.keys() and 'Location/Address' in val['incidentName']:
            try:
                cleaned_name = re.search('^.*(?=(Location/Address))', val['incidentName'], flags=0).group(0).strip()
                updates['incidentName'] = cleaned_name
            except:
                pass

        #
        #   Remove generic position data like
        #   {u'lat': 42.4457707, u'lng': -71.0601603}
        #
        if 'position' in val.keys():

            if val['position'] == 'Rate Limited':
                updates['position'] = None

            if type(val['position']) is list:
                updates['position'] = None

            if type(val['position']) is dict and val['position']['lat'] == '42.459952' and val['position']['lng'] == '-71.05889':
                updates['position'] = None

        if len(updates.keys()) > 0:
            print "Updating %s %s" % (val['incidentNumber'], updates)
            firebase.database().child('reports').child(report.key()).update(updates)

if __name__ == '__main__':
    cleanup()
