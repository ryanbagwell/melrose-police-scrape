"""
A script to clean up some old code and normalize
various fields
"""

from firebase import firebase
from datetime import datetime
import time

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

        if len(updates.keys()) > 0:
            print "Updating %s %s" % (val['incidentNumber'], updates)
            firebase.database().child('reports').child(report.key()).update(updates)

if __name__ == '__main__':
    cleanup()
