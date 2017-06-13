"""
XXXXXX
"""

from firebase import firebase
import csv
import datetime


def write_row(writer, data):

    print data
    print '\n'

    dt = datetime.datetime.strptime(data.get('isotime'), "%Y-%m-%dT%H:%M:%S")

    writer.writerow([
        data.get('incidentTitle'),
        data.get('isotime'),
        dt.strftime("%x"),
        dt.strftime("%m"),
        dt.strftime("%Y"),
        dt.strftime("%X"),
        dt.strftime("%W"),
        data.get('finalLocation'),
        data.get('category'),
        data.get('narrative'),
    ])


def get_writer(file_object):

    writer = csv.writer(file_object, delimiter=',',
                        quotechar='"', quoting=csv.QUOTE_MINIMAL)

    writer.writerow([
        'title',
        'isodate',
        'date',
        'month',
        'year',
        'time',
        'week',
        'location',
        'category',
        'narrative',
    ])

    return writer


def queryResult():

    reports = firebase.database().child('reports').get().each()

    with open('all_incidents.csv', 'w') as csvfile:

        writer = get_writer(csvfile)

        for report in reports:
            print report.key()
            val = report.val()

            write_row(writer, val)

    with open('all_directed_patrols.csv', 'w') as csvfile:

        writer = get_writer(csvfile)

        for report in reports:
            val = report.val()

            if val.get('category') == 'Directed Patrol':

                write_row(writer, val)


    with open('upham_directed_patrols.csv', 'w') as csvfile:

        writer = get_writer(csvfile)

        for report in reports:

            val = report.val()

            if val.get('category') == 'Directed Patrol' and 'UPHAM' in val.get('finalLocation', ''):

                write_row(writer, val)

queryResult()