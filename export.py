"""
XXXXXX
"""

from firebase import firebase
import csv
import datetime
from dateutil.parser import parse

shift1 = parse('07:45:00')
shift2 = parse('15:45:00')
shift3 = parse('23:45:00')


def get_shift(time_str):

    subject = parse(time_str)

    if subject > parse('07:45:00') and subject < shift2:
        return 'FIRST'
    elif subject > shift2 and subject < shift3:
        return 'SECOND'
    else:
        return 'THIRD'

def write_row(writer, data):

    dt = datetime.datetime.strptime(data.get('isotime'), "%Y-%m-%dT%H:%M:%S")

    writer.writerow([
        data.get('incidentTitle'),
        data.get('incidentName'),
        data.get('disposition'),
        data.get('isotime'),
        dt.strftime("%x"),
        dt.strftime("%m"),
        dt.strftime("%Y"),
        dt.strftime("%X"),
        dt.strftime("%W"),
        get_shift(dt.strftime("%X")),
        data.get('finalLocation'),
        data.get('category'),
        data.get('narrative'),
    ])


def get_writer(file_object):

    writer = csv.writer(file_object, delimiter=',',
                        quotechar='"', quoting=csv.QUOTE_MINIMAL)

    writer.writerow([
        'title',
        'name',
        'disposition',
        'isodate',
        'date',
        'month',
        'year',
        'time',
        'week',
        'shift',
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


    with open('upham.csv', 'w') as csvfile:

        writer = get_writer(csvfile)

        for report in reports:

            val = report.val()

            if 'upham' in val.get('finalLocation', '').lower():

                write_row(writer, val)

    with open('howard.csv', 'w') as csvfile:

        writer = get_writer(csvfile)

        for report in reports:

            val = report.val()

            if 'howard' in val.get('finalLocation', '').lower():

                write_row(writer, val)

queryResult()