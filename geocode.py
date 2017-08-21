"""
A script to clean up some old code and normalize
various fields
"""
from firebase import firebase
import requests
from lxml import etree
import os


def geocode():

    reports = firebase.database().child('reports').get().each()

    for report in reports:
        val = report.val()

        updates = {}

        #
        #
        #
        if 'position' not in val.keys():

            resp = requests.get(
                    'https://geoservices.tamu.edu/Services/Geocode/WebService/GeocoderWebServiceHttpNonParsed_V04_01.aspx',
                    data={
                        'apiKey': os.environ.get('GEOCODE_API_KEY'),
                        'version': '4.01',
                        'streetAddress': updates.get('finalLocation', None) or val.get('finalLocation', None),
                        'city': 'Melrose',
                        'state': 'MA',
                        'zip': '02176',
                        'format': 'xml',
                    })

            cleaned = resp.text.replace('<?xml version="1.0" encoding="utf-8"?>', '')[3:]

            root = etree.XML(cleaned)

            lat = root.findtext('.//Latitude')
            lng = root.findtext('.//Longitude')

            if lat is not None:

                updates['position'] = {
                    'lat': lat,
                    'lng': lng,
                }

        if len(updates.keys()) > 0:
            print "Updating %s %s" % (val['incidentNumber'], updates)
            firebase.database().child('reports').child(report.key()).update(updates)

if __name__ == '__main__':
    geocode()
