"""
A script to clean up some old code and normalize
various fields
"""
from firebase import firebase
import requests
from lxml import etree
import os
import time


def query_google(address):

    resp = requests.get(
            'https://maps.googleapis.com/maps/api/geocode/json',
            params={
                'address': "%s, Melrose, MA 02176" % address,
            })

    data = resp.json()

    if data['status'] != 'OK':
        return None

    try:
        position = data['results'][0].get('geometry', {}).get('location', None)
        return {
            'lat': position.get('lat', ''),
            'lng': position.get('lng', ''),
        }
    except:
        return None


def query_location_iq(address):

    resp = requests.get(
            'http://locationiq.org/v1/search.php',
            params={
                'key': os.environ.get('LOCATIONIQ_GEOCODE_API_KEY'),
                'format': 'json',
                'q': "%s, Melrose, MA 02176" % address,
            })

    data = resp.json()

    try:
        if data.get('error', None) == 'Rate Limited':
            return 'Rate Limited'
    except:
        pass

    if len(data) is 0:
        return None

    try:
        return {
            'lat': data[0].get('lat'),
            'lng': data[0].get('lon'),
        }
    except:
        return None


def query_texas(address):

    resp = requests.get(
            'https://geoservices.tamu.edu/Services/Geocode/WebService/GeocoderWebServiceHttpNonParsed_V04_01.aspx',
            data={
                'apiKey': os.environ.get('TEXAS_GEOCODE_API_KEY'),
                'version': '4.01',
                'streetAddress': address,
                'city': 'Melrose',
                'state': 'MA',
                'zip': '02176',
                'format': 'xml',
            })

    cleaned = resp.text.replace('<?xml version="1.0" encoding="utf-8"?>', '')[3:]

    root = etree.XML(cleaned)

    print root.findtext('.//QueryStatusCodeValue')

    #
    #   If this code is 470, we've hit a rate limit,
    #   so pause for 5 minutes
    #
    if root.findtext('.//QueryStatusCodeValue') == '470':
        time.sleep(500)

    lat = root.findtext('.//Latitude')
    lng = root.findtext('.//Longitude')

    if lat is not None:

        return {
            'lat': lat,
            'lng': lng,
        }

    return None


def geocode():

    reports = firebase.database().child('reports').get().each()

    #
    #   Find reports that we need positions for
    #
    reports_without_positions = [report for report in reports if report.val().get('position', None) is not None]

    print "Found %s reporst without positions" % len(reports_without_positions)

    for report in reports:
        val = report.val()

        updates = {}

        finalLocation = updates.get('finalLocation', None) or val.get('finalLocation', None)

        if 'position' in val.keys() and val['position'] is not None:
            continue

        if 'position' not in val.keys() and finalLocation is not None:

            result = query_location_iq(finalLocation)

            if result == 'Rate Limited':
                time.sleep(5)
                print "We hit a rate liit. Pausing for 5 seconds"
                result = query_location_iq(finalLocation)

            if result is not None and result != 'Rate Limited':
                print 'Found location from LocationIQ'

            if result is None or result == 'Rate Limited':
                result = query_google(finalLocation)

                if result is not None and result != 'Rate Limited':
                    print 'Found location from Google'

            if result == 'Rate Limited':
                result = None

            updates['position'] = result

            if result is None:
                print "Couldn't get position for: %s" % finalLocation

            if len(updates.keys()) > 0:
                print "Updating %s %s" % (val['incidentNumber'], updates)
                firebase.database().child('reports').child(report.key()).update(updates)

if __name__ == '__main__':
    geocode()
