# -*- coding: utf-8 -*-

import logging
import re
import arrow

log = logging.getLogger(__name__)

#TODO these should be moved into general file
LICENSES = [{
        "name": "Creative Commons Zero 1.0 Universal",
        "id": "cc-zero",
        "url":"http://creativecommons.org/publicdomain/zero/1.0/"
    },
    {
        "name": "Creative Commons Attribution 4.0 International License",
        "id": "cc-by",
        "url":"http://creativecommons.org/licenses/by/4.0/"
    },
    {
        "name": "Irish PSI General Licence No.: 2005/08/01",
        "id": "psi",
        "url":"http://psi.gov.ie/"
    },
    {
        "name": "Open Data Commons Public Domain Dedication and License",
        "id": "pddl",
        "url":"http://opendatacommons.org/licenses/pddl/"
    },
    {
        "name": "Open Data Commons Attribution License",
        "id": "odc-by",
        "url":"http://opendatacommons.org/licenses/by/"
    },
    {
        "name": "Open Data Commons Open Database License",
        "id": "odc-odbl",
        "url":"http://opendatacommons.org/licenses/odbl/"
    },
    {
        "name": "Other License (Attribution)",
        "id": "other-at",
        "url":""
    },
    {
        "name": "Copyright",
        "id": "copyright",
        "url":""
    },
]

themes = ["Agriculture", "Arts", "Crime", "Economy", "Education", "Energy", "Environment", "Government", "Health", "Housing", "Society", "Science", "Towns", "Transport"]

def normalize_name (string):
    string = string.strip().lower()
    string = re.sub('\s*&\s*', ' and ', string)
    string = re.sub('\s+', ' ', string) #squeeze whitespace
    string = string.replace(' ', '_')   #space to underscore
    string = string.replace('-', '_')   #dash to underscore
    string = string.encode('utf-8')
    # Remove fadas from Irish names ('Met Éireann' => 'met-eireann')
    string = string.replace('Á', 'a').replace('á', 'a')
    string = string.replace('É', 'e').replace('é', 'e')
    string = string.replace('Í', 'i').replace('í', 'i')
    string = string.replace('Ó', 'o').replace('ó', 'o')
    string = string.replace('Ú', 'u').replace('ú', 'u')
    string = string.replace('D\xc4\x82\xc5\x9fn', 'Dun').replace('d\xc4\x82\xc5\x9fn', 'dun')
    string = re.sub('\W', '', string)   #remove non-alphanumeric
    string = re.sub('\_+', '_', string) #squeeze underscore
    string = string.replace('_', '-')   #underscore to dash
    # we use the last 100 chars as its marginally less likely to cause collision
    return string[-100:] #url names have max length of 100 chars

def dcat_to_ckan(dcat_dict):

    package_dict = {}

    package_dict['title'] = dcat_dict.get('title')
    package_dict['notes'] = dcat_dict.get('description')
    package_dict['url'] = dcat_dict.get('landingPage')


    package_dict['tags'] = []
    for keyword in dcat_dict.get('keyword', []):
        package_dict['tags'].append({'name': keyword})

    package_dict['date_released'] = arrow.get(dcat_dict.get('issued')).format('DD/MM/YYYY')
    package_dict['date_updated'] = arrow.get(dcat_dict.get('modified')).format('DD/MM/YYYY')

    package_dict['guid'] = dcat_dict.get('identifier')

    dcat_publisher = dcat_dict.get('publisher')
    if isinstance(dcat_publisher, basestring):
        package_dict['dcat_publisher_name'] = dcat_publisher
    elif isinstance(dcat_publisher, dict) and dcat_publisher.get('name'):
        package_dict['dcat_publisher_title'] = dcat_publisher.get('name')
        package_dict['dcat_publisher_name'] = dcat_publisher.get('name')
        package_dict['dcat_publisher_email'] = dcat_publisher.get('mbox', '-')
        package_dict['dcat_publisher_phone'] = dcat_publisher.get('phone', '-')

    if not package_dict.get('dcat_publisher_name'):
        package_dict['dcat_publisher_name'] = 'Ordnance Survey Ireland'
        package_dict['dcat_publisher_title'] = 'Ordnance Survey Ireland'

    package_dict['owner_org'] = normalize_name(package_dict['dcat_publisher_name'])

    package_dict['collection-name'] = 'arcgis'

    contactPoint = dcat_dict.get('contactPoint')
    package_dict['contact-name'] = contactPoint.get('fn', '-')
    package_dict['contact-email'] = contactPoint.get('hasEmail', '-')
    package_dict['contact-phone'] = contactPoint.get('phone', '-')

    #we have to go pull the license.json and check if the license URL is in the 'link' or 'description' field

    try:
        license_dict = requests.get(dcat_dict['license']).json()
        package_dict['license_id'] = (filter(lambda l: l['url'] and l['url'] in license_dict['description']))[0]['id']
    except:
        package_dict['license_id'] = 'other'

    package_dict['geographic_coverage-other'] = dcat_dict.get('spatial', '')

    if dcat_dict.get('language'):
        package_dict['language'] = ','.join(dcat_dict.get('language', []))
    else:
        package_dict['language'] = 'eng'

    #lowercase themes, lowercase keywords
    kw = [k.lower() for k in dcat_dict.get('keyword', [])]
    th = [t.lower() for t in themes]

    #intersect the set
    #package_dict['theme-primary'] = set(th).intersection(kw).pop().capitalize()
    package_dict['theme-primary'] = "Environment"

    package_dict['resources'] = []
    for distribution in dcat_dict.get('distribution', []):
        resource = {
            'name': distribution.get('title'),
            'description': distribution.get('description'),
            'format': distribution.get('format'),
        }

        if distribution.get('downloadUrl'):
            resource['url'] = distribution.get('downloadUrl')
        elif distribution.get('downloadURL'):
            resource['url'] = distribution.get('downloadURL')
        elif distribution.get('accessUrl'):
            resource['url'] = distribution.get('accessUrl')
        elif distribution.get('accessURL'):
            resource['url'] = distribution.get('accessURL')

        if distribution.get('byteSize'):
            try:
                resource['size'] = int(distribution.get('byteSize'))
            except ValueError:
                pass
        package_dict['resources'].append(resource)

    return package_dict


def ckan_to_dcat(package_dict):

    dcat_dict = {}

    dcat_dict['title'] = package_dict.get('title')
    dcat_dict['description'] = package_dict.get('notes')
    dcat_dict['landingPage'] = package_dict.get('url')


    dcat_dict['keyword'] = []
    for tag in package_dict.get('tags', []):
        dcat_dict['keyword'].append(tag['name'])


    dcat_dict['publisher'] = {}

    for extra in package_dict.get('extras', []):
        if extra['key'] in ['dcat_issued', 'dcat_modified']:
            dcat_dict[extra['key'].replace('dcat_', '')] = extra['value']

        elif extra['key'] == 'language':
            dcat_dict['language'] = extra['value'].split(',')

        elif extra['key'] == 'dcat_publisher_name':
            dcat_dict['publisher']['name'] = extra['value']

        elif extra['key'] == 'dcat_publisher_email':
            dcat_dict['publisher']['mbox'] = extra['value']

        elif extra['key'] == 'guid':
            dcat_dict['identifier'] = extra['value']

    if not dcat_dict['publisher'].get('name') and package_dict.get('maintainer'):
        dcat_dict['publisher']['name'] = package_dict.get('maintainer')
        if package_dict.get('maintainer_email'):
            dcat_dict['publisher']['mbox'] = package_dict.get('maintainer_email')

    dcat_dict['distribution'] = []
    for resource in package_dict.get('resources', []):
        distribution = {
            'title': resource.get('name'),
            'description': resource.get('description'),
            'format': resource.get('format'),
            'byteSize': resource.get('size'),
            # TODO: downloadURL or accessURL depending on resource type?
            'accessURL': resource.get('url'),
        }
        dcat_dict['distribution'].append(distribution)

    return dcat_dict
