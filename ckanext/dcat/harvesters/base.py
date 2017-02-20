import os
import uuid
import logging

import requests

from ckan import plugins as p
from ckan import logic
from ckan import model

from ckan.lib.navl.validators import (ignore_missing,
                                      not_empty,
                                      empty,
                                      ignore,
                                      missing,
                                      not_missing,
                                      keep_extras,
                                      )
from ckan.logic.converters import date_to_db, date_to_form, convert_to_extras, convert_from_extras

from ckanext.dgu.forms.validators import merge_resources, unmerge_resources, \
     validate_resources, \
     validate_additional_resource_types, \
     validate_data_resource_types, \
     validate_license, \
     drop_if_same_as_publisher, \
     populate_from_publisher_if_missing, \
     remove_blank_resources, \
     allow_empty_if_inventory, \
     validate_theme, \
     validate_language

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra

import sys
sys.path.append('/src/dgi-harvesters/lib')
import utils

log = logging.getLogger(__name__)


class DCATHarvester(HarvesterBase):

    MAX_FILE_SIZE = 1024 * 1024 * 50  # 50 Mb
    CHUNK_SIZE = 1024

    force_import = False

    _user_name = None

    def _get_content(self, url, harvest_job, page=1):
        if not url.lower().startswith('http'):
            # Check local file
            if os.path.exists(url):
                with open(url, 'r') as f:
                    content = f.read()
                return content
            else:
                self._save_gather_error('Could not get content for this url', harvest_job)
                return None

        try:
            if page > 1:
                url = url + '&' if '?' in url else url + '?'
                url = url + 'page={0}'.format(page)


            log.debug('Getting file %s', url)

            # first we try a HEAD request which may not be supported
            did_get = False
            r = requests.head(url)
            if r.status_code == 405 or r.status_code == 400:
                r = requests.get(url, stream=True)
                did_get = True
            r.raise_for_status()

            cl = r.headers.get('content-length')
            if cl and int(cl) > self.MAX_FILE_SIZE:
                msg = '''Remote file is too big. Allowed
                    file size: {allowed}, Content-Length: {actual}.'''.format(
                    allowed=self.MAX_FILE_SIZE, actual=cl)
                self._save_gather_error(msg, harvest_job)
                return None

            if not did_get:
                r = requests.get(url, stream=True)

            length = 0
            content = ''
            for chunk in r.iter_content(chunk_size=self.CHUNK_SIZE):
                content = content + chunk
                length += len(chunk)

                if length >= self.MAX_FILE_SIZE:
                    self._save_gather_error('Remote file is too big.', harvest_job)
                    return None

            return content

        except requests.exceptions.HTTPError, error:
            if page > 1 and error.response.status_code == 404:
                # We want to catch these ones later on
                raise

            msg = 'Could not get content. Server responded with %s %s' % (
                error.response.status_code, error.response.reason)
            self._save_gather_error(msg, harvest_job)
            return None
        except requests.exceptions.ConnectionError, error:
            msg = '''Could not get content because a
                                connection error occurred. %s''' % error
            self._save_gather_error(msg, harvest_job)
            return None
        except requests.exceptions.Timeout, error:
            msg = 'Could not get content because the connection timed out.'
            self._save_gather_error(msg, harvest_job)
            return None

    def _get_user_name(self):
        if self._user_name:
            return self._user_name

        user = p.toolkit.get_action('get_site_user')(
            {'ignore_auth': True, 'defer_commit': True},
            {})
        self._user_name = user['name']

        return self._user_name

    def _get_object_extra(self, harvest_object, key):
        '''
        Helper function for retrieving the value from a harvest object extra,
        given the key
        '''
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    def _get_package_name(self, harvest_object, title):

        package = harvest_object.package
        if package is None or package.title != title:
            name = self._gen_new_name(title)
            if not name:
                raise Exception('Could not generate a unique name from the title or the GUID. Please choose a more unique title.')
        else:
            name = package.name

        return name

    def get_original_url(self, harvest_object_id):
        obj = model.Session.query(HarvestObject).\
                                    filter(HarvestObject.id==harvest_object_id).\
                                    first()
        if obj:
            return obj.source.url
        return None

    ## Start hooks

    def modify_package_dict(self, package_dict, dcat_dict, harvest_object):
        '''
            Allows custom harvesters to modify the package dict before
            creating or updating the actual package.
        '''
        return package_dict

    ## End hooks

    def gather_stage(self,harvest_job):
        log.debug('In DCATHarvester gather_stage')


        ids = []

        source_to_publisher = {
            'c45948e6-b6cc-4c88-a60f-ff9f98a8029b': 'c2f170ca-63d0-4498-9e81-759827708e97', #: 'ordnance-survey-ireland',
            '0b7e45bf-5035-4d3c-8344-e08e68c15c93': '0ec4257a-c410-4840-a702-08e351f6781c', #: 'galway-city-council',
            '63ebec7b-4529-45f4-bbf8-7f10634899c9': 'galway-county-council',#: 'galway-county-council',
            'ce26b41c-6859-4118-acc0-e98d8edbe36e': 'roscommon-county-council'#: 'roscommon-county-council'
        }

        query = model.Session.execute("select value as guid, package_id from package_extra where key='guid' and package_id in (select package_id from package_extra where value='arcgis') and package_id in (select id as package_id from package where state='active' and owner_org='%s');" % source_to_publisher[harvest_job.source.id])


        # Get the previous guids for this source
        #query = model.Session.query(HarvestObject.guid, HarvestObject.package_id).\
        #                            filter(HarvestObject.current==True).\
        #                            filter(HarvestObject.harvest_source_id==harvest_job.source.id)
        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = guid_to_package_id.keys()
        guids_in_source = []

        log.debug(guids_in_db);

        # Get file contents
        url = harvest_job.source.url

        previous_guids = []
        page = 1
        while True:

            try:
                content = self._get_content(url, harvest_job, page)
            except requests.exceptions.HTTPError, error:
                if error.response.status_code == 404:
                    if page > 1:
                        # Server returned a 404 after the first page, no more
                        # records
                        log.debug('404 after first page, no more pages')
                        break
                    else:
                        # Proper 404
                        msg = 'Could not get content. Server responded with 404 Not Found'
                        self._save_gather_error(msg, harvest_job)
                        return None
                else:
                    # This should never happen. Raising just in case.
                    raise

            if not content:
                return None


            try:

                batch_guids = []
                for guid, as_string in self._get_guids_and_datasets(content):

                    log.debug('Got identifier: {0}'.format(guid.encode('utf8')))
                    batch_guids.append(guid)

                    if guid not in previous_guids:

                        if guid in guids_in_db:
                            # Dataset needs to be udpated
                            obj = HarvestObject(guid=guid, job=harvest_job,
                                            package_id=guid_to_package_id[guid],
                                            content=as_string,
                                            extras=[HarvestObjectExtra(key='status', value='change')])
                        else:
                            # Dataset needs to be created
                            obj = HarvestObject(guid=guid, job=harvest_job,
                                            content=as_string,
                                            extras=[HarvestObjectExtra(key='status', value='new')])
                        obj.save()
                        ids.append(obj.id)

                if len(batch_guids) > 0:
                    guids_in_source.extend(set(batch_guids) - set(previous_guids))
                else:
                    log.debug('Empty document, no more records')
                    # Empty document, no more ids
                    break

            except ValueError, e:
                msg = 'Error parsing file: {0}'.format(str(e))
                self._save_gather_error(msg, harvest_job)
                return None

            if sorted(previous_guids) == sorted(batch_guids):
                # Server does not support pagination or no more pages
                log.debug('Same content, no more pages')
                break


            page = page + 1

            previous_guids = batch_guids

        # Check datasets that need to be deleted
        guids_to_delete = set(guids_in_db) - set(guids_in_source)
        log.debug(len(guids_in_db))
        log.debug(len(guids_in_source))
        log.debug('delete these:')
        log.debug(guids_to_delete)
        for guid in guids_to_delete:
            log.debug('Deleting %s' % guid)
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HarvestObjectExtra(key='status', value='delete')])
            model.Session.query(HarvestObject).\
                  filter_by(guid=guid).\
                  update({'current': False}, False)
            obj.save()
            ids.append(obj.id)

        # why do we reverse these?
        # also, omfg, list.reverse() doesn't return the list. major wat
        ids.reverse()
        return ids

    def fetch_stage(self,harvest_object):
        return True

    def import_stage(self,harvest_object):
        log.debug('In DCATHarvester import_stage')
        if not harvest_object:
            log.error('No harvest object received')
            return False

	status = self._get_object_extra(harvest_object, 'status')

        if status == 'delete':
            # Delete package
            context = {'model': model, 'session': model.Session, 'user': self._get_user_name()}

            p.toolkit.get_action('package_delete')(context, {'id': harvest_object.package_id})
            log.info('Deleted package {0} with guid {1}'.format(harvest_object.package_id, harvest_object.guid))

            return True

        if self.force_import:
            status = 'change'

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False

        # Get the last harvested object (if any)
        previous_object = model.Session.query(HarvestObject) \
                          .filter(HarvestObject.guid==harvest_object.guid) \
                          .filter(HarvestObject.current==True) \
                          .first()

        # Flag previous object as not current anymore
        if previous_object and not self.force_import:
            previous_object.current = False
            previous_object.add()


        package_dict, dcat_dict = self._get_package_dict(harvest_object)
        if not package_dict:
            return False

        if not package_dict.get('name'):
            package_dict['name'] = self._get_package_name(harvest_object, package_dict['title'])

        # Allow custom harvesters to modify the package dict before creating
        # or updating the package
        package_dict = self.modify_package_dict(package_dict,
                                                dcat_dict,
                                                harvest_object)
        # Unless already set by an extension, get the owner organization (if any)

        # from the harvest source dataset
        # if not package_dict.get('owner_org'):
        #     source_dataset = model.Package.get(harvest_object.source.id)
        #     if source_dataset.owner_org:
        #         package_dict['owner_org'] = source_dataset.owner_org

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        context = {
            'user': self._get_user_name(),
            'ignore_auth': True,
        }

        #if the organization doesn't exist, let's create it
        try:
            log.debug('trying to find org %s' % package_dict['owner_org'])
            org_dict = {
                'id': package_dict['owner_org']
            }
            p.toolkit.get_action('organization_show')(context, org_dict)
        except (logic.NotFound):
            org_dict = {
                'name': package_dict['owner_org'],
                'title': package_dict['dcat_publisher_title'],
                'contact-phone': package_dict.get('dcat_publisher_phone', '-'),
                'contact-email': package_dict.get('dcat_publisher_email', '-'),
                'contact-name': package_dict.get('dcat_publisher_contact_name', '-')
            }
            p.toolkit.get_action('organization_create')(context, org_dict)
            log.info('Created organization %s' % package_dict['owner_org'])

        if status == 'new':

            context['schema'] = self.dcat_package_schema()

            # We need to explicitly provide a package ID
            package_dict['id'] = unicode(uuid.uuid4())

            # Save reference to the package on the object
            harvest_object.package_id = package_dict['id']
            harvest_object.add()

            # Defer constraints and flush so the dataset can be indexed with
            # the harvest object id (on the after_show hook from the harvester
            # plugin)
            model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
            model.Session.flush()

            try:
                package_id = p.toolkit.get_action('package_create')(context, package_dict)
                utils.dataset_update(package_dict, apihost='127.0.0.1', apikey='1eb626b1-25a2-445c-a49b-86081ef12c81', change_resources=False)
                log.info('Created dataset with id %s', package_dict['name'])
            except p.toolkit.ValidationError, e:
                #url already in use
                log.error("Got ValidationError for package:")
                log.error(package_dict)
                pass
            
        elif status == 'change':

            package_dict['id'] = harvest_object.package_id
            package_id = p.toolkit.get_action('package_update')(context, package_dict)
            log.info('Updated dataset with id %s', package_id)

        model.Session.commit()

        return True

    def dcat_package_schema (self):
        package_schema = logic.schema.default_create_package_schema()

        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty, unicode]

        package_schema['id'] = [unicode]
        package_schema['collection-name'] = [unicode]
        package_schema['tags'] = tag_schema
        package_schema['language'] = [ignore_missing, unicode, validate_language],
        package_schema['license_id'] = [not_empty, unicode],
        package_schema['theme-primary'] = [not_empty, unicode, validate_theme],
        package_schema['theme-secondary'] = [ignore_missing, unicode],
        package_schema['date_released'] = [not_empty, date_to_db],
        package_schema['date_updated'] = [ignore_missing, date_to_db],
        package_schema['date_update_future'] = [ignore_missing, date_to_db],
        package_schema['last_major_modification'] = [ignore_missing, date_to_db],
        package_schema['bbox-east'] = [ignore_missing],
        package_schema['bbox-west'] = [ignore_missing],
        package_schema['bbox-north'] = [ignore_missing],
        package_schema['bbox-south'] = [ignore_missing],
        package_schema['vertical_extent'] = [ignore_missing, unicode],
        package_schema['extent_geometry'] = [ignore_missing, unicode],
        package_schema['temporal_coverage-from'] = [ignore_missing, date_to_db],
        package_schema['temporal_coverage-to'] = [ignore_missing, date_to_db],
        package_schema['temporal_coverage-other'] = [ignore_missing, unicode],

        return package_schema
