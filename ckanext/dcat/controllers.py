import json

from ckan.plugins import toolkit

if toolkit.check_ckan_version(min_version='2.1'):
    BaseController = toolkit.BaseController
else:
    from ckan.lib.base import BaseController

from ckanext.dcat.utils import CONTENT_TYPES

import logging
log = logging.getLogger(__name__)

class DCATController(BaseController):

    def read_catalog(self, _format='rdf'):

        data_dict = {
            'page': toolkit.request.params.get('page'),
            'modified_since': toolkit.request.params.get('modified_since'),
            'format': _format,
        }

        toolkit.response.headers.update(
            {'Content-type': CONTENT_TYPES[_format]})
        try:
            return toolkit.get_action('dcat_catalog_show')({}, data_dict)
        except toolkit.ValidationError, e:
            toolkit.abort(409, str(e))

    def read_dataset(self, _id, _format='rdf'):
        if not _format:
            _format = check_access_header()

        if not _format:
            return PackageController().read(_id)

        toolkit.response.headers.update(
            {'Content-type': CONTENT_TYPES[_format]})

        try:
            result = toolkit.get_action('dcat_dataset_show')({}, {'id': _id,
                'format': _format})
        except Exception as exc:
            log.error(exc)
            toolkit.abort(404)

        return result

#        toolkit.response.headers.update(
#            {'Content-type': CONTENT_TYPES[_format]})
#        return toolkit.get_action('dcat_dataset_show')({}, {'id': _id,
#                                                            'format': _format})

    def dcat_json(self):

        data_dict = {
            'page': toolkit.request.params.get('page'),
            'modified_since': toolkit.request.params.get('modified_since'),
        }

        try:
            datasets = toolkit.get_action('dcat_datasets_list')({},
                                                                data_dict)
        except toolkit.ValidationError, e:
            toolkit.abort(409, str(e))

        content = json.dumps(datasets)

        toolkit.response.headers['Content-Type'] = 'application/json'
        toolkit.response.headers['Content-Length'] = len(content)

        return content



