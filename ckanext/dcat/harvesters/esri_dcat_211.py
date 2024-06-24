from ckanext.dcat.interfaces import IDCATRDFHarvester

from ckan import plugins
from ckan.plugins import toolkit
import re
import json

class EsriInspireDcat211Harvester (plugins.SingletonPlugin):
    plugins.implements(IDCATRDFHarvester, inherit=True)

    # using this as a specific pattern of these three namespaces seen in the wild,
    # rather than pulling any of the europa namespaces. 
    pat = re.compile('"@id": ?"(ftype|lang|access)/([A-Z]+)"')
    
    def after_download(self, content, harvest_job):
        ''' 
        ESRI's ArcGis Inspire DCAT has context that's not parsed directly with rdf lib. 

	"@context": {
                ...
		"ftype": "http://publications.europa.eu/resource/authority/file-type/",
		"lang": "http://publications.europa.eu/resource/authority/language/",
		"access": "http://publications.europa.eu/resource/authority/access-right/",
	},

        ...
        	"dct:language": {
		"@id": "lang/ENG"
	},

        lang/ENG here comes out in rdflib as:
         
        rdflib.term.URIRef('file:///usr/lib/ckan/default/src/ckan/lang/ENG')

        instead of:

        rdflib.term.URIRef('http://publications.europa.eu/resource/authority/language/ENG')
        
        This uriref can be resolved using the codelists/controlled vocabulary
        '''

        esri_hack = False
        if harvest_job.source.config:
            esri_hack = json.loads(harvest_job.source.config).get("esri_hack", False)
        if not esri_hack:
            return content, []

        return self.pat.sub(r'"@id": "\1:\2"', content), []
        

        
