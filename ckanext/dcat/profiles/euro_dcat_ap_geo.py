from rdflib import URIRef, BNode, Literal, Namespace

from ckanext.dcat import vocabularies

from .base import (
    RDFProfile,
    DCAT,
    DCT,
    EPSG,
    FOAF,
    LOCN,
    RDF,
    RDFS,
    SKOS,
    XSD,
)
GEODCAT = Namespace('http://data.europa.eu/930/')

from .euro_dcat_ap_breg import breg_geo_common_data_service_fields


class EuropeanDCATAPGeoProfile(RDFProfile):
    def agent(self, dict, ref, pred):
        agent = URIRef(dict['url']) if dict.get('url') else BNode()
        self.g.add((ref, pred, agent))
        self.g.add((agent, RDF.type, FOAF.Agent))
        self._add_triple_from_dict(dict, agent, FOAF.name, 'name', _type=Literal, list_value=True)
        self._add_triple_from_dict(dict, agent, DCT.type, 'type', _type=URIRef, _class=SKOS.Concept)
        
        # TODO: locn:address, org:memberOf
        self._add_triple_from_dict(dict, agent, FOAF.mbox, 'email', _type=URIRef, value_modifier=self._add_mailto)
        self._add_triple_from_dict(dict, agent, FOAF.phone, 'phone', _type=URIRef, value_modifier=lambda num: 'tel:' + num.strip('tel:'))
        self._add_triple_from_dict(dict, agent, FOAF.workplaceHomepage, 'url', _class=FOAF.Document)

    def agents(self, dict, ref):
        for (key, pred) in [
            ('rights_holders', DCT.rightsHolder),
            ('custodians', GEODCAT.custodian),
            ('distributors', GEODCAT.distributor),
            ('originators', GEODCAT.originator),
            ('principal_investigators', GEODCAT.principalInvestigator),
            ('processors', GEODCAT.processor),
            ('resource_providers', GEODCAT.resourceProvider),
            ('users', GEODCAT.user)
        ]:
            for agent in dict.get(key, []):
                self.agent(agent, ref, pred)

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        self._add_triple_from_dict(dataset_dict, dataset_ref, DCT.created, 'metadata_created', date_value=True)
        self._add_triple_from_dict(dataset_dict, dataset_ref, DCT.conformsTo, 'srs', _type=lambda x: EPSG[x.split(':')[1]], list_value=True)
        self._add_triple_from_dict(dataset_dict, dataset_ref, RDFS.comment, 'spatial_resolution')

        self.agents(dataset_dict, dataset_ref)
        # TODO:
        # dqv:hasQualityMeasurement dqv:QualityMeasurement 0..n (spatial resolution - structured)

        # prov:wasUsedBy prov:Activity 0..n

    def graph_from_group(self, group_dict, group_ref):
        if group_dict['type'] != 'data-service':
            return

        self._add_triple_from_dict(group_dict, group_ref, DCT.created, 'metadata_created', date_value=True)
        self._add_triple_from_dict(group_dict, group_ref, DCT.modified, 'metadata_modified', date_value=True)
        self._add_triple_from_dict(group_dict, group_ref, DCT.issued, 'issued', date_value=True, fallbacks=['metadata_created'])

        for language in group_dict.get("language", []):
            uri = vocabularies.languages.lookup(ckan=language)
            self.g.add((group_ref, DCT.language, uri))
            self.g.add((uri, RDF.type, DCT.LinguisticSystem))

        self._add_triple_from_dict(group_dict, group_ref, DCAT.temporalResolution, 'temporal_resolution', _type=Literal, _datatype=XSD.duration, list_value=True)

        start = self._get_dict_value(group_dict, "temporal_start")
        end = self._get_dict_value(group_dict, "temporal_end")
        if start or end:
            temporal_extent = BNode()

            self.g.add((temporal_extent, RDF.type, DCT.PeriodOfTime))
            if start:
                self._add_date_triple(temporal_extent, DCAT.startDate, start)
            if end:
                self._add_date_triple(temporal_extent, DCAT.endDate, end)
            self.g.add((group_ref, DCT.temporal, temporal_extent))

        self._add_triple_from_dict(group_dict, group_ref, DCT.conformsTo, 'srs', _type=lambda x: EPSG[x.split(':')[1]], list_value=True)

        self._add_triple_from_dict(group_dict, group_ref, RDFS.comment, 'spatial_resolution')
        self._add_triple_from_dict(group_dict, group_ref, DCAT.spatialResolutionInMeters, 'spatial_resolution_in_meters', _type=Literal, _datatype=XSD.decimal, value_modifier=float)

        spatial_geom = self._get_dict_value(group_dict, "spatial")
        if spatial_geom:
            spatial_ref = self._get_or_create_spatial_ref(group_dict, group_ref)
            self._add_spatial_value_to_graph(
                spatial_ref, LOCN.geometry, spatial_geom
            )
        # Add fields for spatial text (skos:prefLabel), bbox/centroid (dcat:{}), identifiers, gazetteer (skos:inScheme)?
        # Not present in schema at dataset level

        breg_geo_common_data_service_fields(self, group_dict, group_ref)
        self.agents(group_dict, group_ref)
        

        # TODO:
        # dct:type skos:Concept 0..1
        # There are three seperate dct:type entries in the geo spec.
        # Each has cardinality 0..1, and SHOULD take a value from 3 separate registries (spatial data service classification, spatial data service type, resource type)
        # Base DCAT-AP also has a cardinality 0..1 dct:type, with no particular requirements

        # prov:qualifiedAttribution prov:Attribution 0..n
        # prov:wasUsedBy prov:Activity 0..n

        # dqv:hasQualityMeasurement dqv:QualityMeasurement 0..n

