import json

from rdflib import URIRef, BNode, Literal

from .. import codelists, legal_resources
from ..utils import resource_uri, group_uri

from .base import URIRefOrLiteral, CleanedURIRef
from .base import (
    RDF,
    SKOS,
    DCAT,
    DCATAP,
    DCT,
    XSD,
    ELI
)

from .euro_dcat_ap import EuropeanDCATAPProfile


class EuropeanDCATAP2Profile(EuropeanDCATAPProfile):
    """
    An RDF profile based on the DCAT-AP 2 for data portals in Europe

    More information and specification:

    https://joinup.ec.europa.eu/asset/dcat_application_profile

    """

    def parse_dataset(self, dataset_dict, dataset_ref):

        # call super method
        super(EuropeanDCATAP2Profile, self).parse_dataset(dataset_dict, dataset_ref)

        # Lists
        for key, predicate in (
            ("temporal_resolution", DCAT.temporalResolution),
            ("is_referenced_by", DCT.isReferencedBy),
            ("applicable_legislation", DCATAP.applicableLegislation),
            ("hvd_category", DCATAP.hvdCategory),
        ):
            values = self._object_value_list(dataset_ref, predicate)
            if values:
                dataset_dict["extras"].append({"key": key, "value": json.dumps(values)})
        # Temporal
        start, end = self._time_interval(dataset_ref, DCT.temporal, dcat_ap_version=2)
        if start:
            self._insert_or_update_temporal(dataset_dict, "temporal_start", start)
        if end:
            self._insert_or_update_temporal(dataset_dict, "temporal_end", end)

        # Spatial
        spatial = self._spatial(dataset_ref, DCT.spatial)
        for key in ("bbox", "centroid"):
            self._add_spatial_to_dict(dataset_dict, key, spatial)

        # Spatial resolution in meters
        spatial_resolution_in_meters = self._object_value_float_list(
            dataset_ref, DCAT.spatialResolutionInMeters
        )
        if spatial_resolution_in_meters:
            dataset_dict["extras"].append(
                {
                    "key": "spatial_resolution_in_meters",
                    "value": json.dumps(spatial_resolution_in_meters),
                }
            )

        # Resources
        for distribution in self._distributions(dataset_ref):
            distribution_ref = str(distribution)
            for resource_dict in dataset_dict.get("resources", []):
                # Match distribution in graph and distribution in resource dict
                if resource_dict and distribution_ref == resource_dict.get(
                    "distribution_ref"
                ):
                    #  Simple values
                    for key, predicate in (
                        ("availability", DCATAP.availability),
                        ("compress_format", DCAT.compressFormat),
                        ("package_format", DCAT.packageFormat),
                    ):
                        value = self._object_value(distribution, predicate)
                        if value:
                            resource_dict[key] = value

                    #  Lists
                    for key, predicate in (
                        ("applicable_legislation", DCATAP.applicableLegislation),
                    ):
                        values = self._object_value_list(distribution, predicate)
                        if values:
                            resource_dict[key] = json.dumps(values)

            # Note: data services are not parsed

        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):

        # call super method
        super(EuropeanDCATAP2Profile, self).graph_from_dataset(
            dataset_dict, dataset_ref
        )

        # Lists
        for key, predicate, fallbacks, _type, datatype, _class in (
            ('temporal_resolution', DCAT.temporalResolution, None, Literal, XSD.duration, None),
            ('is_referenced_by', DCT.isReferencedBy, None, URIRefOrLiteral, None, None),
            ('applicable_legislation', DCATAP.applicableLegislation, None, URIRefOrLiteral, None, ELI.LegalResource),
        ):
            self._add_triple_from_dict(dataset_dict, dataset_ref, predicate, key, list_value=True,
                                       fallbacks=fallbacks, _type=_type, _datatype=datatype, _class=_class)

        for eli in dataset_dict.get('applicable_legislation'):
            self.g += legal_resources.info(eli)

        self._add_from_codelist(dataset_dict, dataset_ref, DCATAP.hvdCategory, 'hvd_category',
                                codelists.high_value_dataset_category,
                                list_value=True)

        # Temporal
        start = self._get_dataset_value(dataset_dict, "temporal_start")
        end = self._get_dataset_value(dataset_dict, "temporal_end")
        if start or end:
            temporal_extent_dcat = BNode()

            self.g.add((temporal_extent_dcat, RDF.type, DCT.PeriodOfTime))
            if start:
                self._add_date_triple(temporal_extent_dcat, DCAT.startDate, start)
            if end:
                self._add_date_triple(temporal_extent_dcat, DCAT.endDate, end)
            self.g.add((dataset_ref, DCT.temporal, temporal_extent_dcat))

        # spatial
        spatial_bbox = self._get_dataset_value(dataset_dict, "spatial_bbox")
        spatial_cent = self._get_dataset_value(dataset_dict, "spatial_centroid")

        if spatial_bbox or spatial_cent:
            spatial_ref = self._get_or_create_spatial_ref(dataset_dict, dataset_ref)

            if spatial_bbox:
                self._add_spatial_value_to_graph(spatial_ref, DCAT.bbox, spatial_bbox)

            if spatial_cent:
                self._add_spatial_value_to_graph(
                    spatial_ref, DCAT.centroid, spatial_cent
                )

        # Spatial resolution in meters
        spatial_resolution_in_meters = self._read_list_value(
            self._get_dataset_value(dataset_dict, "spatial_resolution_in_meters")
        )
        if spatial_resolution_in_meters:
            for value in spatial_resolution_in_meters:
                try:
                    self.g.add(
                        (
                            dataset_ref,
                            DCAT.spatialResolutionInMeters,
                            Literal(float(value), datatype=XSD.decimal),
                        )
                    )
                except (ValueError, TypeError):
                    self.g.add(
                        (dataset_ref, DCAT.spatialResolutionInMeters, Literal(value))
                    )


    def graph_from_resource(self, g, dataset_ref, resource_dict, resource_license_fallback, distribution=None):
        distribution = super().graph_from_resource(g, dataset_ref, resource_dict, resource_license_fallback, distribution)

        #  Simple values
        items = [
            ('availability', DCATAP.availability, None, URIRefOrLiteral),
            ('compress_format', DCAT.compressFormat, None, URIRefOrLiteral),
            ('package_format', DCAT.packageFormat, None, URIRefOrLiteral)
        ]

        self._add_triples_from_dict(resource_dict, distribution, items)

        #  Lists
        items = [
            ('applicable_legislation', DCATAP.applicableLegislation, None, URIRefOrLiteral),
        ]
        self._add_list_triples_from_dict(resource_dict, distribution, items)

        for eli in resource_dict.get('applicable_legislation'):
            self.g += legal_resources.info(eli)

        for data_service in resource_dict.get('data_services', []):
            service_uri = URIRef(group_uri({ 'id': data_service, 'type': 'data-service' }))

            self.g.add((distribution, DCAT.accessService, service_uri))
            self.g.add((service_uri, RDF.type, DCAT.DataService))

        return distribution

    def groups(self):
        return { str(uri).split('/')[-1] for uri in self.g.subjects(RDF.type, DCAT.DataService) }

    def graph_from_group(self, group_dict, group_ref):
        if group_dict['type'] != 'data-service':
            return

        catalog = self.g.value(predicate=RDF.type, object=DCAT.Catalog)
        if catalog:
            self.g.add((catalog, DCAT.service, group_ref))

        self._add_triples_from_dict(group_dict, group_ref, [
            ('availability', DCATAP.availability, None, URIRefOrLiteral),
            ('license', DCT.license, None, URIRefOrLiteral),
            ('access_rights', DCT.accessRights, None, URIRefOrLiteral),
            ('title', DCT.title, None, Literal),
            ('endpoint_description', DCAT.endpointDescription, None, Literal),
            ('description', DCT.description, None, Literal),
        ])

        #  Lists
        self._add_list_triples_from_dict(group_dict, group_ref, [
            ('endpoint_url', DCAT.endpointURL, None, URIRefOrLiteral),
            ('serves_dataset', DCAT.servesDataset, None, URIRefOrLiteral),
        ])
        return

    def graph_from_catalog(self, catalog_dict, catalog_ref):

        # call super method
        super(EuropeanDCATAP2Profile, self).graph_from_catalog(
            catalog_dict, catalog_ref
        )
