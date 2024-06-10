from .base import ADMS, URIRefOrLiteral
from .euro_dcat_ap_base import BaseEuropeanDCATAPProfile
import json

from rdflib import term, URIRef, BNode, Literal
import ckantoolkit as toolkit

from ckan.lib.munge import munge_tag

from ckanext.dcat import vocabularies
from ckanext.dcat.utils import (
    resource_uri,
    DCAT_EXPOSE_SUBCATALOGS,
    DCAT_CLEAN_TAGS,
    publisher_uri_organization_fallback,
)
from .base import RDFProfile, URIRefOrLiteral, CleanedURIRef
from .base import (
    RDF,
    XSD,
    SKOS,
    RDFS,
    DCAT,
    DCT,
    ADMS,
    XSD,
    VCARD,
    FOAF,
    SCHEMA,
    SKOS,
    LOCN,
    GSP,
    OWL,
    SPDX,
    GEOJSON_IMT,
    namespaces,
)

config = toolkit.config


class EuropeanDCATAPProfile(BaseEuropeanDCATAPProfile):
    """
    An RDF profile based on the DCAT-AP v1 for data portals in Europe

    More information and specification:

    https://joinup.ec.europa.eu/asset/dcat_application_profile

    """

    def parse_dataset(self, dataset_dict, dataset_ref):

        # Call base method for common properties
        dataset_dict = self._parse_dataset_base(dataset_dict, dataset_ref)

        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):

        # Call base method for common properties
        self._graph_from_dataset_base(dataset_dict, dataset_ref)

        # DCAT AP v1 specific properties
        self._graph_from_dataset_v1_only(dataset_dict, dataset_ref)

    def graph_from_catalog(self, catalog_dict, catalog_ref):

        self._graph_from_catalog_base(catalog_dict, catalog_ref)

    def _graph_from_dataset_v1_only(self, dataset_dict, dataset_ref):
        """
        CKAN -> DCAT v1 specific properties (not applied to higher versions)
        """

        # Other identifiers (these are handled differently in the
        # DCAT-AP v3 profile)
        self._add_triple_from_dict(
            dataset_dict,
            dataset_ref,
            ADMS.identifier,
            "alternate_identifier",
            list_value=True,
            _type=URIRefOrLiteral,
            _class=ADMS.Identifier,
        )
