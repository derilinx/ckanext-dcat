import json

import pytest
from rdflib.term import URIRef

from ckan.tests import factories
from ckan.tests.helpers import call_action
from ckanext.dcat.tests.utils import BaseSerializeTest
from ckanext.dcat.processors import RDFSerializer
from ckanext.dcat import utils
from ckanext.dcat.profiles import RDF, DCAT, DCT


@pytest.mark.usefixtures("with_plugins", "clean_db")
@pytest.mark.ckan_config("ckan.plugins", "dcat scheming_datasets dcat_data_services")
@pytest.mark.ckan_config(
    "scheming.dataset_schemas", "ckanext.dcat.schemas:dcat_ap_data_service.yaml"
)
@pytest.mark.ckan_config(
    "scheming.presets",
    "ckanext.scheming:presets.json " "ckanext.dcat.schemas:presets.yaml ",
)
@pytest.mark.ckan_config("ckanext.dcat.rdf.profiles", "euro_dcat_ap_3")
class TestEuroDCATAP3ProfileSerializeDataService(BaseSerializeTest):
    def test_e2e_ckan_to_dcat(self):
        """
        Create a data service using the scheming schema, check that fields
        are exposed in the DCAT RDF graph
        """

        data_service_dict = json.loads(
            self._get_file_contents("ckan/ckan_dcat_ap_data_service.json")
        )

        # Replaced linked datasets with actual existing ones
        dataset1 = factories.Dataset()
        dataset2 = factories.Dataset()
        data_service_dict["serves_dataset"] = [dataset1["id"], dataset2["id"]]

        data_service = call_action("package_create", **data_service_dict)

        # Make sure schema was used
        assert data_service["type"] == "data_service"
        assert data_service["contact"][0]["name"] == "Contact 1"

        s = RDFSerializer()
        g = s.g

        data_service_ref = s.graph_from_dataset(data_service)

        assert str(data_service_ref) == utils.dataset_uri(data_service)

        assert self._triple(g, data_service_ref, RDF.type, DCAT.DataService)
        assert self._triple(g, data_service_ref, DCT.title, data_service["title"])
        assert self._triple(g, data_service_ref, DCT.description, data_service["notes"])


        assert sorted(self._triples_list_values(g, data_service_ref, DCAT.servesDataset)) == sorted([
            utils.dataset_uri({"id": dataset1["id"]}),
            utils.dataset_uri({"id": dataset2["id"]}),
        ])

        resource = data_service["resources"][0]
        assert self._triple(
            g, data_service_ref, DCAT.endpointURL, URIRef(resource["url"])
        )
        assert self._triple(
            g,
            data_service_ref,
            DCAT.endpointDescription,
            URIRef(resource["endpoint_description"]),
        )
        assert self._triple(
            g,
            data_service_ref,
            DCT["format"],
            URIRef("http://publications.europa.eu/resource/authority/file-type/JSON"),
        )
