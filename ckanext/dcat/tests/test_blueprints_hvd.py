import pytest

from ckantoolkit import url_for
from ckantoolkit.tests import factories

from ckanext.dcat.processors import RDFParser


@pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index")
@pytest.mark.ckan_config("ckan.plugins", "dcat scheming_datasets")
@pytest.mark.ckan_config(
    "scheming.dataset_schemas", "ckanext.dcat.schemas:dcat_ap_full.yaml"
)
@pytest.mark.ckan_config(
    "scheming.presets",
    "ckanext.scheming:presets.json ckanext.dcat.schemas:presets.yaml",
)
class TestEndpointsHVD:

    def test_endpoint_hvd_ttl(self, app):

        datasets = [
            {
                "name": "test-dataset-hvd",
                "title": "Test dataset HVD",
                "applicable_legislation": "http://data.europa.eu/eli/reg_impl/2023/138/oj",
                "resources": [
                    {
                        "id": "03ec9010-793c-4a91-80f4-5c3dd2f3ea2a",
                        "url": "http://example.org/data.csv",
                        "name": "hvd resource",
                        "applicable_legislation": "http://data.europa.eu/eli/reg_impl/2023/138/oj",
                    },
                    {
                        "id": "ec5d251e-c4a1-4468-9552-a5c5e75f74c2",
                        "url": "http://example.org/data2.csv",
                        "name": "non hvd resource",
                    },
                ],
            },
            {
                "name": "test-dataset-non-hvd",
                "title": "Test dataset No HVD",
                "resources": [
                    {
                        "id": "7ce8f1c3-d356-4184-ab21-fe83de084e48",
                        "url": "http://example.org/data3.csv",
                        "name": "non hvd resource",
                    },
                    {
                        "id": "400e9ea8-2229-4821-8796-d43a8d565757",
                        "url": "http://example.org/data4.csv",
                        "name": "non hvd resource",
                    },
                ],
            },
        ]

        for dataset in datasets:
            factories.Dataset(**dataset)

        url = url_for(
            "dcat.read_catalog", _format="ttl", profiles="euro_dcat_ap_hvd_220"
        )

        response = app.get(url)

        assert response.headers["Content-Type"] == "text/turtle; charset=utf-8"

        content = response.body

        # Parse the contents to check it's an actual serialization
        p = RDFParser()

        p.parse(content, _format="turtle")

        dcat_datasets = [d for d in p.datasets()]

        assert len(dcat_datasets) == 1
        assert dcat_datasets[0]["title"] == "Test dataset HVD"

        assert len(dcat_datasets[0]["resources"]) == 1

        assert dcat_datasets[0]["resources"][0]["name"] == "hvd resource"
