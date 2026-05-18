import pytest

from ckanext.dcat.profiles import DCAT, RDF, DCT
from ckanext.dcat.processors import RDFSerializer
from ckanext.dcat.tests.utils import BaseSerializeTest


class TestEuroDCATAPHVDProfileSerialize(BaseSerializeTest):

    def test_serialize_hvd_datasets_in_catalog(self):

        datasets = [
            {
                "name": "test-dataset-hvd",
                "title": "Test dataset HVD",
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

        s = RDFSerializer(profiles=["euro_dcat_ap_hvd_220"])
        g = s.g
        catalog = s.serialize_catalog(
            catalog_dict={}, dataset_dicts=datasets, _format="ttl"
        )

        g.parse(data=catalog, format="ttl")

        dataset_refs = [d for d in g.subjects(RDF.type, DCAT.Dataset)]

        assert len(dataset_refs) == 1

        dataset_ref = dataset_refs[0]

        assert self._triple(g, dataset_ref, DCT.title, "Test dataset HVD")

        distribution_refs = [d for d in g.objects(dataset_ref, DCAT.distribution)]

        assert len(distribution_refs) == 1

        distribution_ref = distribution_refs[0]

        assert self._triple(g, distribution_ref, DCT.title, "hvd resource")
