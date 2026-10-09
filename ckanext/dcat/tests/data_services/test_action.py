try:
    from unittest import mock
except ImportError:
    import mock
import pytest


from ckan.plugins import toolkit

from ckantoolkit import config
from ckantoolkit.tests import helpers, factories


@pytest.mark.usefixtures("with_plugins", "clean_db")
@pytest.mark.ckan_config("ckan.plugins", "dcat scheming_datasets dcat_data_services")
@pytest.mark.ckan_config(
    "scheming.dataset_schemas", "ckanext.dcat.schemas:dcat_ap_data_service.yaml"
)
@pytest.mark.ckan_config(
    "scheming.presets",
    "ckanext.scheming:presets.json " "ckanext.dcat.schemas:presets.yaml ",
)
class TestDataServices:

    def test_data_service_fields(self):

        dataset = factories.Dataset()

        data_service = factories.Dataset(
            type="data_service", serves_dataset=[dataset["id"]]
        )

        dataset_dict = helpers.call_action("package_show", id=dataset["id"])

        data_service_dict = helpers.call_action("package_show", id=data_service["id"])

        assert dataset_dict["data_services"] == [
            {
                "id": data_service["id"],
                "name": data_service["name"],
                "title": data_service["title"],
                "type": data_service["type"],
            }
        ]
        assert data_service_dict["served_datasets"] == [
            {
                "id": dataset["id"],
                "name": dataset["name"],
                "title": dataset["title"],
                "type": dataset["type"],
            }
        ]

    def test_data_service_multiple_datasets(self):

        dataset1 = factories.Dataset()
        dataset2 = factories.Dataset()

        data_service = factories.Dataset(
            type="data_service", serves_dataset=[dataset1["id"], dataset2["id"]]
        )

        dataset_dict = helpers.call_action("package_show", id=dataset1["id"])

        data_service_dict = helpers.call_action("package_show", id=data_service["id"])

        assert dataset_dict["data_services"] == [
            {
                "id": data_service["id"],
                "name": data_service["name"],
                "title": data_service["title"],
                "type": data_service["type"],
            }
        ]
        assert sorted(
            data_service_dict["served_datasets"], key=lambda d: d["id"]
        ) == sorted(
            [
                {
                    "id": dataset1["id"],
                    "name": dataset1["name"],
                    "title": dataset1["title"],
                    "type": dataset1["type"],
                },
                {
                    "id": dataset2["id"],
                    "name": dataset2["name"],
                    "title": dataset2["title"],
                    "type": dataset2["type"],
                },
            ],
            key=lambda d: d["id"],
        )

    def test_data_service_multiple_datasets_auth_private_dataset(self):
        user = factories.User()
        sysadmin = factories.Sysadmin()
        # User does not belong to the organization
        org = factories.Organization()
        dataset1 = factories.Dataset(owner_org=org["id"], private=True)
        dataset2 = factories.Dataset()

        data_service = factories.Dataset(
            type="data_service", serves_dataset=[dataset1["id"], dataset2["id"]]
        )

        # User can only see one served dataset
        context = {"user": user["name"], "ignore_auth": False}
        data_service_dict = helpers.call_action(
            "package_show", context=context, id=data_service["id"]
        )

        assert data_service_dict["served_datasets"] == [
            {
                "id": dataset2["id"],
                "name": dataset2["name"],
                "title": dataset2["title"],
                "type": dataset2["type"],
            },
        ]

        # Sysadmin can see both datasets
        context = {"user": sysadmin["name"], "ignore_auth": False}
        data_service_dict = helpers.call_action(
            "package_show", context=context, id=data_service["id"]
        )

        assert sorted(
            data_service_dict["served_datasets"], key=lambda d: d["id"]
        ) == sorted(
            [
                {
                    "id": dataset1["id"],
                    "name": dataset1["name"],
                    "title": dataset1["title"],
                    "type": dataset1["type"],
                },
                {
                    "id": dataset2["id"],
                    "name": dataset2["name"],
                    "title": dataset2["title"],
                    "type": dataset2["type"],
                },
            ],
            key=lambda d: d["id"],
        )

    def test_data_service_auth_private_data_service(self):
        user = factories.User()
        sysadmin = factories.Sysadmin()
        # User does not belong to the organization
        org = factories.Organization()
        dataset = factories.Dataset()

        data_service = factories.Dataset(
            type="data_service",
            serves_dataset=[dataset["id"]],
            owner_org=org["id"],
            private=True,
        )

        # User can not see data service in dataset
        context = {"user": user["name"], "ignore_auth": False}
        dataset_dict = helpers.call_action(
            "package_show", context=context, id=dataset["id"]
        )

        assert dataset_dict["data_services"] == []

        # Sysadmin can see data service
        context = {"user": sysadmin["name"], "ignore_auth": False}
        dataset_dict = helpers.call_action(
            "package_show", context=context, id=dataset["id"]
        )
        assert dataset_dict["data_services"] == [
            {
                "id": data_service["id"],
                "name": data_service["name"],
                "title": data_service["title"],
                "type": data_service["type"],
            }
        ]

    def test_data_service_multiple_data_services(self):

        dataset = factories.Dataset()

        data_service1 = factories.Dataset(
            type="data_service", serves_dataset=[dataset["id"]]
        )
        data_service2 = factories.Dataset(
            type="data_service", serves_dataset=[dataset["id"]]
        )

        dataset_dict = helpers.call_action("package_show", id=dataset["id"])

        assert sorted(dataset_dict["data_services"], key=lambda d: d["id"]) == sorted(
            [
                {
                    "id": data_service1["id"],
                    "name": data_service1["name"],
                    "title": data_service1["title"],
                    "type": data_service1["type"],
                },
                {
                    "id": data_service2["id"],
                    "name": data_service2["name"],
                    "title": data_service2["title"],
                    "type": data_service2["type"],
                },
            ],
            key=lambda d: d["id"],
        )
