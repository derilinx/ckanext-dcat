import json

import pytest

from ckantoolkit import Invalid
from ckanext.dcat.validators import (
    data_service_serves_dataset,
)
from ckan.tests import factories


def test_data_service_serves_dataset_valid_series():

    user = factories.User()
    data_service = factories.Dataset(type="data_service")

    value = json.dumps([data_service["id"]])
    context = {"user": user["name"]}

    assert data_service_serves_dataset(value, context)


def test_data_service_serves_dataset_valid_multiple_series():

    user = factories.User()
    data_service1 = factories.Dataset(type="data_service")
    data_service2 = factories.Dataset(type="data_service")

    value = json.dumps([data_service1["id"], data_service2["id"]])
    context = {"user": user["name"]}

    assert data_service_serves_dataset(value, context)


def test_data_service_serves_dataset_invalid_dataset_not_found():

    value = json.dumps(["some_id_2"])

    with pytest.raises(Invalid) as e:
        data_service_serves_dataset(value, {})

    assert e.value.error == "Dataset not found"


def test_data_service_serves_dataset_auth():
    user = factories.User()
    org = factories.Organization(users=[{"name": user["name"], "capacity": "admin"}])
    data_service = factories.Dataset(type="data_service", owner_org=org["id"])

    value = json.dumps([data_service["id"]])
    context = {"user": user["name"], "ignore_auth": False}

    assert data_service_serves_dataset(value, context)


def test_data_service_serves_dataset_auth_failed():
    user = factories.User()
    # User does not belong to the organization
    org = factories.Organization()
    data_service = factories.Dataset(type="data_service", owner_org=org["id"])

    value = json.dumps([data_service["id"]])
    context = {"user": user["name"], "ignore_auth": False}

    with pytest.raises(Invalid) as e:
        data_service_serves_dataset(value, context)

    assert (
        e.value.error
        == "User not authorized to add these datasets to this data service"
    )
