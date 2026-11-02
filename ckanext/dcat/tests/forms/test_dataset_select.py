import json

import pytest

from ckan import model
from ckan.plugins import toolkit

from ckan.tests import factories

from ckanext.dcat.helpers import dataset_select_options, get_datasets_by_ids

pytestmark = [
    pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index"),
    pytest.mark.ckan_config(
        "ckan.plugins", "dcat scheming_datasets dcat_data_services"
    ),
    pytest.mark.ckan_config(
        "scheming.dataset_schemas",
        "ckanext.dcat.schemas:dcat_ap_data_service.yaml "
        "ckanext.scheming:ckan_dataset.yaml",
    ),
    pytest.mark.ckan_config(
        "scheming.presets",
        "ckanext.scheming:presets.json ckanext.dcat.schemas:presets.yaml",
    ),
]


@pytest.fixture
def search_fixtures():
    user = factories.UserWithToken()
    org = factories.Organization(users=[{"name": user["name"], "capacity": "editor"}])
    other_org = factories.Organization(
        users=[{"name": user["name"], "capacity": "member"}]
    )
    editable = factories.Dataset(title="Test editable", owner_org=org["id"])
    editable_private = factories.Dataset(
        title="Test editable private", owner_org=org["id"], private=True
    )
    readable = factories.Dataset(title="Test readable", owner_org=other_org["id"])
    other_private = factories.Dataset(
        title="Test other private",
        owner_org=factories.Organization()["id"],
        private=True,
    )
    return {
        "user": user,
        "editable": editable,
        "editable_private": editable_private,
        "readable": readable,
        "other_private": other_private,
    }


def _search(app, user, **params):
    params.setdefault("q", "test")
    resp = app.get(
        toolkit.url_for("dcat_forms.dataset_select_search"),
        query_string=params,
        headers={"Authorization": user["token"]},
    )
    return resp.json


def _result_ids(result):
    return [d["id"] for d in result["results"]]


class TestDatasetSelectSearch:

    def test_anonymous_not_allowed(self, app):
        app.get(
            toolkit.url_for("dcat_forms.dataset_select_search"),
            query_string={"q": "test"},
            status=403,
        )

    def test_empty_query(self, app):
        user = factories.UserWithToken()
        assert _search(app, user, q="") == {"results": [], "more": False}

    def test_result_format(self, app):
        sysadmin = factories.SysadminWithToken()
        org = factories.Organization(title="Test org")
        dataset = factories.Dataset(title="Test dataset", owner_org=org["id"])

        result = _search(app, sysadmin)

        assert result == {
            "results": [
                {
                    "id": dataset["id"],
                    "text": "Test dataset",
                    "url": toolkit.url_for("dataset.read", id=dataset["name"]),
                    "organization": "Test org",
                }
            ],
            "more": False,
        }

    def test_only_editable_datasets_returned(self, app, search_fixtures):

        ids = _result_ids(_search(app, search_fixtures["user"]))

        assert search_fixtures["editable"]["id"] in ids
        assert search_fixtures["editable_private"]["id"] in ids
        assert search_fixtures["readable"]["id"] not in ids
        assert search_fixtures["other_private"]["id"] not in ids

    @pytest.mark.parametrize(
        "q,found",
        [
            ("Test g", True),
            ("test geov", True),
            ("geoviews", True),
            ("Test geoviews", True),
            ("Test qqzz", False),
            ("Test g:", False),
            ("Test AND", False),
        ],
    )
    def test_partial_words(self, app, q, found):
        sysadmin = factories.SysadminWithToken()
        dataset = factories.Dataset(title="Test geoviews", notes="")

        ids = _result_ids(_search(app, sysadmin, q=q))

        assert (dataset["id"] in ids) is found

    def test_user_without_orgs(self, app, search_fixtures):
        user = factories.UserWithToken()

        assert _search(app, user) == {"results": [], "more": False}

    def test_sysadmin_sees_all(self, app, search_fixtures):
        sysadmin = factories.SysadminWithToken()

        ids = _result_ids(_search(app, sysadmin))

        assert search_fixtures["editable"]["id"] in ids
        assert search_fixtures["editable_private"]["id"] in ids
        assert search_fixtures["readable"]["id"] in ids
        assert search_fixtures["other_private"]["id"] in ids

    def test_selected_datasets_excluded(self, app, search_fixtures):

        ids = _result_ids(
            _search(
                app, search_fixtures["user"], selected=search_fixtures["editable"]["id"]
            )
        )

        assert search_fixtures["editable"]["id"] not in ids
        assert search_fixtures["editable_private"]["id"] in ids

    def test_dataset_types(self, app):
        sysadmin = factories.SysadminWithToken()
        dataset = factories.Dataset(title="Test dataset")
        data_service = factories.Dataset(title="Test service", type="data_service")

        ids = _result_ids(_search(app, sysadmin))
        assert dataset["id"] in ids
        assert data_service["id"] not in ids

        ids = _result_ids(_search(app, sysadmin, dataset_type="dataset,data_service"))
        assert dataset["id"] in ids
        assert data_service["id"] in ids

    def test_pagination(self, app):
        sysadmin = factories.SysadminWithToken()
        datasets = [factories.Dataset(title="Test dataset") for _ in range(12)]

        page1 = _search(app, sysadmin, page=1)
        page2 = _search(app, sysadmin, page=2)

        assert len(page1["results"]) == 10
        assert page1["more"] is True
        assert len(page2["results"]) == 2
        assert page2["more"] is False
        assert sorted(_result_ids(page1) + _result_ids(page2)) == sorted(
            d["id"] for d in datasets
        )


class TestGetDatasetsByIds:

    def test_empty(self, app):
        with app.flask_app.test_request_context():
            assert get_datasets_by_ids(None) == []
            assert get_datasets_by_ids([]) == []
            assert get_datasets_by_ids("") == []

    def test_order_and_formats(self, app):
        dataset1 = factories.Dataset()
        dataset2 = factories.Dataset()
        # Not requested, should not be returned
        factories.Dataset()
        factories.Dataset()

        with app.flask_app.test_request_context():
            ids = [dataset2["id"], dataset1["id"]]

            for value in (ids, json.dumps(ids)):
                result = get_datasets_by_ids(value)
                assert [d["id"] for d in result] == [dataset2["id"], dataset1["id"]]

            result = get_datasets_by_ids(dataset1["id"])
            assert [d["id"] for d in result] == [dataset1["id"]]

    def test_unavailable_datasets_kept(self, app):
        dataset = factories.Dataset()
        private_dataset = factories.Dataset(
            owner_org=factories.Organization()["id"], private=True
        )

        with app.flask_app.test_request_context():
            result = get_datasets_by_ids(
                [dataset["id"], private_dataset["id"], "not-found"]
            )

        assert result[0]["id"] == dataset["id"]
        assert result[1] == {"id": private_dataset["id"]}
        assert result[2] == {"id": "not-found"}


class TestDatasetSelectOptions:

    def test_options(self, app):
        dataset = factories.Dataset(title="Test dataset")

        with app.flask_app.test_request_context():
            result = dataset_select_options([dataset["id"], "not-found"])

        assert result == [
            {
                "id": dataset["id"],
                "text": "Test dataset",
                "url": toolkit.url_for("dataset.read", id=dataset["name"]),
                "organization": None,
            },
            {
                "id": "not-found",
                "text": "Restricted dataset",
            },
        ]


class TestDatasetSelectForm:

    def test_edit_form_shows_selected_datasets(self, app):
        user = factories.UserWithToken()
        org = factories.Organization(
            users=[{"name": user["name"], "capacity": "editor"}]
        )
        dataset = factories.Dataset(title="Served dataset", owner_org=org["id"])
        data_service = factories.Dataset(
            type="data_service", owner_org=org["id"], serves_dataset=[dataset["id"]]
        )

        resp = app.get(
            toolkit.url_for("data_service.edit", id=data_service["name"]),
            headers={"Authorization": user["token"]},
        )

        assert (
            'data-module-source="{}"'.format(
                toolkit.url_for("dcat_forms.dataset_select_search")
            )
            in resp.body
        )
        assert 'data-module-field-name="serves_dataset"' in resp.body
        assert (
            '<input type="hidden" name="serves_dataset" value="{}" />'.format(
                dataset["id"]
            )
            in resp.body
        )
        assert "Served dataset" in resp.body
        assert "-dataset-select.js" in resp.body
        assert 'data-module="dcat-dataset-select"' in resp.body

    @pytest.mark.parametrize("num_datasets", [0, 1, 2])
    def test_edit_form_submit(self, app, num_datasets):
        user = factories.UserWithToken()
        org = factories.Organization(
            users=[{"name": user["name"], "capacity": "editor"}]
        )
        datasets = [factories.Dataset(owner_org=org["id"]) for _ in range(2)]
        data_service = factories.Dataset(
            type="data_service",
            owner_org=org["id"],
            serves_dataset=[datasets[0]["id"]],
        )

        selected = [d["id"] for d in datasets[:num_datasets]]
        app.post(
            toolkit.url_for("data_service.edit", id=data_service["name"]),
            data={
                "name": data_service["name"],
                "title": data_service["title"],
                "notes": "",
                "owner_org": org["id"],
                "serves_dataset": selected,
                "save": "",
            },
            headers={"Authorization": user["token"]},
            follow_redirects=False,
            status=302,
        )

        data_service = model.Package.get(data_service["id"])
        stored = data_service.extras.get("serves_dataset")
        assert (json.loads(stored) if stored else []) == selected

    def test_edit_form_inline_error(self, app):
        user = factories.UserWithToken()
        org = factories.Organization(
            users=[{"name": user["name"], "capacity": "editor"}]
        )
        not_editable = factories.Dataset(owner_org=factories.Organization()["id"])
        data_service = factories.Dataset(type="data_service", owner_org=org["id"])

        resp = app.post(
            toolkit.url_for("data_service.edit", id=data_service["name"]),
            data={
                "name": data_service["name"],
                "title": data_service["title"],
                "notes": "",
                "owner_org": org["id"],
                "serves_dataset": [not_editable["id"]],
                "save": "",
            },
            headers={"Authorization": user["token"]},
        )

        assert "User not authorized to add these datasets" in resp.body
        assert '<span class="error-block">' in resp.body
        # The submitted value is kept in the form
        assert (
            '<input type="hidden" name="serves_dataset" value="{}" />'.format(
                not_editable["id"]
            )
            in resp.body
        )


class TestDatasetSelectDisplay:

    def test_display_snippet(self, app):
        dataset = factories.Dataset(title="Served dataset")
        private_dataset = factories.Dataset(
            title="Hidden dataset",
            owner_org=factories.Organization()["id"],
            private=True,
        )
        data_service = factories.Dataset(
            type="data_service",
            serves_dataset=[dataset["id"], private_dataset["id"]],
        )

        resp = app.get(toolkit.url_for("data_service.read", id=data_service["name"]))

        assert "Serves dataset(s)" in resp.body
        assert toolkit.url_for("dataset.read", id=dataset["name"]) in resp.body
        assert "Hidden dataset" not in resp.body
