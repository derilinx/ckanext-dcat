import re

from flask import Blueprint, jsonify, make_response

import ckantoolkit as toolkit

from ckan.views.dataset import CreateView

import ckan.plugins.toolkit as toolkit
import ckanext.dcat.utils as utils
from ckanext.dcat.helpers import (
    format_select2_options,
    endpoints_enabled,
    solr_or_query,
)

import ckan.lib.base as base
from ckan.lib.search import SearchError
from ckan.common import _
import ckan.logic as logic

NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized

config = toolkit.config


dcat = Blueprint("dcat", __name__, url_defaults={"package_type": "dataset"})


def read_catalog(_format=None, package_type=None):
    return utils.read_catalog_page(_format)


def read_dataset(_id, _format=None, package_type=None):
    try:
        return utils.read_dataset_page(_id, _format)
    except NotFound:
        return base.abort(404, _(u'Dataset not found'))
    except NotAuthorized:
        return base.abort(403, _(u'Unauthorized to read package %s') % _id)


if endpoints_enabled():

    # requirements={'_format': 'xml|rdf|n3|ttl|jsonld'}
    dcat.add_url_rule(
        config.get(
            "ckanext.dcat.catalog_endpoint", utils.DEFAULT_CATALOG_ENDPOINT
        ).replace("{_format}", "<_format>"),
        view_func=read_catalog,
    )

    # TODO: Generalize for all dataset types
    dcat.add_url_rule(
        "/dataset_series/<_id>.<_format>",
        view_func=read_dataset,
        endpoint="read_dataset_series",
    )
    dcat.add_url_rule(
        "/data_service/<_id>.<_format>",
        view_func=read_dataset,
        endpoint="read_data_service",
    )

    dcat.add_url_rule(
        "/dataset/<_id>.<_format>", view_func=read_dataset, endpoint="read_dataset"
    )


if toolkit.asbool(config.get(utils.ENABLE_CONTENT_NEGOTIATION_CONFIG)):
    dcat.add_url_rule("/", view_func=read_catalog)

    dcat.add_url_rule("/dataset/new", view_func=CreateView.as_view(str("new")))
    dcat.add_url_rule("/dataset/<_id>", view_func=read_dataset)

dcat_json_interface = Blueprint("dcat_json_interface", __name__)


def dcat_json():
    datasets = utils.dcat_json_page()
    return jsonify(datasets)


dcat_json_interface.add_url_rule(
    config.get("ckanext.dcat.json_endpoint", "/dcat.json"), view_func=dcat_json
)


croissant = Blueprint("croissant", __name__)


def read_dataset_croissant(_id):

    try:
        user_name = (
            toolkit.current_user.name
            if hasattr(toolkit, "current_user")
            else toolkit.g.user
        )

        context = {
            "user": user_name,
        }
        data_dict = {"id": _id}

        dataset_dict = toolkit.get_action("package_show")(context, data_dict)
    except (toolkit.ObjectNotFound, toolkit.NotAuthorized):
        return toolkit.abort(
            404, toolkit._("Dataset not found or you have no permission to view it")
        )

    response = make_response(toolkit.h.croissant(dataset_dict))
    response.headers["Content-type"] = "application/ld+json"

    return response


croissant.add_url_rule(
    "/dataset/<_id>/croissant.jsonld", view_func=read_dataset_croissant
)


dcat_forms = Blueprint("dcat_forms", __name__, url_prefix="/dcat/forms")

DATASET_SELECT_ROWS = 10

_solr_special_re = re.compile(r'([+\-&|!(){}\[\]^"~*?:\\/])')


def _build_solr_query(q):
    """
    Turns the user input into a Solr query that also matches partial words
    as the user types, eg `Test g` -> `Test (g OR g*)`. The exact term is kept
    as well because wildcard terms are not stemmed.
    """
    terms = []
    for term in q.split():
        # Don't let user input act as boolean operators
        if term in ("AND", "OR", "NOT"):
            term = term.lower()
        terms.append(_solr_special_re.sub(r"\\\1", term))
    terms[-1] = "({0} OR {0}*)".format(terms[-1])
    return " ".join(terms)


def dataset_select_search():
    """
    Returns the datasets matching the query that the current user can edit,
    in the format expected by select2.

    Params:
        q: search terms
        page: results page, starting at 1
        dataset_type: comma-separated list of dataset types, defaults to dataset
        selected: comma-separated list of dataset ids to exclude
    """
    user = toolkit.current_user
    if not user.is_authenticated:
        return toolkit.abort(403, _("Not authorized to see this page"))

    args = toolkit.request.args
    no_results = {"results": [], "more": False}

    q = args.get("q", "").strip()
    if not q:
        return jsonify(no_results)

    try:
        page = max(int(args.get("page", 1)), 1)
    except ValueError:
        page = 1

    dataset_types = [
        t for t in args.get("dataset_type", "dataset").split(",")
    ] or ["dataset"]

    fq_list = ["dataset_type:({})".format(solr_or_query(dataset_types))]

    if not user.sysadmin:
        orgs = toolkit.get_action("organization_list_for_user")(
            {"user": user.name}, {"permission": "update_dataset"}
        )
        if not orgs:
            return jsonify(no_results)
        fq_list.append(
            "owner_org:({})".format(solr_or_query([org["id"] for org in orgs]))
        )

    selected = [_id for _id in args.get("selected", "").split(",") if _id]
    if selected:
        fq_list.append("-id:({})".format(solr_or_query(selected)))

    try:
        result = toolkit.get_action("package_search")(
            {"user": user.name},
            {
                "q": _build_solr_query(q),
                "defType": "edismax",
                "fq_list": fq_list,
                "include_private": True,
                "rows": DATASET_SELECT_ROWS,
                "start": (page - 1) * DATASET_SELECT_ROWS,
            },
        )
    except (toolkit.ValidationError, SearchError):
        return jsonify(no_results)

    return jsonify(
        {
            "results": format_select2_options(result["results"]),
            "more": result["count"] > page * DATASET_SELECT_ROWS,
        }
    )


dcat_forms.add_url_rule("/dataset-search", view_func=dataset_select_search)
