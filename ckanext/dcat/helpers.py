"""
Helpers used by templates
"""

import simplejson as json

import ckantoolkit as toolkit

from pyld import jsonld

from ckanext.dcat.processors import RDFSerializer
from ckanext.dcat.profiles.croissant import JSONLD_CONTEXT

config = toolkit.config


ENABLE_RDF_ENDPOINTS_CONFIG = "ckanext.dcat.enable_rdf_endpoints"


def endpoints_enabled():
    return toolkit.asbool(config.get(ENABLE_RDF_ENDPOINTS_CONFIG, True))


def get_endpoint(_type="dataset"):
    return "dcat.read_dataset" if _type == "dataset" else "dcat.read_catalog"


def _get_serialization(
    dataset_dict, profiles=None, _format="jsonld", context=None, frame=None
):

    serializer = RDFSerializer(profiles=profiles)

    output = serializer.serialize_dataset(
        dataset_dict, _format=_format, context=context
    )

    # parse result again to prevent UnicodeDecodeError and add formatting

    if _format == "jsonld":
        try:
            json_data = json.loads(output)

            if frame:
                json_data = jsonld.frame(json_data, frame)

            return json.dumps(
                json_data,
                sort_keys=True,
                indent=4,
                separators=(",", ": "),
                cls=json.JSONEncoderForHTML,
            )
        except ValueError:
            # result was not JSON, return anyway
            pass
    return output


def structured_data(dataset_dict, profiles=None):
    """
    Returns a string containing the structured data of the given
    dataset id and using the given profiles (if no profiles are supplied
    the default profiles are used).

    This string can be used in the frontend.
    """

    if not profiles:
        profiles = config.get("ckanext.dcat.structured_data.profiles", ["schemaorg"])

    return _get_serialization(dataset_dict, profiles, "jsonld")


def croissant(dataset_dict, profiles=None, jsonld_context=None):
    """
    Returns a string containing the Croissant ML representation of the given
    dataset using the `croissant` profile.
    This string can be used in the frontend.
    """

    if not profiles:
        profiles = config.get("ckanext.dcat.croissant.profiles", ["croissant"])

    context = jsonld_context or JSONLD_CONTEXT

    frame = {"@context": context, "@type": "sc:Dataset"}

    return _get_serialization(
        dataset_dict, profiles, "jsonld", context=context, frame=frame
    )


def solr_or_query(values):
    """
    Returns a Solr query string matching any of the provided values, eg
    `"value1" OR "value2"`, with the values quoted and escaped.
    """
    return " OR ".join(
        '"{}"'.format(value.replace("\\", "\\\\").replace('"', '\\"'))
        for value in values
    )


def get_datasets_by_ids(ids):
    """
    Returns the dataset dicts for the provided list of dataset ids.

    Datasets that don't exist or that the current user can not access are
    returned as {"id": <id>} so they are not dropped when saving the form.
    """
    if not ids:
        return []

    if isinstance(ids, str):
        try:
            ids = json.loads(ids)
        except ValueError:
            ids = [ids]

    if not isinstance(ids, list):
        return []

    ids = [_id for _id in ids if _id and isinstance(_id, str)]
    if not ids:
        return []

    user = toolkit.current_user
    result = toolkit.get_action("package_search")(
        {"user": user.name if user.is_authenticated else ""},
        {
            "fq_list": ["id:({0})".format(solr_or_query(ids))],
            "include_private": True,
            "rows": len(ids),
        },
    )

    found = {}
    for dataset_dict in result["results"]:
        found[dataset_dict["id"]] = dataset_dict

    return [found.get(_id, {"id": _id}) for _id in ids]


def format_select2_options(dataset_dicts):

    out = []
    for dataset_dict in dataset_dicts:

        if not dataset_dict.get("name"):
            out.append({
                "id": dataset_dict["id"],
                "text": toolkit._("Restricted dataset"),
            })
            continue

        organization = dataset_dict.get("organization") or {}
        out.append(
            {
                "id": dataset_dict["id"],
                "text": toolkit.h.dataset_display_name(dataset_dict),
                "url": toolkit.url_for(
                    "{}.read".format(dataset_dict["type"]), id=dataset_dict["name"]
                ),
                "organization": organization.get("title") or organization.get("name"),
            }
        )

    return out


def dataset_select_options(dataset_ids):
    """
    Returns the select2 options for the provided list of dataset ids.
    """

    dataset_dicts = get_datasets_by_ids(dataset_ids)

    return format_select2_options(dataset_dicts)
