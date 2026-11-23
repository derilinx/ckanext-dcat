import json
from pathlib import Path

from ckanext.dcat.codelists.extract import Codelist, extract_codelist

CODELISTS_DIR = Path(__file__).parent.parent / "codelists"

HVD_FILE = CODELISTS_DIR / "high-value-dataset-category.rdf"
DATA_THEME_FILE = CODELISTS_DIR / "data-theme-skos.rdf"

HVD_PREFIX = "http://data.europa.eu/bna/"


def _rdf(concepts):
    """Build a minimal SKOS RDF/XML document from (uri, label, order, broader)"""
    descriptions = []
    for uri, label, order, broader in concepts:
        broader_xml = (
            '<skos:broader rdf:resource="{}"/>'.format(broader) if broader else ""
        )
        descriptions.append(
            """
  <rdf:Description rdf:about="{uri}">
    <rdf:type rdf:resource="http://www.w3.org/2004/02/skos/core#Concept"/>
    <skos:prefLabel xml:lang="en">{label}</skos:prefLabel>
    <euvoc:order>{order}</euvoc:order>
    {broader_xml}
  </rdf:Description>""".format(
                uri=uri, label=label, order=order, broader_xml=broader_xml
            )
        )
    return """<?xml version="1.0" encoding="utf-8"?>
<rdf:RDF
  xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  xmlns:skos="http://www.w3.org/2004/02/skos/core#"
  xmlns:euvoc="http://publications.europa.eu/ontology/euvoc#">
  <rdf:Description rdf:about="http://example.org/scheme">
    <rdf:type rdf:resource="http://www.w3.org/2004/02/skos/core#ConceptScheme"/>
  </rdf:Description>{}
</rdf:RDF>""".format(
        "".join(descriptions)
    )


def _count(nodes):
    return sum(1 + _count(n.get("children", [])) for n in nodes)


class TestHierarchicalCodelist:
    def setup_method(self):
        self.codelist = extract_codelist("hvd", HVD_FILE, langs=("en",))

    def test_choices_flat_contains_all_elements(self):
        assert len(self.codelist.choices) == 96
        assert len(self.codelist.choices_map) == 96
        assert all("children" not in c for c in self.codelist.choices)

    def test_lookup_any_level(self):
        # top-level, second and third level elements
        assert self.codelist.labels(HVD_PREFIX + "c_164e0bf5") == {
            "en": "Meteorological"
        }
        for choice in self.codelist.choices:
            assert self.codelist.labels(choice["value"]) == choice["label"]

    def test_parent_property(self):
        choices = {c["value"]: c for c in self.codelist.choices}

        top_level = [c for c in choices.values() if "parent" not in c]
        assert len(top_level) == 6

        for choice in choices.values():
            if "parent" in choice:
                assert choice["parent"] in choices

    def test_choices_ordered_parents_first(self):
        seen = set()
        for choice in self.codelist.choices:
            if "parent" in choice:
                assert choice["parent"] in seen
            seen.add(choice["value"])

    def test_choices_nested(self):
        nested = self.codelist.choices_nested

        assert len(nested) == 6
        assert _count(nested) == 96

        def check(nodes, parent):
            for node in nodes:
                assert set(node.keys()) <= {"label", "value", "children"}
                if "children" in node:
                    assert node["children"]
                    check(node["children"], node["value"])
                flat = next(
                    c for c in self.codelist.choices if c["value"] == node["value"]
                )
                assert flat.get("parent") == parent

        check(nested, None)

        depths = []

        def depth(nodes, d):
            for node in nodes:
                depths.append(d)
                depth(node.get("children", []), d + 1)

        depth(nested, 1)
        assert max(depths) == 3


class TestFlatCodelist:
    def test_nested_same_as_choices(self):
        codelist = extract_codelist("data_theme", DATA_THEME_FILE, langs=("en",))

        assert codelist.choices
        assert all("parent" not in c for c in codelist.choices)
        assert codelist.choices_nested == codelist.choices


class TestCodelistFromRDF:
    def test_children_sorted_by_order(self, tmp_path):
        path = tmp_path / "test.rdf"
        path.write_text(
            _rdf(
                [
                    ("http://example.org/b", "B", "02", None),
                    ("http://example.org/b2", "B2", "0202", "http://example.org/b"),
                    ("http://example.org/a", "A", "01", None),
                    ("http://example.org/b1", "B1", "0201", "http://example.org/b"),
                ]
            )
        )
        codelist = extract_codelist("test", path, langs=("en",))

        assert [c["value"] for c in codelist.choices] == [
            "http://example.org/a",
            "http://example.org/b",
            "http://example.org/b1",
            "http://example.org/b2",
        ]
        assert codelist.choices_nested == [
            {"label": {"en": "A"}, "value": "http://example.org/a"},
            {
                "label": {"en": "B"},
                "value": "http://example.org/b",
                "children": [
                    {"label": {"en": "B1"}, "value": "http://example.org/b1"},
                    {"label": {"en": "B2"}, "value": "http://example.org/b2"},
                ],
            },
        ]

    def test_unknown_broader_is_top_level(self, tmp_path):
        path = tmp_path / "test.rdf"
        path.write_text(
            _rdf(
                [
                    ("http://example.org/a", "A", "01", None),
                    (
                        "http://example.org/orphan",
                        "Orphan",
                        "0901",
                        "http://example.org/missing",
                    ),
                ]
            )
        )
        codelist = extract_codelist("test", path, langs=("en",))

        assert all("parent" not in c for c in codelist.choices)
        assert [n["value"] for n in codelist.choices_nested] == [
            "http://example.org/a",
            "http://example.org/orphan",
        ]
