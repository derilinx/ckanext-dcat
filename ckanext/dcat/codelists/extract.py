'''
Converts EU RDF descriptions of codelists to ckanext-scheming choices.

<rdf:Description rdf:about="http://data.europa.eu/bna/c_164e0bf5">
	<startDate xmlns="http://publications.europa.eu/ontology/euvoc#" rdf:datatype="http://www.w3.org/2001/XMLSchema#date">2019-07-06</startDate>
	<created xmlns="http://purl.org/dc/terms/" rdf:datatype="http://www.w3.org/2001/XMLSchema#date">2023-09-05</created>
	<modified xmlns="http://purl.org/dc/terms/" rdf:datatype="http://www.w3.org/2001/XMLSchema#date">2023-09-13</modified>
	<rdf:type rdf:resource="http://www.w3.org/2004/02/skos/core#Concept"/>
	<prefLabel xmlns="http://www.w3.org/2004/02/skos/core#" xml:lang="en">Meteorological</prefLabel>
	<prefLabel xmlns="http://www.w3.org/2004/02/skos/core#" xml:lang="es">Meteorología</prefLabel>
	<prefLabel xmlns="http://www.w3.org/2004/02/skos/core#" xml:lang="lv">Meteoroloģijas datu kopas</prefLabel>
	<prefLabel xmlns="http://www.w3.org/2004/02/skos/core#" xml:lang="mt">Data meteoroloġika</prefLabel>
	<prefLabel xmlns="http://www.w3.org/2004/02/skos/core#" xml:lang="nl">Meteorologische data</prefLabel>
	<prefLabel xmlns="http://www.w3.org/2004/02/skos/core#" xml:lang="sv">Meteorologiska data</prefLabel>
        ...
	<order xmlns="http://publications.europa.eu/ontology/euvoc#" rdf:datatype="http://www.w3.org/2001/XMLSchema#integer">3</order>
	<definition xmlns="http://www.w3.org/2004/02/skos/core#" xml:lang="en">data sets as described in Commission Implementing Regulation (EU) 2023/138 of 21 December 2022 laying down a list of specific high-value datasets and the arrangements for their publication and re-use, Annex, Section 3</definition>
	<inScheme xmlns="http://www.w3.org/2004/02/skos/core#" rdf:resource="http://data.europa.eu/bna/asd487ae75"/>
	<topConceptOf xmlns="http://www.w3.org/2004/02/skos/core#" rdf:resource="http://data.europa.eu/bna/asd487ae75"/>
</rdf:Description>

to:

 {
    "label": {
      "en": "Meteorological",
      "ga": "Meit\u00e9areola\u00edoch",
      "mt": "Data meteorolo\u0121ika"
    },
    "value": "http://data.europa.eu/bna/c_164e0bf5"
  },


These are suitable for inclusion in the schema.json.

'''

import rdflib
import rdflib.parser

from rdflib.namespace import Namespace, RDF, SKOS

from pathlib import Path
import xml
import json
from functools import lru_cache

from ckan.plugins import toolkit

import logging
log = logging.getLogger(__name__)


EUVOC = Namespace("http://publications.europa.eu/ontology/euvoc#")


class Codelist:
    """
    A list of choices extracted from a SKOS concept scheme.

    `choices` is a flat list with all elements regardless of their position in
    the hierarchy. Each element has `label` and `value` keys, plus a `parent`
    key (the URI of its skos:broader concept) for non top-level elements.

    `choices_nested` contains the same elements organized as a tree, with
    child elements listed under a `children` key. For codelists without a
    hierarchy it is equivalent to `choices`.
    """

    def __init__(self, name, choices, scheme):
        self.name = name
        self.choices = choices
        self.scheme = scheme
        self.choices_map = {elt['value']: elt['label'] for elt in choices}
        self.choices_nested = self._build_nested(choices)

    @staticmethod
    def _build_nested(choices):
        nodes = {
            elt['value']: {'label': elt['label'], 'value': elt['value']}
            for elt in choices
        }
        nested = []
        for elt in choices:
            node = nodes[elt['value']]
            parent = nodes.get(elt.get('parent'))
            if parent is not None:
                parent.setdefault('children', []).append(node)
            else:
                nested.append(node)
        return nested

    def labels(self, val):
        return self.choices_map.get(val, {})

    def write_json(self, dest=None):
        if dest is None:
            dest = Path(__file__).parent / (self.name.replace("_", "-") + '.json')
        with open(dest, 'w') as f:
            json.dump(self.choices_nested, f, indent=2)
        return dest


@lru_cache(None)
def extract_codelist(name: str, f:Path, langs=None):

    if not langs:
        langs = toolkit.aslist(toolkit.config.get('ckan.locales_offered')) or ['en']
        # filter out variants of languages, en_GB doesn't match en.
        langs = set(l.split('_')[0] for l in langs)

    g = rdflib.ConjunctiveGraph()
    try:
        g.parse(data=f.read_text(), format='xml')
    # Apparently there is no single way of catching exceptions from all
    # rdflib parsers at once, so if you use a new one and the parsing
    # exceptions are not cached, add them here.
    # PluginException indicates that an unknown format was passed.
    except (SyntaxError, xml.sax.SAXParseException,
            rdflib.plugin.PluginException, TypeError) as e:
        raise Exception(e)

    concepts = set(g.subjects(RDF.type, SKOS.Concept))
    choices = []

    for subject in concepts:
        labels = {l.language: str(l) for l in g.objects(subject, SKOS.prefLabel) if l.language in langs }
        order = g.value(subject, EUVOC.order, default=str(subject))

        choice = {"label": labels,
                  "value": str(subject)}

        parent = g.value(subject, SKOS.broader)
        if parent is not None:
            if parent in concepts:
                choice["parent"] = str(parent)
            else:
                log.warning(
                    "Codelist '%s': broader concept %s of %s not found, "
                    "treating it as a top-level element", name, parent, subject)

        choices.append((order, choice))

    ordered_choices = [choice for order, choice in sorted(choices, key=lambda c: c[0])]

    scheme = None
    for subject in g.subjects(RDF.type, SKOS.ConceptScheme):
        scheme = str(subject)

    log.debug("Extracting codelist '%s' with label languages %s", name, langs)
    return Codelist(name, ordered_choices, scheme)


def write_json():
    for path in Path(__file__).parent.glob('*.rdf'):
        codelist = extract_codelist(path.stem, path)
        codelist.write_json(path.parent / (path.stem + '.json'))
