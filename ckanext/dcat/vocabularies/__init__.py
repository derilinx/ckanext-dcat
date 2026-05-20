import logging

from pathlib import Path

from rdflib.plugins.sparql import prepareQuery
from rdflib import Graph, Literal, URIRef
from rdflib.term import Variable

log = logging.getLogger(__name__)

namespaces = dict(
    dc="http://purl.org/dc/elements/1.1/",
    at="http://publications.europa.eu/ontology/authority/",
)


class Vocabulary:

    _cache = {}

    def __init__(self, vocab_filename, query):
        self.query = prepareQuery(query, initNs=namespaces)

        self.vocab_filename = vocab_filename

        self.graph = Graph()
        with open(Path(__file__).parent / vocab_filename) as f:
            self.graph.parse(f, format='xml')

    def lookup(self, ckan=None, uri=None):
        out = None

        if isinstance(ckan, str):
            # Use base form for languages like en_GB
            ckan = ckan.split("_")[0]
            ckan = Literal(ckan)

        key = "ckan" if ckan else "uri"
        out_key = "uri" if ckan else "ckan"
        value = ckan if ckan else uri

        if value and self._cache.get(value):
            return self._cache[value]

        try:
            (binding, ) = self.graph.query(self.query, initBindings={key: value}).bindings
            out = binding[Variable(out_key)]
        except ValueError:
            log.debug("Could not lookup value for vocabulary %s: %s", self.vocab_filename, value)
            out = Literal(value)

        self._cache[value] = out

        return out


languages = Vocabulary('languages-skos.rdf', """
    SELECT DISTINCT ?uri ?ckan
    WHERE {
        ?uri at:op-mapped-code [
            dc:source "iso-639-1";
            at:legacy-code ?ckan
        ].
    }
""")

file_types = Vocabulary('filetypes-skos.rdf', """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX euvoc: <http://publications.europa.eu/ontology/euvoc#>

SELECT ?uri ?ckan
WHERE {
  ?uri a skos:Concept ;
           dc:identifier ?ext ;
  FILTER(STR(?ext) = ?ckan)
}
""")

