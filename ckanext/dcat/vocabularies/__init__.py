from pathlib import Path

from rdflib.plugins.sparql import prepareQuery
from rdflib import Graph, Literal
from rdflib.term import Variable

namespaces = dict(
    dc="http://purl.org/dc/elements/1.1/",
    at="http://publications.europa.eu/ontology/authority/",
)

class Vocabulary:
    def __init__(self, vocab_filename, query):
        self.query = prepareQuery(query, initNs=namespaces)

        self.graph = Graph()
        with open(Path(__file__).parent / vocab_filename) as f:
            self.graph.parse(f, format='xml')

    def lookup(self, ckan=None, uri=None):
        if isinstance(ckan, str):
            ckan = Literal(ckan)
        (binding, ) = self.graph.query(self.query, initBindings={'ckan': ckan} if ckan else {'uri': uri}).bindings
        return binding[Variable('uri' if ckan else 'ckan')]


languages = Vocabulary('languages-skos.rdf', """
    SELECT DISTINCT ?uri ?ckan
    WHERE {
        ?uri at:op-mapped-code [
            dc:source "iso-639-1";
            at:legacy-code ?ckan
        ].
    }
""")
