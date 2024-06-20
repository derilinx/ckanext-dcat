import sys
from pathlib import Path

from rdflib import Graph

namespaces = dict(
    dc="http://purl.org/dc/elements/1.1/",
    at="http://publications.europa.eu/ontology/authority/",
)

Graph().parse(sys.stdin, format='xml').query('''
    CONSTRUCT WHERE {
        ?uri at:op-mapped-code [
            dc:source "iso-639-1";
            at:legacy-code ?lang;
        ].
    }
''', initNs=namespaces).graph.serialize((Path(__file__).parent / 'languages-skos.rdf').open('wb'), format='xml')
