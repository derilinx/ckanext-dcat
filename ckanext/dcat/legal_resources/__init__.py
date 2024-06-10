from pathlib import Path
from rdflib import Graph, URIRef, Namespace

ELI = Namespace("http://data.europa.eu/eli/ontology#")
DCT = Namespace("http://purl.org/dc/terms/")

graph = Graph().parse(Path(__file__).parent / 'legal_resources.ttl')
graph.bind("dct", DCT)
graph.bind("eli", ELI)


KNOWN_ELIS = [
    str(uri) for (uri, )
    in graph.query("""
        SELECT ?uri
        WHERE {
            ?uri a eli:LegalResource.
        }
    """)
]

def info(eli):
    # Take the graph limited to nodes with the given ELI as a subject,
    # or with the object of such a node as the subject, i.e. with
    # transitive objects, but only one layer
    return graph.query("""
        CONSTRUCT { ?uri ?p ?o. ?o ?p2 ?o2. }
        WHERE {
             ?uri ?p ?o.
             OPTIONAL { ?o ?p2 ?o2. }
        }
    """, initBindings={'uri': URIRef(eli)}).graph
