"""
   Compile a graph of `eli:LegalResource`s for a set of EurLex references 
"""

import rdflib
import rdflib.plugins.sparql
import bs4
import requests
import sys

ELIS = [
    "http://data.europa.eu/eli/dir/2007/2/2019-06-26",
    "http://data.europa.eu/eli/reg_impl/2023/138/oj"
 ]

ELI = rdflib.Namespace("http://data.europa.eu/eli/ontology#")
DCT = rdflib.Namespace("http://purl.org/dc/terms/")

# Handle absolute or prefixed URIs
def uriref(g, uri):
   if uri.startswith("http"):
      return rdflib.URIRef(uri)
   else:
      (ns, name) = uri.split(':')
      return next(namespace for (prefix, namespace) in g.namespace_manager.namespaces() if prefix == ns) + name

def insert_from_rdfa(g, document):
   # In EurLex documents, all rdfa metadata is in <meta about=...> tags
   # We only parse the tags relevant to us
   soup = bs4.BeautifulSoup(document, "html.parser", parse_only=bs4.SoupStrainer("meta", about=True))

   for tag in soup.contents:
      ref = uriref(g, tag["about"])
      if tag.has_attr("typeof"):
         g.add((ref, rdflib.RDF.type, uriref(g, tag["typeof"])))
      elif tag.has_attr("resource"):
         g.add((ref, uriref(g, tag["property"]), uriref(g, tag["resource"])))
      else:
         g.add((
            ref,
            uriref(g, tag["property"]),
            rdflib.Literal(tag["content"], lang=tag.get("lang"))
         ))

QUERY = rdflib.plugins.sparql.prepareQuery("""
   CONSTRUCT {
      ?uri
         a eli:LegalResource;
         dct:description ?title;
         dct:type ?type.
      ?type a eli:ResourceType.
   } WHERE { 
      ?uri
         a eli:LegalResource;
         eli:is_realized_by/eli:title ?title;
         eli:type_document ?type.
   }
""", initNs=dict(eli=ELI, dct=DCT))

if __name__ == "__main__":
   g = rdflib.Graph()
   g.bind("dct", DCT)
   g.bind("eli", ELI)

   for eli in ELIS + sys.argv[1:]:
      print(f"Getting {eli}", file=sys.stderr)

      r = requests.get(eli)
      r.raise_for_status()

      insert_from_rdfa(g, r.text)

   g.query(QUERY).graph.print()
