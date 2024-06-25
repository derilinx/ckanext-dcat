The `languages-skos.rdf` file is in this directory is extracted from
[op.europa.eu](https://op.europa.eu/en/web/eu-vocabularies/dataset/-/resource?uri=http://publications.europa.eu/resource/dataset/language),
and can be downloaded directly from 
[here](https://op.europa.eu/o/opportal-service/euvoc-download-handler?cellarURI=http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fcellar%2F381c0438-e0fa-11ee-8b2b-01aa75ed71a1.0001.05%2FDOC_1&fileName=languages-skos.rdf).
The license is unclear.

To regenerate it, run `curl 'https://op.europa.eu/o/opportal-service/euvoc-download-handler?cellarURI=http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fcellar%2F381c0438-e0fa-11ee-8b2b-01aa75ed71a1.0001.05%2FDOC_1&fileName=languages-skos.rdf' | python extract.py`
with python's `rdflib` installed.
