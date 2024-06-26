from rdflib import URIRef, Literal

from .. import codelists
from ..utils import resource_uri
from .base import (
    RDFProfile,
    CleanedURIRef,
    DCAT,
    DCT,
    RDF,
    RDFS,
    XSD,
    QB,
    STAT,
    DQV,
    OA,
)

class EuropeanDCATAPStatProfile(RDFProfile):
    def graph_from_dataset(self, dataset_dict, dataset_ref):
        # Implementing these as flat URIs for now
        self._add_triple_from_dict(dataset_dict, dataset_ref, STAT.attribute, 'stat_attributes', list_value=True, _class=QB.AttributeProberty, _type=URIRef)
        self._add_triple_from_dict(dataset_dict, dataset_ref, STAT.dimension, 'stat_dimensions', list_value=True, _class=QB.DimensionProberty, _type=URIRef)

        self._add_triple_from_dict(dataset_dict, dataset_ref, STAT.numSeries, 'stat_num_series', list_value=True, _datatype=XSD.integer, _type=Literal)

        self._add_from_codelist(dataset_dict, dataset_ref, STAT.statUnitMeasure, 'stat_unit_measure', codelists.measurement_units, list_value=True)

        for quality_annotation in dataset_dict.get('stat_quality_annotations', []):
            ref = CleanedURIRef(quality_annotation['uri'])
            self.g.add((dataset_ref, DQV.hasQualityAnnotation, ref))
            self.g.add((ref, RDF.type, OA.Annotation))
            self.g.add((ref, OA.hasTarget, dataset_ref))

            self._add_triple_from_dict(quality_annotation, ref, OA.motivation, 'motivation')

            body = quality_annotation.get('body', '')
            if body.startswith('http'):
                self.g.add((ref, OA.hasBody, CleanedURIRef(body)))
            elif body:
                self.g.add((ref, OA.bodyText, Literal(body)))
                

        for resource in dataset_dict.get('resources', []):
            distribution = CleanedURIRef(resource_uri(resource))
            self._add_from_codelist(resource, distribution, DCT.type, 'stat_type', codelists.distribution_types)
