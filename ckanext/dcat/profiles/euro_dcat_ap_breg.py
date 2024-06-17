from rdflib import URIRef, Literal
from .base import (
    RDFProfile,
    RDF,
    RDFS,
    DCT,
    DCAT,
    DQV,
    CPSV,
    URIRefOrLiteral
)

class EuropeanDCATAPBRegProfile(RDFProfile):
    DATASET_FIELDS = [
        ('identifiers', DCT.identifier, None, Literal, None),
        ('has_part', DCT.hasPart, None, URIRefOrLiteral, DCAT.Dataset),
        ('has_quality_annotation', DQV.hasQualityAnnotation, None, URIRefOrLiteral, DQV.QualityAnnotation),
        ('has_quality_measurement', DQV.hasQualityMeasurement, None, URIRefOrLiteral, DQV.QualityMeasurement),
        ('is_part_of', DCT.isPartOf, None, URIRefOrLiteral, DCAT.Dataset),
        ('is_replaced_by', DCT.isReplacedBy, None, URIRefOrLiteral, DCAT.Dataset),
        ('is_required_by', DCT.isRequiredBy, None, URIRefOrLiteral, DCAT.Dataset),
        ('references', DCT.references, None, URIRefOrLiteral, RDFS.Resource),
        ('requires', DCT.requires, None, URIRefOrLiteral, DCAT.Dataset),
    ]

    # TODO: Data Services, all the other BReg classes

    def parse_dataset(self, dataset_dict, dataset_ref):
        for (pred, key, cls, type) in self.DATASET_FIELDS:
            dataset_dict[key] = self._object_value_list(dataset_ref, pred)

        dataset_dict['follows_rules'] = [
            {
                'identifier': str(rule),
                'title': str(self.g.value(rule, DCT.title)),
                'description': str(self.g.value(rule, DCT.description))
            }
            for rule in self.g.objects(dataset_ref, CPSV.follows)
        ]


    def graph_from_dataset(self, dataset_dict, dataset_ref):
        self._add_list_triples_from_dict(dataset_dict, dataset_ref, self.DATASET_FIELDS)

        for rule in dataset_dict.get('follows_rules', []):
            ref = URIRef(rule['identifier'])
            self.g.add((dataset_ref, CPSV.follows, ref))
            self.g.add((ref, RDF.type, CPSV.Rule))
            self.g.add((ref, DCT.identifier, Literal(str(ref))))
            self.g.add((ref, DCT.title, Literal(rule['title'])))
            self.g.add((ref, DCT.description, Literal(rule['description'])))

