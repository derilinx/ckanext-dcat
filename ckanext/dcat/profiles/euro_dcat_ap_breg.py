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
    BREG_FIELDS = [
        (DCT.identifier, 'identifiers', None),
        (DCT.hasPart, 'has_part', DCAT.Dataset),
        (DQV.hasQualityAnnotation, 'has_quality_annotation', DQV.QualityAnnotation),
        (DQV.hasQualityMeasurement, 'has_quality_measurement', DQV.QualityMeasurement),
        (DCT.isPartOf, 'is_part_of', DCAT.Dataset),
        (DCT.isReplacedBy, 'is_replaced_by', DCAT.Dataset),
        (DCT.isRequiredBy, 'is_required_by', DCAT.Dataset),
        (DCT.references, 'references', RDFS.Resource),
        (DCT.requires, 'requires', DCAT.Dataset),
    ]
    # TODO: Data Services, all the other BReg classes

    def parse_dataset(self, dataset_dict, dataset_ref):
        for (pred, key, cls) in self.BREG_FIELDS:
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
        for (pred, key, cls) in self.BREG_FIELDS:
            self._add_triple_from_dict(dataset_dict, dataset_ref, pred, key, _type=URIRefOrLiteral, list_value=True, _class=cls)

        for rule in dataset_dict.get('follows_rules', []):
            ref = URIRef(rule['identifier'])
            self.g.add((dataset_ref, CPSV.follows, ref))
            self.g.add((ref, RDF.type, CPSV.Rule))
            self.g.add((ref, DCT.identifier, Literal(str(ref))))
            self.g.add((ref, DCT.title, Literal(rule['title'])))
            self.g.add((ref, DCT.description, Literal(rule['description'])))

