from rdflib import URIRef, Literal, BNode
from .base import (
    RDFProfile,
    RDF,
    RDFS,
    DCT,
    DCAT,
    DQV,
    CPSV,
    SKOS,
    VCARD,
    FOAF,
    CleanedURIRef,
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

    DATASERVICE_FIELDS = [
        ('identifiers', DCT.identifier, None, Literal, None),
        ('keywords', DCAT.keyword, None, URIRefOrLiteral, None),
        ('themes', DCAT.theme, None, URIRefOrLiteral, SKOS.Concept),
        ('conforms_to', DCT.conformsTo, None, URIRefOrLiteral, DCT.Standard),
        ('landing_page', DCT.landingPage, None, URIRefOrLiteral, FOAF.Document),
        ('documentation', FOAF.page, None, URIRefOrLiteral, FOAF.Document),
    ]

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

    def graph_from_group(self, group_dict, group_ref):
        if group_dict['type'] != 'data-service':
            return

        self._add_list_triples_from_dict(group_dict, group_ref, self.DATASERVICE_FIELDS)
        self._add_triples_from_dict(group_dict, group_ref, [
            (DCT.type, 'dcat_type', SKOS.Concept, URIRefOrLiteral)
        ])


        for rule in group_dict.get('follows_rules', []):
            ref = URIRef(rule['identifier'])
            self.g.add((group_ref, CPSV.follows, ref))
            self.g.add((ref, RDF.type, CPSV.Rule))
            self.g.add((ref, DCT.identifier, Literal(str(ref))))
            self.g.add((ref, DCT.title, Literal(rule['title'])))
            self.g.add((ref, DCT.description, Literal(rule['description'])))

        for contact_point in group_dict.get('contact_point', []):
            page, email = contact_point['contact_page'], contact_point['contact_email']

            ref = CleanedURIRef(page) if page else BNode()

            self.g.add((group_ref, DCAT.contactPoint, ref))
            self.g.add((ref, RDF.type, VCARD.Kind))

            self.g.add((ref, VCARD.hasEmail, URIRef(self._add_mailto(email))))

        # publisher is only using repeating_subfields because there aren't non-repeating subfields in scheming
        (publisher, ) = group_dict.get('publisher', [None])
        if publisher:
            ref = BNode()
            self.g.add((group_ref, DCT.publisher, ref))
            self.g.add((ref, RDF.type, FOAF.Agent))

            self._add_triple_from_dict(publisher, ref, FOAF.name, 'names', list_value=True)
            self._add_triple_from_dict(publisher, ref, DCT.type, 'type', _class=SKOS.Concept)
            self._add_triple_from_dict(publisher, ref, DCT.identifier, 'identifier')


    def graph_from_dataset(self, dataset_dict, dataset_ref):
        self._add_list_triples_from_dict(dataset_dict, dataset_ref, self.DATASET_FIELDS)

        for rule in dataset_dict.get('follows_rules', []):
            ref = URIRef(rule['identifier'])
            self.g.add((dataset_ref, CPSV.follows, ref))
            self.g.add((ref, RDF.type, CPSV.Rule))
            self.g.add((ref, DCT.identifier, Literal(str(ref))))
            self.g.add((ref, DCT.title, Literal(rule['title'])))
            self.g.add((ref, DCT.description, Literal(rule['description'])))

