from .euro_dcat_ap_2 import EuropeanDCATAP2Profile

LEGISLATION_HVD_URI = "http://data.europa.eu/eli/reg_impl/2023/138/oj"


class EuropeanDCATAPHVD220Profile(EuropeanDCATAP2Profile):
    """Read only catalog output for the HVD Profile, that only includes HVD resources"""

    def graph_from_dataset(self, dataset_dict, dataset_ref):

        if any(
            LEGISLATION_HVD_URI in r.get("applicable_legislation", [])
            for r in dataset_dict["resources"]
        ) or (LEGISLATION_HVD_URI in dataset_dict.get("applicable_legislation", [])):
            return super().graph_from_dataset(dataset_dict, dataset_ref)

    def graph_from_resource(
        self,
        dataset_dict,
        dataset_ref,
        resource_dict,
        distribution_ref=None,
        resource_license_fallback=None,
    ):

        if LEGISLATION_HVD_URI in resource_dict.get("applicable_legislation", []):
            return super().graph_from_resource(
                dataset_dict,
                dataset_ref,
                resource_dict,
                distribution_ref,
                resource_license_fallback,
            )
