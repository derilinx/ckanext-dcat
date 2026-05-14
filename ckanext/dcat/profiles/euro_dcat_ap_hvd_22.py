from .euro_dcat_ap_2 import EuropeanDCATAP2Profile


class EuropeanDCATAPHVD220Profile(EuropeanDCATAP2Profile):
    """Read only catalog output for the HVD Profile, that only includes HVD resources"""

    def graph_from_resource(
        self,
        dataset_dict,
        dataset_ref,
        resource_dict,
        distribution_ref=None,
        resource_license_fallback=None,
    ):
        legislation_value = "http://data.europa.eu/eli/reg_impl/2023/138/oj"
        if legislation_value in self._read_list_value(
            self._get_dataset_value(resource_dict, "applicable_legislation")
        ):
            return super().graph_from_resource(
                dataset_dict,
                dataset_ref,
                resource_dict,
                distribution_ref,
                resource_license_fallback,
            )
