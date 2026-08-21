
from .extract import extract
from pathlib import Path

high_value_dataset_category = extract(Path(__file__).parent / 'high-value-dataset-category.rdf')
measurement_units = extract(Path(__file__).parent / 'measurement-units.rdf')
distribution_types = extract(Path(__file__).parent / 'distribution-types.rdf')
data_theme = extract(Path(__file__).parent / 'data-theme-skos.rdf')


__all__ = ['high_value_dataset_category', 'measurement_units', 'distribution_types', 'data_theme']
