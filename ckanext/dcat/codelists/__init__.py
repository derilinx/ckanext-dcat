from pathlib import Path

from .extract import extract_codelist

_CODELIST_FILES = {
    "high_value_dataset_category": "high-value-dataset-category.rdf",
    "measurement_units": "measurement-units.rdf",
    "distribution_types": "distribution-types.rdf",
    "data_theme": "data-theme-skos.rdf",
}

_codelists = {}


def init_codelists(force=False):
    """Parse the RDF codelists and expose them as module attributes
    (e.g. `codelists.data_theme`).
    """
    if _codelists and not force:
        return _codelists

    if force:
        extract_codelist.cache_clear()

    base = Path(__file__).parent
    for name, filename in _CODELIST_FILES.items():
        _codelists[name] = extract_codelist(name, base / filename)

    return _codelists


def __getattr__(name):
    # PEP 562: only called for names not found as real module attributes
    if name in _CODELIST_FILES:
        if not _codelists:
            init_codelists()
        return _codelists[name]
    raise AttributeError(f"Unknown codelist: {name}")


def __dir__():
    return sorted(set(globals()) | set(_CODELIST_FILES))


__all__ = list(_CODELIST_FILES) + ["init_codelists"]
