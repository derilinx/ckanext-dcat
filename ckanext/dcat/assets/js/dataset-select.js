/*
 * select2 widget for the `dcat_dataset_select` scheming preset.
 *
 * source      - URL of the dataset search endpoint
 * fieldName   - name of the field, used for the hidden inputs submitted
 * datasetType - comma-separated list of dataset types that can be selected
 * initial     - list of selected datasets, as returned by the endpoint
 * interval    - delay between requests in milliseconds (default: 300)
 *
 * Selected datasets are submitted as one hidden input per dataset id, so the
 * field gets a list of ids like the `multiple_text` preset.
 */
this.ckan.module('dcat-dataset-select', function ($) {
  return {
    options: {
      source: null,
      fieldName: null,
      datasetType: 'dataset',
      initial: [],
      interval: 300
    },

    initialize: function () {
      $.proxyAll(this, /_on/, /format/);

      this.input = this.$('.dcat-dataset-select-input');
      this.values = this.$('.dcat-dataset-select-values');

      this.input.select2({
        multiple: true,
        width: '100%',
        minimumInputLength: 1,
        ajax: {
          url: this.options.source,
          dataType: 'json',
          quietMillis: this.options.interval,
          data: this._onRequestData,
          results: function (data) { return data; }
        },
        formatResult: this.formatResult,
        formatSelection: this.formatSelection,
        formatNoMatches: this.formatNoMatches,
        formatSearching: this.formatSearching,
        formatLoadMore: this.formatLoadMore,
        formatAjaxError: this.formatAjaxError,
        formatInputTooShort: this.formatInputTooShort
      });
      this.input.select2('data', this.options.initial);
      this.input.on('change', this._onChange);
    },

    _onRequestData: function (term, page) {
      return {
        q: term,
        page: page,
        dataset_type: this.options.datasetType,
        selected: this.input.select2('val').join(',')
      };
    },

    /* Keeps the hidden inputs submitted with the form in sync */
    _onChange: function () {
      var name = this.options.fieldName;
      var values = this.values.empty();
      $.each(this.input.select2('val'), function (i, id) {
        values.append($('<input type="hidden">').attr('name', name).val(id));
      });
    },

    formatResult: function (dataset, container, query, escapeMarkup) {
      var out = escapeMarkup(dataset.text);
      if (dataset.organization) {
        out += ' <small class="text-muted">' + escapeMarkup(dataset.organization) + '</small>';
      }
      return out;
    },

    formatSelection: function (dataset, container, escapeMarkup) {
      if (!dataset.url) {
        return '<span class="text-muted">' + escapeMarkup(dataset.text) + '</span>';
      }
      return $('<a target="_blank" rel="noopener"></a>')
        .attr('href', dataset.url)
        .text(dataset.text)
        .prop('outerHTML');
    },

    formatNoMatches: function () {
      return this._('No datasets found');
    },

    formatSearching: function () {
      return this._('Searching...');
    },

    formatLoadMore: function () {
      return this._('Loading more results...');
    },

    formatAjaxError: function () {
      return this._('There was an error while searching');
    },

    formatInputTooShort: function () {
      return this._('Start typing to search datasets');
    }
  };
});
