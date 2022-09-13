from datetime import timezone
import dateutil
import pandas as pd
import sys

from analyzere import MonetaryUnit, Reinstatement
from math import ceil


class LayerParser:
    def _parse_date(self, row, layer_term):
        parsed_date = None
        value = self._get_value(layer_term, row)
        if value:
            try:
                date_str = dateutil.parser.parse(value, ignoretz=True)
                parsed_date = date_str.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError) as e:
                raise Exception('Unrecognized {} date: {}'.format(layer_term, value))
        return parsed_date

    def str_to_number_or_bool(self, number_as_str, _as):
        """Convert a string to a numeric type.

            NOTE: There is special handling for percentage values:

                    str_to_number_or_bool('96.2%', 'float')  yields: 0.962
                    str_to_number_or_bool('12,345', 'float') yields: 12345.0
                    str_to_number_or_bool('12,345', 'int')   yields: 12345
                    str_to_number_or_bool('FALSE', 'bool')   yields: False
                    str_to_number_or_bool('True', 'bool')   yields: True
                    str_to_number_or_bool('true', 'bool')   yields: True

            Returns None if 'number_as_str' cannot be converted to '_as'
        """
        if number_as_str is None:
            return None

        if type(number_as_str) == _as:
            return number_as_str

        clean = str(number_as_str).strip().replace(',', '')

        percentage = '%' in clean
        clean = clean.strip('%')
        clean = clean.lower()

        try:
            if _as == 'float':
                if clean == '':
                    return 0.0
                else:
                    return (float(clean) if not percentage
                            else float(float(clean) / 100))
            elif _as == 'int':
                return (int(clean) if not percentage
                        else int(float(clean) / 100))
            elif _as == 'bool':
                return True if clean == 'true' else False
        except ValueError as e:
            print('Value error')
            return None

    def _get_value(self, are_field, row):
        """Get the value of the given ARe 'field' from the row."""
        return getattr(row, are_field)

    def _get_value_as(self, are_field, row, _as='str'):
        """Get the value of the given ARe 'field' from the row in a specific data type."""
        raw_value = self._get_value(are_field, row)

        # We have the raw value, now try to convert it to '_as'
        value = self.str_to_number_or_bool(raw_value, _as)

        if value is None:
            raise Exception('Invalid "{}" value "{}" specified'.format(are_field, raw_value))

        return value

    def _get_MonetaryUnit(self, field, row):
        """Get a MonetaryUnit for the given ARe 'field' from the row."""
        # First get the value (double) for the MonetaryUnit
        value = self._get_value_as(field, row, 'float')
        if value is None:
            return None

        # If the field is a limit, and the value in the row is zero, we assume
        # "unlimited"
        if self.layer_columns.limit in field and str(value).lower() in ['unlimited']:
            value = sys.float_info.max
        else:
            value = self._get_value_as(field, row, 'float')

        if value is None:
            return None

        # Get the 'currency' for the MonetaryUnit.
        ccy = None
        # Check if currency exist in the dataframe
        if self.layer_columns.currency in self.layer_df.columns:
            ccy = self._get_value(self.layer_columns.currency, row)
        else:
            ccy_field = '{}_ccy'.format(field)
            if ccy_field in self.layer_df.columns:
                ccy = self._get_value(ccy_field, row)
            else:
                ccy = self.defaults.default_currency

        return MonetaryUnit(value, ccy)

    def _get_participation(self, row):
        participation = self._get_value_as(self.layer_columns.participation,
                                           row,
                                           'float')

        return participation

    def _parse_reinstatement_string(self, reinstatement_str):
        """Parse Reinstaement records from a (double) delimited string.

        Example:

        reinstatement_str: '1.0;0.5|1.0;.05|0.5;0.25'

        Returns:  [Reinstatement(premium=1.0, brokerage=0.5),
                   Reinstatement(premium=1.0, brokerage=0.5),
                   Reinstatement(premium=0.5, brokerage=0.25)]

        The ';' separator splits the Premium and Brokerage values, and the '|'
        character splits the records.

        NOTE: The 'reinstatement_str' string *MUST* have at least 1 pair of
              Reinstatement Premim + Reinstatement Brokerage
        """

        reinstatements = []

        if reinstatement_str is None:
            return reinstatements

        # If the 'reinstatement_str' is a single integer it is an ambiguous
        # reinsatement definition, we reject it.
        as_float = None
        try:
            as_float = float(reinstatement_str)
        except ValueError:
            pass

        if as_float is not None:
            raise Exception(
                'Reinstatement string "{}" is ambiguous; '
                'Reinstatements must be a delimited pair of float '
                'values'.format(
                    reinstatement_str))

        # If the reinstatement_str is an empty string, we return an empty list
        # of Reinstatements
        if ''.join(reinstatement_str.split()) == '':
            return reinstatements

        # The first non-numeric character will be considered the
        # premium/brokerage (value) separator
        value_separator = None
        for c in reinstatement_str:
            if c.isdigit() or c == '.':
                continue
            if c in self._reinstatement_str_separators:
                value_separator = c
                break

        if value_separator is None:
            self.errors[self._line_number].append(
                'Reinstatement string "{}" did not contain a value '
                'separator'.format(reinstatement_str))

            return reinstatements

        # The next (different) non-numeric character will be the record
        # separator
        record_separator = None
        for c in reinstatement_str:
            if c.isdigit() or c in ['.', value_separator]:
                continue
            if c in self._reinstatement_str_separators:
                record_separator = c
                break

        # Split the string on the record_separator
        for record in reinstatement_str.split(record_separator):
            pair = record.split(value_separator)
            if len(pair) < 2:
                raise Exception(
                    'Reinstatement string "{}" missing value '
                    'separator'.format(record))

            elif len(pair) > 2:
                raise Exception(
                    'Reinstatement string "{}" missing record '
                    'separator'.format(record))

            premium_str, brokerage_str = record.split(value_separator)
            valid = True
            try:
                premium = float(premium_str)
                if premium < 0.0:
                    raise Exception('Negative Reinstatement Premium provided: {}' \
                                    .format(premium))

            except ValueError:
                raise Exception(
                    'Invalid Reinstatement Premium value "{}" in record "{}"' \
                        .format(premium_str, record))

            try:
                brokerage = float(brokerage_str)
                if brokerage < 0.0:
                    raise Exception('Negative Reinstatement Brokerage provided: {}'.
                                    format(brokerage))

            except ValueError:
                raise Exception(
                    'Invalid Reinstatement Brokerage value "{}" in record "{}"' \
                        .format(brokerage_str, record))

            reinstatements.append(
                Reinstatement(premium=float(premium),
                              brokerage=float(brokerage)))

        return reinstatements

    def _parse_reinstatements(self, row, agg_limit, occ_limit):
        """Parse reinstatement values from a row.

        There are two supported methods for specifying Reinstatements.
        Each of the methods described demonstrate how 3 resinstatments each
        with 100% Premium and 50% Brokerage would be defined.


        Option 1: Count + Premium [+ Brokerage]
        --------
            Reinstatement Count,Reinst Premium,Reinst Brokerage,
            3,1.0,0.5


        Option 2: Values in double-delimited string
        --------
            Reinstatements
            1.0;0.5|1.0;0.5|1.0;0.5

        The ';' separator splits the Premium and Brokerage values, and the '|'
        character splits the records.
        """

        computed_reinstatements = 0
        provided_reinstatements = []

        if (agg_limit is not None and occ_limit is not None and
                agg_limit not in [0, sys.float_info.max] and
                occ_limit not in [0, sys.float_info.max]):
            computed_reinstatements = ceil((agg_limit
                                            / occ_limit) - 1)
            print('Computed reinstatements: {}'.format(computed_reinstatements))

        # Option 1: Count + Premium [+ Brokerage]
        # If 'Reinstatement Count' is proivded, it *must* be accompanied by
        # at least a 'Reinstatement Premium' column
        if self.layer_columns.reinstatement_count in self.layer_df.columns:
            if self.layer_columns.reinstatement_premium not in self.layer_df.columns:
                raise Exception(
                    'A "Reinstatement Premium" column is required '
                    'when a "Reinstatement Count" field is provided')

            # Option 1: Count + Premium [+ Brokerage]
            # Ensure all fields are float values (not strings)
            reinstatement_count = self._get_value_as(self.layer_columns.reinstatement_count,
                                                     row,
                                                     'int')

            reinstatement_premium = self._get_value_as(self.layer_columns.reinstatement_premium,
                                                       row,
                                                       'float')
            if reinstatement_premium < 0.0:
                raise Exception('Negative Reinstatement Premium provided: '
                                '{}'.format(reinstatement_premium))

            if self.layer_columns.reinstatement_brokerage in self.layer_df.columns:
                reinstatement_brokerage = self._get_value_as(self.layer_columns.reinstatement_brokerage,
                                                             row,
                                                             'float')
                if reinstatement_brokerage < 0.0:
                    raise Exception('Negative Reinstatement Brokerage provided: '
                                    '{}'.format(reinstatement_premium))

            # each of the reinstatements have the same values
            for i in range(reinstatement_count):
                provided_reinstatements.append(
                    Reinstatement(premium=reinstatement_premium,
                                  brokerage=reinstatement_brokerage))

        # Option 2: Values in double-delimited string
        elif self.layer_columns.reinstatements in self.layer_df.columns:
            provided_reinstatements = self._parse_reinstatement_string(
                self._get_value(self.layer_columns.reinstatements, row))

        # Return the list of Reinstatements
        return provided_reinstatements

    def update_layer_record(self, row, layer_term, new_value, index):
        self.layer_df.at[index, layer_term] = new_value

    def process_layer_term(self, layer_term, row, index):
        parsed_value = None
        if layer_term in self.layer_df.columns:
            # Parse participation
            if layer_term == self.layer_columns.participation:
                parsed_value = self._get_participation(row)

            # Parse inception and expiry
            elif (layer_term == self.layer_columns.inception) or \
                    (layer_term == self.layer_columns.expiry):
                parsed_value = self._parse_date(row, layer_term)

            # Parse reinstatements
            elif (layer_term == self.layer_columns.reinstatements) or \
                    (layer_term == self.layer_columns.reinstatement_count):
                agg_limit = self._get_value_as(self.layer_columns.aggregate_limit, row, 'float')
                occ_limit = self._get_value_as(self.layer_columns.limit, row, 'float')

                parsed_value = self._parse_reinstatements(row, agg_limit, occ_limit)

            # For all other financial terms
            else:
                parsed_value = self._get_MonetaryUnit(layer_term, row)
            self.update_layer_record(row, layer_term, parsed_value, index)

    def process_loss_metadata(self, row, index):
        if self.layer_columns.loss_set_start_date in self.layer_df.columns:
            parsed_value = self._parse_date(row, self.layer_columns.loss_set_start_date)
            self.update_layer_record(row, self.layer_columns.loss_set_start_date, parsed_value, index)

    def process_layer_metadata(self, row, index):
        # Pickup columns that are neither financial layer terms, nor non-financial layer terms
        non_metadata_columns = self.non_financial_layer_terms + self.financial_layer_terms
        metadata_columns = list(set(list(self.layer_df.columns)) - set(non_metadata_columns))

        meta_data = {}
        for col in metadata_columns:
            value = getattr(row, col)
            if pd.isna(value):
                value = ''
            meta_data[col] = value
        self.update_layer_record(row, self.layer_columns.meta_data, meta_data, index)

    def process_layer_record(self):
        self.non_financial_layer_terms = [
            self.layer_columns.layer_id,
            self.layer_columns.loss_set_id,
            self.layer_columns.loss_set_currency,
            self.layer_columns.loss_set_start_date,
            self.layer_columns.layer_type,
            self.layer_columns.description,
            self.layer_columns.meta_data,
            self.layer_columns.currency
        ]

        self.financial_layer_terms = list(filter(None, list(set(self.layer_columns._asdict().values())
                                          - set(self.non_financial_layer_terms))))

        self.layer_df['meta_data'] = self.layer_df.apply(lambda x: {})
        for index, row in self.layer_df.iterrows():
            # Process financial layer terms recognized by ARe.
            for financial_term in self.financial_layer_terms:
                self.process_layer_term(financial_term, row, index)

            # Process Loss Metadata - Start Date and Currency
            self.process_loss_metadata(row, index)

            # Process Layer Metadata
            self.process_layer_metadata(row, index)

    def validate_layer_file(self):
        assert (len(self.layer_df) > 0), 'Bulk Layer input has no content'
        if not self.layer_columns.layer_id in self.layer_df.columns:
            raise Exception('{} not found in Bulk Layer input'.format
                            (self.layer_columns.layer_id))

    def parse_layer_df(self):
        self.layer_columns = self.config.layer_columns
        self.defaults = self.config.defaults

        self.validate_layer_file()
        self.process_layer_record()

        return self.layer_df

        
    def __init__(self, layer_df, config):
        self.layer_df = layer_df
        self.config = config
        self._reinstatement_str_separators = ['~', '!', '@', '#', '$', '%',
                                              '^', '&', '*', '_', '+',
                                              '=', '|', ';', ':', '/']
