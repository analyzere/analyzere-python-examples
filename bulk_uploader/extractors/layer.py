from datetime import timezone
from re import A
import dateutil
from enum import Enum, auto
from collections import Counter
from math import isnan

import logging
LOG = logging.getLogger()


REINSTATEMENT_SEPARATORS = [
    '~', '!', '@', '#', '$', '%', '^', '&', '*', '_', '+', 
    '=', '|', ';', ':', '/'
]

def _get_typed_value(v, T):
    if v is None:
        return None
    elif type(v) is T:
        if isnan(v):
            return None
        else:
            return v

    clean = str(v).strip().replace(',', '')
    percentage = '%' in clean
    clean = clean.strip('%').lower()
    
    if not clean:
        return T()
    else:
        return T(T(clean) / T(100)) if percentage else T(clean)


def _get_float_value(v):
    return _get_typed_value(v, float)


def _get_int_value(v):
    return _get_typed_value(v, int)


def _get_bool_value(v):
    if v is None or type(v) is bool:
        return v
    
    return str(v).strip().lower() == "true"

def get_date(row, field):
    if field in row:
        value = getattr(row, field).item()
        if value:
            try:
                date = dateutil.parser.parse(value, ignoretz=True)
                return date.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError) as e:
                raise ValueError('Unrecognized {} date: {}'.format(field, value))
        else:
            return None
    else:
        return None


def get_float(row, field):
    return _get_float_value(getattr(row, field).item())
    

def get_int(row, field):
    return _get_int_value(getattr(row, field).item())


def get_bool(row, field):
    return _get_bool_value(getattr(row, field).item())

def get_str(row, field):
    if field in row:
        v = getattr(row, field).item()
        if v is None or type(v) is str:
            return v
        return None
    else:
        return None


def get_money(row, field, field_ccy=None):
    if field in row:
        value = get_float(row, field)
        if field_ccy and field_ccy in row:
            ccy = get_str(row, field_ccy)
        else:
            ccy = None
        return (value, ccy)
    else:
        return (None, None)


class ReinstatementStyle(Enum):
    NONE = auto()
    ONE = auto()
    TWO = auto()

class LayerExtractor:
    def _get_reinstatement_style(self):
        """
        Returns the configured reinstatements as a list of tuples. There
        are two fundamental styles of specifying reinstatements:
        
        1. Column Reinstatements: double delimited string like
           '1.0;0.5|1.0;.05|0.5;0.25'
        2. Columns ReinstatementCount, ReinstatementPremium,
           ReinstatementBrokerage: same reinstatement terms are applied to
           each of the ReinstatementCount reinstatements.
        """

        style_1 = [self.layer_columns.reinstatements in self.layer_df]
        style_2 = [
            self.layer_columns.reinstatement_count in self.layer_df,
            self.layer_columns.reinstatement_premium in self.layer_df,
            self.layer_columns.reinstatement_brokerage in self.layer_df
        ]

        if any(style_1) and any(style_2):
            # Can't mix type 1 and type 2
            raise ValueError(
                "Ambiguous reinstatement definition columns found. "
                "Only use single column with double-delimited string "
                "or three columns denoting number of reinstatements, "
                "their premium and their brokerage."
            )
        elif not all(style_1) and any(style_1):
            # This should not be possible with only one column, but let's
            # report it anyway.
            raise ValueError(
                f"You must specify all columns required for the "
                f"specified style of reinstatements: {','.join(style_1)}"
            )
        elif not all(style_2) and any(style_2):
            # This can happen when the user does not specify all of the
            # required columns.
            raise ValueError(
                f"You must specify all columns required for the "
                f"specified style of reinstatements: {','.join(style_2)}"
            )
        elif all(style_1):
            # Process style 1 reinstatements
            return ReinstatementStyle.ONE
        elif all(style_2):
            # Process style 2 reinstatements
            return ReinstatementStyle.TWO
        elif not any(style_1) and not any(style_2):
            # No reinstatements defined
            return ReinstatementStyle.NONE
        else:
            # All else is incomplete
            raise ValueError(
                f"Incomplete definition of reinstatements. Please select "
                f"style [{','.join(style_1)}] or "
                f"style [{','.join(style_2)}]."
            )

    def _get_reinstatements_style_1(self, row):
        value = get_str(row, self.layer_columns.reinstatements)
        
        # If nothing is specified return an empty list of reinstatements
        if not value:
            return []

        value = value.strip()

        # Determine the count of each character in value string
        counts = Counter(value)
        filtered = dict(
            (separator, count) 
            for separator, count in counts.items()
            if separator in REINSTATEMENT_SEPARATORS
        )

        # We should only have at most 2 separators
        if len(filtered) > 2:
            raise ValueError(f"Malformed reinstatements value string: {value}")

        # If we only have one separator then only a single reinstatement is
        # specified and the separator is the premium-brokerage separator.
        # We can pick any other separator as the reinstatement separator.
        if len(filtered) == 1:
            # Get the separator
            pb_separator = list(filtered.keys()).pop()
            
            # Make sure the separator only occurs once
            if filtered[pb_separator] != 1:
                # There should only be one occurrence of the
                # premium-brokerage separator
                raise ValueError(f"Malformed reinstatements value string: {value}")
            
            # Pick any other separator as the reinstatement separator
            ri_separator = [
                sep for sep in REINSTATEMENT_SEPARATORS if sep != pb_separator
            ].pop()
        elif len(filtered) == 2:
            # Get both separators and their counts
            first, second = list(filtered.items())
            
            # Make sure both separators don't occur the same amount and
            # their frequency is just one occurrence apart.
            if first[1] == second[1] or abs(first[1]-second[1]) != 1:
                raise ValueError(f"Malformed reinstatements value string: {value}")

            # The premium-brokerage separator must be the more frequent
            # separator
            pb_separator = first[0] if (first[1]-1) == second[1] else second[0]
            #                              Note^^^
             
            # The reinstatement separator must be the less frequent
            # separator
            ri_separator = first[0] if (first[1]+1) == second[1] else second[0]
            #                              Note^^^
        else:
            raise ValueError(f"Malformed reinstatements value string: {value}")

        # Create the list of reinstatements, this will raise errors if
        # there are any malformed structures
        return [
            (
                float(premium),
                float(brokerage)
            )
            for premium, brokerage in [
                tuple(ri.split(pb_separator)) 
                for ri in value.split(ri_separator)
            ]
        ]

    
    def _get_reinstatements_style_2(self, row):
        count = get_int(row, self.layer_columns.reinstatement_count)
        premium = get_float(row, self.layer_columns.reinstatement_premium)
        brokerage = get_float(row, self.layer_columns.reinstatement_brokerage)

        return [ (premium, brokerage) ] * count

    def get_reinstatements(self, row):
        if self.reinstatement_style == ReinstatementStyle.NONE:
            # If no reinstatements are defined, simply return an empty list.
            return []
        elif self.reinstatement_style == ReinstatementStyle.ONE:
            return self._get_reinstatements_style_1(row)
        elif self.reinstatement_style == ReinstatementStyle.TWO:
            return self._get_reinstatements_style_2(row)
        else:
            return []


    def get_metadata(self, row):
        metadata_columns = list(set(self.layer_df.columns) - set(self.layer_columns))
        return {
            column: getattr(row, column).item()
            for column in metadata_columns
        }        


    def validate(self):
        if len(self.layer_df) == 0:
            raise ValueError("Layer input is empty")

        if not self.layer_columns.layer_id in self.layer_df.columns:
            raise ValueError(
                f"Required column {self.layer_columns.layer_id} not "
                f"found in layer input file."
            )
            

    def get_layers(self):
        layers = self.layer_df[self.layer_columns.layer_id]
        unique_layers = layers.unique()

        if len(layers) != len(unique_layers):
            raise ValueError(
                "Every layer may only occur once in layer data file."
            )

        return list(unique_layers)
    

    def get_layer_row(self, layer):
        return self.layer_df.loc[
            self.layer_df[self.layer_columns.layer_id] == layer
        ]
       

    def __init__(self, layer_df, config):
        self.layer_df = layer_df
        self.config = config
        self.layer_columns = self.config.layer_columns
        self.reinstatement_style = self._get_reinstatement_style()
        self.validate()

        
