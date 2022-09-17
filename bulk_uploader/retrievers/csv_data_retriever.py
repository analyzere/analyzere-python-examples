import pandas as pd
import sys


class CSVDataRetriever:
    """
    Retrieves the bulk Layer data and Loss data from CSV 
    and loads it in a DataFrame.
    
    """
    @classmethod
    def add_parser_arguments(cls, parser):
        parser.add_argument("--layers", required=True, metavar="LAYERS_CSV",
                        help="Bulk Layer Terms CSV")

        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--elt", help="Bulk ELT CSV")
        group.add_argument("--yelt", help="Bulk YELT CSV")
        group.add_argument("--ylt", help="Bulk YLT CSV")


    def get_bulk_loss_df(self):
        try:
            self.loss_df = pd.read_csv(self._losses_file)

            return self.loss_df
        except Exception as e:
            sys.exit('Error occurred while reading Bulk Loss CSV: {}'.format(e))

    def get_bulk_layer_df(self):
        try:
            self.layer_df = pd.read_csv(self._layers_file)

            return self.layer_df
        except Exception as e:
            sys.exit('Error occurred while reading Bulk Layer Terms CSV: {}'.format(e))

    def get_bulk_data(self):
        try:
            layer_df = self.get_bulk_layer_df()
            loss_df = self.get_bulk_loss_df()

            return layer_df, loss_df
        except Exception as e:
            sys.exit('Error while retrieving data from CSV source: {}'.format(e))

    
    @property
    def loss_type(self):
        return self._loss_type


    def __init__(self, args, config):
        loss_type, loss_file = (
            args.elt and ("elt", args.elt) 
            or args.yelt and ("yelt", args.yelt)
            or args.ylt and ("ylt", args.ylt)
        )
        
        self._layers_file = args.layers
        self._losses_file = loss_file
        self._loss_type = loss_type
