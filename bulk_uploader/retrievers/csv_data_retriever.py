import pandas as pd


class CSVDataRetriever:
    """
    Retrieves layer definitions and loss data from CSV and loads them into 
    Pandas DataFrames for downstream processing.
    """
    @classmethod
    def add_parser_arguments(cls, parser):
        """
        Defines additional command-line arguments that are relevant for
        this data retriever.
        """
        parser.add_argument("--layers", required=True, metavar="LAYERS_CSV",
                        help="Bulk Layer Terms CSV")

        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--elt", help="Bulk ELT CSV")
        group.add_argument("--yelt", help="Bulk YELT CSV")
        group.add_argument("--ylt", help="Bulk YLT CSV")


    def get_losses(self):
        # Read the losses CSV file into DataFrame
        return pd.read_csv(self._losses_file)


    def get_layers(self):
        # Read the layers CSV file into DataFrame
        return pd.read_csv(self._layers_file)


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
