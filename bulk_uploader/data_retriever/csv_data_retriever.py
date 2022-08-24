import pandas as pd
import sys


class CSVDataRetriever:
    """
    Retrieves the bulk Layer data and Loss data from CSV 
    and loads it in a DataFrame.
    
    """
    def get_bulk_loss_df(self):
        try:
            self.loss_df = pd.read_csv(self.bulk_loss_file)

            return self.loss_df
        except Exception as e:
            sys.exit('Error occurred while reading Bulk Loss CSV: {}'.format(e))

    def get_bulk_layer_df(self):
        try:
            self.layer_df = pd.read_csv(self.bulk_layer_file)

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

    def __init__(self, bulk_layer_file, bulk_loss_file):
        self.bulk_layer_file = bulk_layer_file
        self.bulk_loss_file = bulk_loss_file
