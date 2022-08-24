import sys
from collections import Counter


class LossParser:
    """
    Responsible for parsing and validating the content of the Loss DataFrame
    and modifying column names to adhere with Analyze Re column naming conventions.

    """

    def get_column_names(self, config):
        """
        Return column names specific to
        Loss type (ELT, YELT, YLT)
        """
        column_names = [config.loss_set_id,
                        config.loss]
        if self.loss_type == 'elt':
            column_names.append(config.event_id)

        elif self.loss_type == 'yelt':
            column_names.append(config.event_id)
            column_names.append(config.trial_id)
            column_names.append(config.sequence)

        elif self.loss_type == 'ylt':
            column_names.append(config.trial_id)

        return column_names

    def check_required_columns(self):
        """
        Check if the required columns are present in the Loss DataFrame.
        """
        column_names = self.get_column_names(self.bulk_loss_set_columns)
    
        assert (set(column_names).issubset(set(list(self.loss_df.columns)))), \
            "Required columns are not found in the Bulk Loss input: {}".format(column_names)

        self.loss_df = self.loss_df[column_names]

    def check_loss_content(self):
        """
        Validate the content of Loss DataFrame.
        """
        assert(len(self.loss_df) > 0), 'Bulk Loss input has no content'

        if self.loss_df.isnull().values.any():
            raise Exception('Bulk Loss input has empty values for some columns')

    def modify_column_names(self):
        """
        Modify the Loss DataFrame columm names to align with Analyze Re 
        Loss columns expectations
        """
        are_loss_columns = self.get_column_names(self.ARe_loss_columns)
        input_loss_column_names = self.get_column_names(self.bulk_loss_set_columns)

        column_mapper = {}  # {input_column: ARe_column}
        for input_column, are_column in zip(input_loss_column_names, are_loss_columns):
            column_mapper[input_column] = are_column

        # Replace the column names in the loss dataframe
        self.loss_df = self.loss_df.rename(columns=column_mapper)

    def process_loss_df(self):
        try:
            print('Parsing bulk loss content')
            self.check_required_columns()
            self.check_loss_content()
            self.modify_column_names()

        except Exception as e:
            sys.exit('Error while parsing bulk loss content: {}'.format(e))

    def parse_loss_df(self):
        # Get the input loss column names from config
        self.bulk_loss_set_columns = self.config_parser.get_loss_set_columns()

        # Get the ARe expected column names from config
        self.ARe_loss_columns = self.config_parser.get_ARe_loss_set_columns()

        self.process_loss_df()

        return self.loss_df

    def __init__(self, loss_df, loss_type, config_parser):
        self.loss_df = loss_df
        self.loss_type = loss_type.lower()
        self.config_parser = config_parser
