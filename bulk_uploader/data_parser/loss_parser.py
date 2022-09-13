from collections import namedtuple


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
        required_columns = set(
            self.get_column_names(self.bulk_loss_set_columns)
        )
        provided_columns = set(self.loss_df.columns)
    
        # Check whether all required columns are included.
        if not required_columns.issubset(provided_columns):
            raise ValueError(
                "Not all required required columns are found "
                "in the input loss data."
            )
        
        # Create a new view of the DataFrame only containing the required
        # columns.
        self.loss_df = self.loss_df[list(required_columns)]

    def check_loss_content(self):
        """
        Validate the content of Loss DataFrame.
        """
        if not len(self.loss_df):
            raise ValueError("Input loss file contains no losses.")

        if self.loss_df.isnull().values.any():
            raise ValueError(
                "Input loss file has empty values for some columns."
            )

    def modify_column_names(self):
        """
        Modify the Loss DataFrame columm names to align with Analyze Re 
        Loss columns expectations
        """

        # These are the column names required by Analyze Re
        are_target_columns = namedtuple(
            "TargetColumns", 
            [
                "loss_set_id", "event_id", "loss", "trial_id", "sequence",
                "reinstatement_premium", "reinstatement_brokerage"
            ]
        )._make(
            [
                "LayerId", 
                "EventId", 
                "Loss", 
                "Trial", 
                "Sequence", 
                "ReinstatementPremium", 
                "ReinstatementBrokerage"
            ]
        )

        # We'll create a mapping from the source column name to the
        # corresponding Analyze Re column name. get_column_names enforces a
        # specific order of the column names, so zip() works here.
        column_mapper=dict(
            (src, dst)
            for src, dst in zip(
                self.get_column_names(self.bulk_loss_set_columns),
                self.get_column_names(are_target_columns)
            )
        )

        # Replace the column names in the loss dataframe
        self.loss_df = self.loss_df.rename(columns=column_mapper)

    def parse_loss_df(self):
        # Get the input loss column names from config
        self.bulk_loss_set_columns = self.config.loss_set_columns

        self.check_required_columns()
        self.check_loss_content()
        self.modify_column_names()

        return self.loss_df

    def __init__(self, loss_df, loss_type, config):
        self.loss_df = loss_df
        self.loss_type = loss_type.lower()
        self.config = config
