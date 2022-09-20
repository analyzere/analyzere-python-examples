from collections import namedtuple

# These are the column names required by Analyze Re
ARE_TARGET_COLUMNS = namedtuple(
    "TargetColumns", 
    [
        "loss_set_id", "event_id", "loss", "trial_id", "day",
        "reinstatement_premium", "reinstatement_brokerage"
    ]
)._make(
    [
        "LayerId", 
        "EventId", 
        "Loss", 
        "Trial", 
        "Day", 
        "ReinstatementPremium", 
        "ReinstatementBrokerage"
    ]
)

REQUIRED_COLUMNS = dict(
    elt=["event_id", "loss"],
    yelt=["trial_id", "day", "event_id", "loss"],
    ylt=["trial_id", "loss"],
)

OPTIONAL_COLUMNS = dict(
    elt=[],
    yelt=["reinstatement_premium", "reinstatement_brokerage"],
    ylt=["reinstatement_premium", "reinstatement_brokerage"]
)

SORT_COLUMNS = dict(
    elt=["event_id"],
    yelt=["trial_id", "day", "event_id"],
    ylt=["trial_id"]
)

class LossSetExtractor:
    """
    Responsible for parsing and validating the content of the Loss DataFrame
    and modifying column names to adhere with Analyze Re column naming conventions.

    """

    def get_column_names(self, config, column_set=REQUIRED_COLUMNS, ifin=None):
        """
        Return column names specific to
        Loss type (ELT, YELT, YLT)
        """
        return [
            getattr(config, column) 
            for column in column_set[self.loss_type] 
            if ifin is None or getattr(config, column) in ifin.columns
        ]
    
    def check_required_columns(self):
        """
        Check if the required columns are present in the Loss DataFrame.
        """
        required_columns = set(
            self.get_column_names(self.loss_set_columns)
        )
        provided_columns = set(self.loss_df.columns)
    
        # Check whether all required columns are included.
        if not required_columns.issubset(provided_columns):
            raise ValueError(
                "Not all required required columns are found "
                "in the input loss data."
            )
                
    
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

    
    def rename_columns(self):
        """
        Modify the Loss DataFrame columm names to align with Analyze Re 
        Loss columns expectations
        """
        # We'll create a mapping from the source column name to the
        # corresponding Analyze Re column name.
        column_mapper=dict(
            (
                getattr(self.loss_set_columns, column),
                getattr(ARE_TARGET_COLUMNS, column)
            )
            for column in ARE_TARGET_COLUMNS._fields
        )

        # Replace the column names in the loss dataframe
        self.loss_df = self.loss_df.rename(columns=column_mapper)
    
    def _transform_loss_sets(self):
        self.check_required_columns()
        self.check_loss_content()
        self.rename_columns()


    def get_loss_set(self, loss_set_id):
        """
        Returns the subset of the loss data that is associated with a
        specific loss_set_id.
        """
        # Constrain all losses to only the loss set in question
        loss_set = self.loss_df[
            self.loss_df[ARE_TARGET_COLUMNS.loss_set_id] == loss_set_id
        ]
        # Determine required columns
        required_columns = self.get_column_names(ARE_TARGET_COLUMNS)       
        # Determine optional columns available in the loss set
        optional_columns = self.get_column_names(
            ARE_TARGET_COLUMNS, 
            OPTIONAL_COLUMNS,
            loss_set
        )

        # Return constrained loss set with only the relevant supported
        # columns and sorted according the recommended sort strategy.
        return loss_set[required_columns + optional_columns].sort_values(
            by=self.get_column_names(ARE_TARGET_COLUMNS, SORT_COLUMNS)
        )


    def __init__(self, loss_df, loss_type, config):
        self.loss_df = loss_df
        self.loss_type = loss_type.lower()
        self.config = config
        self.loss_set_columns = self.config.loss_set_columns
        self._transform_loss_sets()
