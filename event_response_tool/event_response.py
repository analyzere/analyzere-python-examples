import os
import logging
import logging.config
import shutil
import configparser
import argparse
import pytz
from datetime import datetime
from types import SimpleNamespace

import analyzere

from utils.alert import Alert as alert
from utils.file_handler import read_input_file, file_exists
from layer_loss_duplicator.duplicate_layer_loss import LayerLossDuplicator
from ap_creator.create_ap import AnalysisProfileCreator


# Load config file
config_file = os.path.abspath("config/event_response_config.ini")
if not os.path.exists(config_file):
    alert.error("Config file not present. Cannot continue.")
config = configparser.ConfigParser()
config.read(config_file)


class EventResponseHandler:
    def __init__(self, output_dir, event_response_inputs):
        self.output_dir = output_dir
        self.event_response_inputs = event_response_inputs

    def execute(self):
        if self.event_response_inputs.event_weights_csv:
            self.event_weights_df = read_input_file(
                self.event_response_inputs.event_weights_csv
            )
        # METHOD 1 - MIXTURE DISTRIBUTION METHOD
        if self.event_response_inputs.mixture_distribution_method:
            alert.info("Using Mixture Distribution method")
            ap_creator = AnalysisProfileCreator(
                config=config,
                event_weights_df=self.event_weights_df,
                simulation_description=self.event_response_inputs.mixture_distribution_simulation_description,
                old_analysis_profile_uuid=self.event_response_inputs.old_analysis_profile_uuid,
                analysis_profile_description=self.event_response_inputs.mixture_distribution_analysis_profile_description,
                trial_count=self.event_response_inputs.max_trial_per_event,
                simulation_start_date=self.event_response_inputs.mixture_distribution_simulation_start_date,
            )
            ap_creator.update_analysis_profile()

        else:
            # METHOD 2 - CONVOLUTION METHOD
            alert.info("Using Convolution method")
            analysis_profile_uuid = None

            if self.event_response_inputs.create_analysis_profile:
                ap_creator = AnalysisProfileCreator(
                    config=config,
                    event_weights_df=None,
                    catalog_description=self.event_response_inputs.convolution_catalog_description,
                    simulation_description=self.event_response_inputs.convolution_simulation_description,
                    analysis_profile_description=self.event_response_inputs.convolution_analysis_profile_description,
                    trial_count=self.event_response_inputs.trial_count,
                    total_num_of_events=self.event_response_inputs.total_number_of_events,
                    simulation_start_date=self.event_response_inputs.convolution_simulation_start_date,
                )
                analysis_profile_uuid = ap_creator.create_analysis_profile()

            if (
                self.event_response_inputs.layer_views_csv
                or self.event_response_inputs.portfolio_view_uuid
            ):
                if not analysis_profile_uuid:
                    analysis_profile_uuid = (
                        self.event_response_inputs.analysis_profile_uuid_for_loss_update
                    )

                modify_layer_loss = LayerLossDuplicator(
                    config=config,
                    output_dir=self.output_dir,
                    event_weights_df=self.event_weights_df,
                    analysis_profile_uuid=analysis_profile_uuid,
                    layer_ids_csv=self.event_response_inputs.layer_views_csv,
                    portfolio_uuid=self.event_response_inputs.portfolio_view_uuid,
                )
                modify_layer_loss.modify_layer_loss_data()


# Set up logging
def init_logging():
    logging.config.fileConfig("logging.ini")
    logger = logging.getLogger(__name__)


def init_directories():
    # Create unique output directory for every attempt (FORMAT: archive/event_response-[datetime])
    # to store the output files and log
    now = datetime.now()
    current_datetime_string = now.strftime("%d-%m-%Y-%H:%M:%S")
    target_dir = "{}-{}".format("event_response", current_datetime_string)
    output_directory = config.get("defaults", "output_directory")
    full_output_dir = "{}/{}".format(output_directory, target_dir)

    try:
        os.makedirs(full_output_dir, exist_ok=True)
    except OSError as e:
        alert.exception(
            f"Creation of {full_output_dir} directory failed: {e}"
        )
    else:
        if os.path.isfile("activity.log"):
            shutil.move("activity.log", f"{full_output_dir}/activity.log")
        return full_output_dir


def login(url, username, password):
    try:
        analyzere.base_url = url
        analyzere.username = username
        analyzere.password = password

        analyzere.EventCatalog.list(limit=0)
    except Exception as e:
        alert.exception(f"Unable to connect to {url}: {e}")
    else:
        alert.info(f"Log in successful for user {username}")


def validate_inputs(event_response_inputs):
    # Analyze Re server
    if not event_response_inputs.analyzere_url:
        alert.error("Please input AnalyzeRe URL")
    if not event_response_inputs.analyzere_username:
        alert.error("Please input AnalyzeRe username")
    if not event_response_inputs.analyzere_password:
        alert.error("Please input AnalyzeRe password")

    # METHOD 1 - MIXTURE DISTRIBUTION METHOD
    if event_response_inputs.mixture_distribution_method:
        # Event weights CSV - required for creating weighted simgrid
        if not event_response_inputs.event_weights_csv:
            alert.error(
                "Please input the path of the CSV containing event weights"
            )
        if not event_response_inputs.old_analysis_profile_uuid:
            alert.error(
                "Please provide the existing Analysis Profile UUID in 'old_analysis_profile_uuid' field for Mixture Distribution method"
            )
        try:
            _ = int(event_response_inputs.max_trial_per_event)
        except ValueError:
            alert.error(
                f"max_trial_per_event variable should be a valid integer: {event_response_inputs.max_trial_per_event}"
            )

    # METHOD 2 - CONVOLUTION METHOD
    if not event_response_inputs.mixture_distribution_method:
        # If new Analysis Profile is to be created, check if trial count and
        # and total number of events to be included in the catalog are provided
        if event_response_inputs.create_analysis_profile:
            if not(event_response_inputs.total_number_of_events 
                   and event_response_inputs.trial_count):
                alert.error(
                    "Please provide the total number of events and the trial count for creating a new Analysis Profile using the convolution method"
                )

        # If new Analysis Profile need not be created, check if
        # an existing Analysis Profile UUID is provided
        if not event_response_inputs.create_analysis_profile:
            if not event_response_inputs.analysis_profile_uuid_for_loss_update:
                alert.error(
                    "Please provide the existing Analysis Profile UUID for updating the LayerViews and LossSets or create a new Analysis Profile"
                )

            # If an existing Analysis Profile UUID is provided, check if layer_views_csv or portfolio_view_uuid is provided
            if event_response_inputs.analysis_profile_uuid_for_loss_update:
                if not (event_response_inputs.layer_views_csv or 
                        event_response_inputs.portfolio_view_uuid):
                    alert.error(
                        "Please input either layer_views_csv or portfolio_view_uuid for processing the LayerViews and LossSets"
                    )
            
            # Event weights CSV - required for updating LossSets
            if not event_response_inputs.event_weights_csv:
                alert.error(
                    "Please input the path of the CSV containing event weights"
                )

def check_and_set_optional_inputs(event_response_inputs):
    event_response_inputs.analyzere_url = (
        event_response_inputs.analyzere_url
        if event_response_inputs.analyzere_url
        else config.get("server", "base_url")
    )
    event_response_inputs.analyzere_username = (
        event_response_inputs.analyzere_username
        if event_response_inputs.analyzere_username
        else config.get("server", "username")
    )
    event_response_inputs.analyzere_password = (
        event_response_inputs.analyzere_password
        if event_response_inputs.analyzere_password
        else config.get("server", "password")
    )
    event_response_inputs.convolution_catalog_description = (
        event_response_inputs.convolution_catalog_description
        if event_response_inputs.convolution_catalog_description
        else config.get("ap_creator", "default_catalog_name")
    )
    event_response_inputs.convolution_simulation_description = (
        event_response_inputs.convolution_simulation_description
        if event_response_inputs.convolution_simulation_description
        else config.get("ap_creator", "default_simulation_name")
    )
    event_response_inputs.convolution_analysis_profile_description = (
        event_response_inputs.convolution_analysis_profile_description
        if event_response_inputs.convolution_analysis_profile_description
        else config.get("ap_creator", "default_analysis_profile_name")
    )
    event_response_inputs.mixture_distribution_simulation_description = (
        event_response_inputs.mixture_distribution_simulation_description
        if event_response_inputs.mixture_distribution_simulation_description
        else config.get("ap_creator", "default_simulation_name")
    )
    event_response_inputs.mixture_distribution_analysis_profile_description = (
        event_response_inputs.mixture_distribution_analysis_profile_description
        if event_response_inputs.mixture_distribution_analysis_profile_description
        else config.get("ap_creator", "default_analysis_profile_name")
    )
    if event_response_inputs.mixture_distribution_simulation_start_date:
        try:
            event_response_inputs.mixture_distribution_simulation_start_date = datetime.strptime(
                event_response_inputs.mixture_distribution_simulation_start_date,
                "%Y-%m-%d",
            )
        except Exception as e:
            alert.error(
                "Unable to convert mixture_distribution_simulation_start_date to valid datetime object, provide date in 'yyyy-mm-dd' format: {e}"
            )
    else:
        event_response_inputs.mixture_distribution_simulation_start_date = None 

    if event_response_inputs.convolution_simulation_start_date:
        try:
            event_response_inputs.convolution_simulation_start_date = datetime.strptime(
                event_response_inputs.convolution_simulation_start_date, "%Y-%m-%d"
            )
        except Exception as e:
            alert.error(
                "Unable to convert convolution_simulation_start_date to valid datetime object, provide date in 'yyyy-mm-dd' format: {e}"
            )
    else:
        event_response_inputs.convolution_simulation_start_date = datetime(
            datetime.now().year, 1, 1, tzinfo=pytz.utc
        )

    return event_response_inputs


def process(event_response_inputs):
    # Accepted attributes
    #     analyzere_url - Analyze Re URL
    #     analyzere_username - Analyze Re username
    #     analyzere_password - Analyze Re password
    #     event_weights_csv - The CSV containing event weights
    #     mixture_distribution_method - Flag to toggle between Mixture Distribution and Convolution
    #     old_analysis_profile_uuid - UUID of the existing Analysis Profile that needs to be updated using mixture distribution
    #     max_trial_per_event - Maximum trial per event in the new Simulation grid
    #     mixture_distribution_simulation_description - Description of the new Simulation grid
    #     mixture_distribution_simulation_start_date - Start Date of the new Simulation grid
    #     mixture_distribution_analysis_profile_description - Description of the new Analysis Profile
    #     create_analysis_profile - Flag to decide if a new Analysis Profile is to be created using Convolution method
    #     total_number_of_events - Total number of events to be created in the Event Catalog
    #     trial_count - Trial count per event for Simulation created using Convolution method
    #     convolution_catalog_description - Description of the new Catalog
    #     convolution_simulation_description - Description of the new Simulation
    #     convolution_analysis_profile_description - Description of the new Analysis Profile
    #     convolution_simulation_start_date - Start date of the new Simulation
    #     layer_views_csv - The CSV containing list of layer_views UUID
    #     portfolio_view_uuid - The UUID of the portfolio_view (from where layer_views can be retrieved)
    #     analysis_profile_uuid_for_loss_update - The UUID of the existing Analysis Profile to be used for updating LayerViews and LossSets

    init_logging()
    output_dir = init_directories()

    # Validate inputs
    event_response_inputs = check_and_set_optional_inputs(
        event_response_inputs
    )
    validate_inputs(event_response_inputs)

    # Check if the input files exist
    for input_file in [
        event_response_inputs.event_weights_csv,
        event_response_inputs.layer_views_csv,
    ]:
        file_exists(input_file)

    login(
        event_response_inputs.analyzere_url,
        event_response_inputs.analyzere_username,
        event_response_inputs.analyzere_password,
    )

    event_response_handler = EventResponseHandler(
        output_dir=output_dir, event_response_inputs=event_response_inputs
    )
    event_response_handler.execute()


def construct_argument_parser():
    parser = argparse.ArgumentParser()

    # Server arguments
    parser.add_argument("--url", help="AnalyzeRe Server URL")
    parser.add_argument("--username", help="AnalyzeRe Username")
    parser.add_argument("--password", help="Analyzere Password")

    # Event weight CSV
    parser.add_argument(
        "--event_weights_csv",
        help="The path of the CSV file containing events and their "
        "corresponding weights",
    )

    # Choices for deciding between Mixture distribution and convolution method
    parser.add_argument(
        "--method",
        dest="method",
        choices=["mixture_distribution", "convolution"],
        help="The method to use for Event Response analysis",
        required=True,
    )

    # Method 1 - Mixture Distribution
    parser.add_argument(
        "--old_analysis_profile_uuid",
        help="The UUID of the existing Analysis Profile that needs to be updated",
    )
    parser.add_argument(
        "--max_trial_per_event",
        help="Maximum trial per event in the weighted Simulation grid",
        type=int,
    )
    parser.add_argument(
        "--mixture_distribution_simulation_description",
        help="Description of the new Simulation grid to be created, if not provided, the default value from config file is used",
    )
    parser.add_argument(
        "--mixture_distribution_analysis_profile_description",
        help="Description of the new Analysis Profile to be created, if not provided, the default value from config file is used",
    )
    parser.add_argument(
        "--mixture_distribution_simulation_start_date",
        help="Start date of the Simulation, if not provided, the date defaults to original Simgrid start date",
    )

    # Method 2 - Convolution

    # Flag to decide if a new Analysis Profile is to be created
    parser.add_argument(
        "--new_analysis_profile",
        dest="create_analysis_profile",
        action="store_true",
    )
    parser.add_argument(
        "--no_new_analysis_profile",
        dest="create_analysis_profile",
        action="store_false",
    )
    parser.set_defaults(create_analysis_profile=True)

    parser.add_argument(
        "--total_number_of_events",
        help="The total number of events to be created in the Event Catalog",
        type=int,
    )
    parser.add_argument(
        "--trial_count",
        help="Trial count per event in the Simulation grid",
        type=int,
    )
    parser.add_argument(
        "--convolution_catalog_description",
        help="Description of the new Event Catalog to be created, if not provided, the default value from config file is used",
    )
    parser.add_argument(
        "--convolution_simulation_description",
        help="Description of the new Simulation grid to be created, if not provided, the default value from config file is used",
    )
    parser.add_argument(
        "--convolution_analysis_profile_description",
        help="Description of the new Analysis Profile to be created, if not provided, the default value from config file is used",
    )
    parser.add_argument(
        "--convolution_simulation_start_date",
        help="Start date of the Simulation, if not provided, the date defaults to the first day of the current year",
    )

    # LayerView/PortfolioView arguments
    parser.add_argument(
        "--layer_views_csv",
        help="The path of the CSV file containing LayerView UUIDs",
    )
    parser.add_argument(
        "--portfolio_view_uuid",
        help="The UUID of PortfolioView containing the required LayerViews",
    )

    # Existing Analysis Profile UUID for updating LayerViews and LossSets
    parser.add_argument(
        "--analysis_profile_uuid_for_loss_update",
        help="UUID of the existing AnalysisProfile to which the LayerViews and LossSets are to be duplicated",
    )

    return parser


if __name__ == "__main__":
    # Create the command-line arguments parser and parse the arguments
    parser = construct_argument_parser()
    args = parser.parse_args()

    # Server arguments
    url = args.url
    username = args.username
    password = args.password

    # Event weight CSV
    event_weights_csv = args.event_weights_csv

    # Method to be used for Event Response
    mixture_distribution_method = (
        True if args.method == "mixture_distribution" else False
    )

    # Method 1 - Mixture Distribution
    old_analysis_profile_uuid = args.old_analysis_profile_uuid
    max_trial_per_event = args.max_trial_per_event
    mixture_distribution_simulation_description = args.mixture_distribution_simulation_description
    mixture_distribution_analysis_profile_description = args.mixture_distribution_analysis_profile_description
    mixture_distribution_simulation_start_date = args.mixture_distribution_simulation_start_date
  

    # Method 2 - Convolution
    create_analysis_profile = args.create_analysis_profile
    total_number_of_events = args.total_number_of_events
    trial_count = args.trial_count
    convolution_catalog_description = args.convolution_catalog_description
    convolution_simulation_description = args.convolution_simulation_description
    convolution_analysis_profile_description = args.convolution_analysis_profile_description
    convolution_simulation_start_date = args.convolution_simulation_start_date

    layer_views_csv = args.layer_views_csv
    portfolio_view_uuid = args.portfolio_view_uuid
    analysis_profile_uuid_for_loss_update = args.analysis_profile_uuid_for_loss_update


    event_response_inputs = SimpleNamespace(
        analyzere_url=url,
        analyzere_username=username,
        analyzere_password=password,
        event_weights_csv=event_weights_csv,
        mixture_distribution_method=mixture_distribution_method,
        old_analysis_profile_uuid=old_analysis_profile_uuid,
        max_trial_per_event=max_trial_per_event,
        mixture_distribution_simulation_description=mixture_distribution_simulation_description,
        mixture_distribution_analysis_profile_description=mixture_distribution_analysis_profile_description,
        mixture_distribution_simulation_start_date=mixture_distribution_simulation_start_date,
        create_analysis_profile=create_analysis_profile,
        total_number_of_events=total_number_of_events,
        trial_count=trial_count,
        convolution_catalog_description=convolution_catalog_description,
        convolution_simulation_description=convolution_simulation_description,
        convolution_analysis_profile_description=convolution_analysis_profile_description,
        convolution_simulation_start_date=convolution_simulation_start_date,
        layer_views_csv=layer_views_csv,
        portfolio_view_uuid=portfolio_view_uuid,
        analysis_profile_uuid_for_loss_update=analysis_profile_uuid_for_loss_update,
    )

    process(event_response_inputs)
