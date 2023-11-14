import os
import logging
import logging.config
import shutil
import configparser
import argparse
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
        self.event_weights_df = read_input_file(
            self.event_response_inputs.event_weights_csv
        )
        if (
            self.event_response_inputs.layer_views_csv
            or self.event_response_inputs.portfolio_view_uuid
        ):
            modify_layer_loss = LayerLossDuplicator(
                config=config,
                output_dir=self.output_dir,
                event_weights_df=self.event_weights_df,
                layer_ids_csv=self.event_response_inputs.layer_views_csv,
                portfolio_uuid=self.event_response_inputs.portfolio_view_uuid,
            )
            modify_layer_loss.modify_layer_loss_data()
        else:
            ap_creator = AnalysisProfileCreator(
                config=config,
                event_weights_df=self.event_weights_df,
                total_num_of_events=self.event_response_inputs.total_number_of_events,
                trial_count=self.event_response_inputs.trial_count,
                catalog_description=self.event_response_inputs.catalog_description,
                simulation_description=self.event_response_inputs.simulation_description,
                analysis_profile_description=self.event_response_inputs.analysis_profile_description,
                old_analysis_profile_uuid=self.event_response_inputs.old_analysis_profile_uuid,
            )
            ap_creator.build_analysis_profile()


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
    # Check for required inputs
    if (
        event_response_inputs.analyzere_url is None
        or len(event_response_inputs.analyzere_url) == 0
    ):
        alert.error("Please input AnalyzeRe URL")
    if (
        event_response_inputs.analyzere_username is None
        or len(event_response_inputs.analyzere_username) == 0
    ):
        alert.error("Please input AnalyzeRe username")
    if (
        event_response_inputs.analyzere_password is None
        or len(event_response_inputs.analyzere_password) == 0
    ):
        alert.error("Please input AnalyzeRe password")

    if (
        event_response_inputs.event_weights_csv is None
        or len(event_response_inputs.event_weights_csv) == 0
    ):
        alert.error(
            "Please input the path of the CSV containing event weights"
        )

    # If layer_view CSV or portfolio_view UUID is not provided,
    # check if analysis profile inputs are available
    if (
        event_response_inputs.layer_views_csv is None
        or len(event_response_inputs.layer_views_csv) == 0
    ) and (
        event_response_inputs.portfolio_view_uuid is None
        or len(event_response_inputs.portfolio_view_uuid) == 0
    ):
        if not (
            event_response_inputs.old_analysis_profile_uuid
            or (
                event_response_inputs.total_number_of_events
                and event_response_inputs.trial_count
            )
        ):
            alert.error(
                "Please provide inputs for either duplicating LayerViews or creating/updating Event Response Analysis Profile"
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
    event_response_inputs.catalog_description = (
        event_response_inputs.catalog_description
        if event_response_inputs.catalog_description
        else config.get("ap_creator", "default_catalog_name")
    )
    event_response_inputs.simulation_description = (
        event_response_inputs.simulation_description
        if event_response_inputs.simulation_description
        else config.get("ap_creator", "default_simulation_name")
    )
    event_response_inputs.analysis_profile_description = (
        event_response_inputs.analysis_profile_description
        if event_response_inputs.analysis_profile_description
        else config.get("ap_creator", "default_analysis_profile_name")
    )

    return event_response_inputs


def process(event_response_inputs):
    # Accepted attributes
    #     analyzere_url - Analyze Re URL
    #     analyzere_username - Analyze Re username
    #     analyzere_password - Analyze Re password
    #     event_weights_csv - The CSV containing event weights
    #     layer_views_csv - The CSV containing list of layer_views UUID
    #     portfolio_view_uuid - The UUID of the portfolio_view (from where layer_views can be retrieved)
    #     total_number_of_events - The total number of events to be created in the event response analysis profile
    #     trial_count - The maximum number of trials per event
    #     catalog_description - The description of the new event catalog to be created
    #     simulation_description - The description of the new simgrid to be created
    #     analysis_profile_description - The description of the new analysis profile to be created
    #     old_analysis_profile_uuid - The UUID of the analysis profile to be updated with a new simgrid

    # warnings.filterwarnings("ignore")
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

    # Required event weight CSV
    parser.add_argument(
        "--event_weights_csv",
        help="The path of the CSV file containing events and their "
        "corresponding weights",
        required=True,
    )

    # LayerView manipulation arguments
    parser.add_argument(
        "--layer_views_csv",
        help="The path of the CSV file containing LayerView UUIDs",
    )
    parser.add_argument(
        "--portfolio_view_uuid",
        help="The UUID of PortfolioView containing the required LayerViews",
    )

    # Analysis Profile creation/updation arguments
    parser.add_argument(
        "--total_number_of_events",
        help="The total number of events to be created in the event response analysis profile",
    )
    parser.add_argument(
        "--trial_count", help="The maximum number of trials per event"
    )
    parser.add_argument(
        "--catalog_description",
        help="The description of the new event catalog to be created",
    )
    parser.add_argument(
        "--simulation_description",
        help="The description of the new simgrid to be created",
    )
    parser.add_argument(
        "--analysis_profile_description",
        help="The description of the new analysis profile to be created",
    )
    parser.add_argument(
        "--old_analysis_profile_uuid",
        help="The UUID of the analysis profile to be updated with a new simgrid",
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

    # Mandatory event weight argument
    event_weights_csv = args.event_weights_csv

    # LayerView manipulation arguments
    layer_views_csv = args.layer_views_csv
    portfolio_view_uuid = args.portfolio_view_uuid

    # Analysis Profile creation/updation arguments
    total_number_of_events = args.total_number_of_events
    trial_count = args.trial_count
    catalog_description = args.catalog_description
    simulation_description = args.simulation_description
    analysis_profile_description = args.analysis_profile_description
    old_analysis_profile_uuid = args.old_analysis_profile_uuid

    event_response_inputs = SimpleNamespace(
        analyzere_url=url,
        analyzere_username=username,
        analyzere_password=password,
        event_weights_csv=event_weights_csv,
        layer_views_csv=layer_views_csv,
        portfolio_view_uuid=portfolio_view_uuid,
        total_number_of_events=total_number_of_events,
        trial_count=trial_count,
        catalog_description=catalog_description,
        simulation_description=simulation_description,
        analysis_profile_description=analysis_profile_description,
        old_analysis_profile_uuid=old_analysis_profile_uuid,
    )

    process(event_response_inputs)
