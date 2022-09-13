import argparse
import logging
import logging.config

import analyzere

from data_parser.config import ConfigFile
from data_retriever.csv_data_retriever import CSVDataRetriever
from data_retriever.sql_data_retriever import SQLDataRetriever
from data_parser.loss_parser import LossParser
from data_parser.layer_parser import LayerParser
from data_uploader.are_uploader import BulkUploader

logging.config.fileConfig("logging.ini")
LOG = logging.getLogger(__name__)

retrievers = {
    "csv": CSVDataRetriever,
    "sql": SQLDataRetriever
}


def construct_argument_parser():
    # Global Parser
    parser = argparse.ArgumentParser()

    # General arguments
    parser.add_argument("--url", help="server URL")
    parser.add_argument("--username", help="username")
    parser.add_argument("--password", help="password")
    parser.add_argument("--config", help="configuration file", required=True)

    subparsers = parser.add_subparsers(dest='source', metavar="SOURCE")
    subparsers.required=True
    # Add the additional arguments for our retrievers
    for name in retrievers:
        subparser = subparsers.add_parser(name)
        # Each retriever adds its own arguments
        retrievers[name].add_parser_arguments(subparser)

    return parser


def set_and_check_credentials(url, username, password):
    try:
        analyzere.base_url = url
        analyzere.username = username
        analyzere.password = password

        analyzere.EventCatalog.list(limit=0)
    except Exception as e:
        LOG.error(f"Unable to connect to {url}: {e}")
        raise e
    else:
        LOG.info(f"Log in successful for user {username}")


if __name__ == "__main__":
    # Create the command-line arguments parser and parse arguments
    parser = construct_argument_parser()
    args = parser.parse_args()

    # Load the config file
    config = ConfigFile(args.config)
    
    # Log in to the server - Use the server details from the config 
    # file unless it has been overridden by the command-line arguments.
    url = (
        args.url if args.url 
        else config.server.base_url
    )
    username = (
        args.username if args.username 
        else config.server.username
    )
    password = (
        args.password if args.password 
        else config.server.password
    )
    
    # This will simply throw if the credentials don't work.
    set_and_check_credentials(url, username, password)
    
    # Initialze the retriever
    retriever = retrievers[args.source](args, config)

    # Retrieve layer and loss data from the source
    layer_df, loss_df = retriever.get_bulk_data()
    LOG.info("Successfully read input data.")
        
    # Parse the data from Layer and Loss Dataframes
    loss_parser = LossParser(loss_df, retriever.loss_type, config)
    processed_loss_df = loss_parser.parse_loss_df()
    LOG.info("Successfully parsed input loss data.")

    layer_parser = LayerParser(layer_df, config)
    processed_layer_df = layer_parser.parse_layer_df()
    LOG.info("Successfully parsed input layer definitions.")
    
    # Upload the data from Layer and Loss Dataframes
    bulk_uploader = BulkUploader(
        processed_layer_df,
        processed_loss_df,
        retriever.loss_type,
        config.defaults.loss_position,
        config.defaults.analysis_profile_uuid,
        analyzere,
        config
    )
    bulk_uploader.bulk_upload()
    bulk_uploader.write_output_files()
