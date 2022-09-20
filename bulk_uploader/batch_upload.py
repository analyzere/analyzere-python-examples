import argparse
import logging
import logging.config
from string import ascii_uppercase
from random import choices

import analyzere

# analyzere.tls_verify = False

from pathlib import Path
from config import ConfigFile
from retrievers.csv_data_retriever import CSVDataRetriever
from retrievers.sql_data_retriever import SQLDataRetriever
from extractors.loss_set import LossSetExtractor
from extractors.layer import LayerExtractor
from uploaders.are_uploader import BatchUploader

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
    parser.add_argument(
        "--config", 
        help="configuration file", 
        default=Path(__file__).parent / "config.ini"
    )
    parser.add_argument(
        "--batch-id", 
        dest="batch_id", 
        default=''.join(choices(ascii_uppercase, k=6))
    )

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

    LOG.info(f"Uploading batch with Batch-ID: {args.batch_id}")

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
    layers_data = retriever.get_layers()
    losses_data = retriever.get_losses()
    LOG.info("Successfully read input data.")
        
    # Parse the data from Layer and Loss Dataframes
    loss_set_extractor = LossSetExtractor(losses_data, retriever.loss_type, config)
    LOG.info("Successfully initialized loss set extractor.")

    layer_extractor = LayerExtractor(layers_data, config)
    LOG.info("Successfully initialized layer extractor.")
    
    # Upload the data from Layer and Loss Dataframes
    batch_uploader = BatchUploader(
        layer_extractor,
        loss_set_extractor,
        args.batch_id,
        config
    )
    batch_uploader.batch_upload()