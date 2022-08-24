import argparse
import configparser
import os
import sys
import analyzere
import requests
import warnings


from data_parser.config_parser import ConfigFileParser
from data_retriever.csv_data_retriever import CSVDataRetriever
from data_retriever.sql_data_retriever import SQLDataRetriever
from data_parser.loss_parser import LossParser
from data_parser.layer_parser import LayerParser
from data_uploader.are_uploader import BulkUploader

from analyzere import AuthenticationError

warnings.filterwarnings("ignore")

def get_config_file(config_file_name):
    config_file = os.path.abspath(config_file_name)
    if not os.path.exists(config_file):
        sys.exit('Config file not present. Cannot continue.')
    config = configparser.ConfigParser()
    config.read(config_file)

    return config


def construct_argument_parser():
    # Global Parser
    parser = argparse.ArgumentParser()

    parser.add_argument("--url", help="Server URL")
    parser.add_argument("--user", help="Username")
    parser.add_argument("--password", help="Password")

    subparsers = parser.add_subparsers(dest='command')
    csv = subparsers.add_parser('upload_from_csv')
    sql = subparsers.add_parser('upload_from_sql')
    subparsers.required=True

    # CSV
    csv.add_argument("--layer",
                        help="Bulk Layer Terms CSV")

    group = csv.add_mutually_exclusive_group()
    group.add_argument("--elt", help="Bulk ELT CSV")
    group.add_argument("--yelt", help="Bulk YELT CSV")
    group.add_argument("--ylt", help="Bulk YLT CSV")

    # SQL
    parsed_args = vars(parser.parse_args())

    return parsed_args


def are_login(url, username, password):
    try:
        analyzere.base_url = url
        analyzere.username = username
        analyzere.password = password

        analyzere.EventCatalog.list(limit=0)
    except AuthenticationError as e:
        analyzere.username = ""
        analyzere.password = ""
        status_msg = "Invalid username/password"
        return False, status_msg, analyzere
        # Handle 'gaierror' (i.e. invalid url)
    except requests.ConnectionError as e:
        status_msg = "Could not resolve URL"
        return False, status_msg, analyzere
    except OSError as e:
        status_msg = "OSError: {}".format(e)
        return False, status_msg, analyzere
    else:
        return True, 'Log in successful for user {}'.format(username), analyzere


def check_loss_file_input(elt_file, yelt_file, ylt_file):
    loss_type = None
    loss_file = None

    if elt_file:
        loss_type = 'elt'
        loss_file = elt_file
    elif yelt_file:
        loss_type = 'yelt'
        loss_file = yelt_file
    else:
        loss_type = 'ylt'
        loss_file = ylt_file

    return loss_type, loss_file


if __name__ == '__main__':
    args = construct_argument_parser()

    # Load the config file
    config_file_name = 'config/config.ini'
    config = get_config_file(config_file_name)
    config_parser = ConfigFileParser(config)

    # Log in to the server - Use the server details from the config 
    # file unless it has been overridden by the command-line arguments.
    url = username = pwd = None
    if args['url'] and args['user'] and args['password']:
        url = args['url']
        username = args['user']
        pwd = args['password']
    else:
        url = config_parser.get_server_details().base_url
        username = config_parser.get_server_details().username
        pwd = config_parser.get_server_details().password
    logged_in, status, analyzere = are_login(url, username, pwd)

    if not logged_in:
        sys.exit(status)
    print(status)

    # Initialze retrievers
    upload_from_csv = False
    loss_type = None
    if args['command'] == 'upload_from_csv':
        upload_from_csv = True
        loss_type, loss_file = check_loss_file_input(args['elt'], args['yelt'], args['ylt'])
        csv_retriever = CSVDataRetriever(args['layer'], loss_file)
    else:
        sql_retriever = SQLDataRetriever(config_parser)

    # Retrieve layer and loss data from the source
    layer_df, loss_df = csv_retriever.get_bulk_data() if upload_from_csv else sql_retriever.get_bulk_data()
    print('Retrieved bulk data')

    # Parse the data from Layer and Loss Dataframes
    loss_type = config_parser.get_defaults().loss_type if not loss_type else loss_type
    loss_parser = LossParser(loss_df, loss_type, config_parser)
    processed_loss_df = loss_parser.parse_loss_df()

    layer_parser = LayerParser(layer_df, config_parser)
    processed_layer_df = layer_parser.parse_layer_df()

    # Upload the data from Layer and Loss Dataframes
    bulk_uploader = BulkUploader(
        processed_layer_df,
        processed_loss_df,
        loss_type,
        config_parser.get_defaults().loss_position,
        config_parser.get_defaults().analysis_profile_uuid,
        analyzere,
        config_parser
    )
    bulk_uploader.bulk_upload()
    bulk_uploader.write_output_files()
