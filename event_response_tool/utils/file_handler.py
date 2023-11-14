import os
import logging
import pandas as pd
import io
import re

from utils.alert import Alert as alert

logger = logging.getLogger()


def file_exists(file_path):
    if file_path is not None and len(str(file_path)) > 0:
        try:
            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"{file_path}")
        except FileNotFoundError as e:
            alert.exception(
                f"Unable to locate the input file '{file_path}': {e}"
            )
        else:
            alert.debug(f"Found file {file_path}")


def read_input_file(file_path):
    try:
        input_file_df = pd.read_csv(file_path)
        input_file_df.columns = input_file_df.columns.str.lower()
    except Exception as e:
        alert.exception(
            f"Exception occurred while reading file '{file_path}': {e}"
        )
    else:
        return input_file_df


def read_byte_stream_into_csv(byte_stream):
    try:
        input_file_df = pd.read_csv(io.BytesIO(byte_stream))
        input_file_df.columns = input_file_df.columns.str.lower()
    except Exception as e:
        alert.exception(f"Exception occurred while reading byte stream: {e}")
    else:
        return input_file_df


def find_column(keyword, column_names):
    keyword_regex = re.compile(r"{}".format(keyword), re.IGNORECASE)
    for column_name in column_names:
        if keyword_regex.findall(column_name):
            return column_name


def write_output_file(output_data, column_names_list, file_name, file_path):
    try:
        output_file_df = pd.DataFrame(output_data, columns=column_names_list)
        output_file_df.to_csv(f"{file_path}/{file_name}", index=False)
    except Exception as e:
        alert.exception(
            f"Exception occurred while writing file {file_path}/{file_name}: {e}"
        )
    else:
        return output_file_df


def join(df_1, df_2, how="inner", on=None):
    return pd.merge(df_1, df_2, how=how, on=on)
