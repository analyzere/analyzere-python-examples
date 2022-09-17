from analyzere import (
    LossSet,
    Layer,
    AnalysisProfile,
    MonetaryUnit,
    Reinstatement,
)
import multiprocessing
import sys
import csv

from analyzere import InvalidRequestError
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
import pandas as pd

from extractors.layer import (
    LayerExtractor,
    get_str,
    get_date,
    get_money,
    get_float,
    get_int,
)
from extractors.loss_set import LossSetExtractor

import logging

LOG = logging.getLogger()

##################################

from uuid import uuid4


def patch_loss_set_save(ls):
    ls.id = str(uuid4())
    LOG.info(f"Saved loss set {ls.description} to ID {ls.id}")
    return ls


LossSet.save = patch_loss_set_save


def patch_loss_set_upload_data(ls, f):
    LOG.info(f"Uploading {len(f.readlines())} rows for LS {ls.description}")


LossSet.upload_data = patch_loss_set_upload_data


def patch_layer_save(layer):
    layer.id = str(uuid4())
    LOG.info(f"Saved layer {layer.description} to ID {layer.id}")
    return layer


Layer.save = patch_layer_save


####################################


class DataToCSVIO:
    def __init__(self, data):
        self.csv_io = StringIO()
        data.to_csv(self.csv_io, index=False)

    def __enter__(self):
        self.csv_io.seek(0)
        return self.csv_io

    def __exit__(self, *args):
        pass


class BatchUploader:
    """
    Responsible for uploading Losses and Layers to the platform.

    Some of the main tasks performed by this class includes,
        * Splitting the LossSets
        * Uploading the individual LossSets
        * Uploading the individual Layers
        * Writing the uploaded Layer - Loss UUID mapping in a CSV

    """

    def write_loss_data_to_string(self, df, column_list):
        """
        Store the Loss content in a file-like string buffer
        """
        csv_string = StringIO()
        df.to_csv(csv_string, index=False, columns=column_list)

        return csv_string.getvalue()

    def check_loss_set_upload_status(self, loss_set):
        while True:
            loss_set.reload()
            if loss_set.status in [
                "processing_failed",
                "processing_succeeded",
            ]:
                break
        return loss_set.status, loss_set.status_message

    def retrieve_loss_metadata_from_layer_terms(self, loss_set_id):
        layer_row = self.layers_df.loc[
            self.layers_df[self.layer_columns.layer_id] == loss_set_id
        ]
        currency = None
        start_date = None

        if self.layer_columns.loss_set_currency in self.layers_df.columns:
            currency = layer_row[self.layer_columns.loss_set_currency].values[
                0
            ]

        if self.layer_columns.loss_set_start_date in self.layers_df.columns:
            start_date = layer_row[
                self.layer_columns.loss_set_start_date
            ].values[0]

        return currency, start_date

    def sort_loss_data(self, loss_df, columns):
        sorted_loss_df = loss_df.sort_values(by=columns)
        return sorted_loss_df

    def _upload_ylt(self, loss_df, unique_loss_set_name):
        try:
            trial_id = self.loss_set_columns.trial_id
            loss = self.loss_set_columns.loss
            reinstatement_premium = (
                self.loss_set_columns.reinstatement_premium
            )
            reinstatement_brokerage = (
                self.loss_set_columns.reinstatement_brokerage
            )
            has_reinstatements = False

            # Sort the YLT dataframe
            sorted_loss_df = self.sort_loss_data(loss_df, [trial_id])

            column_list = [trial_id, loss]

            if (
                reinstatement_premium in loss_df.columns
                and reinstatement_brokerage in loss_df.columns
            ):
                column_list.append(reinstatement_premium)
                column_list.append(reinstatement_brokerage)
                has_reinstatements = True

            # Write Loss DataFrame contents to a string
            loss_data_str = self.write_loss_data_to_string(
                sorted_loss_df, column_list
            )

            # Fetch LossSet Currency and StartDate from Layer terms file if they exist
            (
                currency,
                start_date,
            ) = self.retrieve_loss_metadata_from_layer_terms(
                unique_loss_set_name
            )

            # Set default values for currency and start_date if the user hasn't provided them
            if not currency:
                currency = self.defaults.default_currency
            if not start_date:
                start_date = self.defaults.default_start_date

            trial_count = self.defaults.default_trial_count

            # Upload the YLT
            loss_set = analyzere.LossSet(
                type="YLTLossSet",
                description=unique_loss_set_name,
                start_date=start_date,
                currency=currency,
                loss_type="LossNetOfAggregateTerms"
                if has_reinstatements
                else self.loss_position,
                trial_count=trial_count,
            ).save()

            loss_set.upload_data(loss_data_str)

            status, status_msg = self.check_loss_set_upload_status(loss_set)

            return (
                loss_set.id,
                loss_set.description,
                status,
                status_msg,
                unique_loss_set_name,
            )

        except Exception as e:
            raise Exception(
                "Error occurred while uploading LossSet {}: {}".format(
                    unique_loss_set_name, e
                )
            )

    def _upload_yelt(self, loss_df, unique_loss_set_name):
        try:
            trial_id = self.loss_set_columns.trial_id
            event_id = self.loss_set_columns.event_id
            sequence = self.loss_set_columns.sequence
            loss = self.loss_set_columns.loss
            reinstatement_premium = (
                self.loss_set_columns.reinstatement_premium
            )
            reinstatement_brokerage = (
                self.loss_set_columns.reinstatement_brokerage
            )
            has_reinstatements = False

            # Sort the YELT dataframe
            sorted_loss_df = self.sort_loss_data(
                loss_df, [trial_id, sequence, event_id]
            )

            column_list = [trial_id, event_id, sequence, loss]

            if (
                reinstatement_premium in loss_df.columns
                and reinstatement_brokerage in loss_df.columns
            ):
                column_list.append(reinstatement_premium)
                column_list.append(reinstatement_brokerage)
                has_reinstatements = True

            # Write Loss DataFrame contents to a string
            loss_data_str = self.write_loss_data_to_string(
                sorted_loss_df, column_list
            )

            # Fetch LossSet Currency and StartDate from Layer terms file if they exist

            (
                currency,
                start_date,
            ) = self.retrieve_loss_metadata_from_layer_terms(
                unique_loss_set_name
            )

            # Set default values for currency and start_date if the user hasn't provided them
            if not currency:
                currency = self.defaults.default_currency
            if not start_date:
                start_date = self.defaults.default_start_date

            trial_count = self.defaults.default_trial_count

            # Upload the YELT
            loss_set = analyzere.LossSet(
                type="YELTLossSet",
                description=unique_loss_set_name,
                event_catalogs=[self.catalog],
                start_date=start_date,
                currency=currency,
                loss_type="LossNetOfAggregateTerms"
                if has_reinstatements
                else self.loss_position,
                trial_count=trial_count,
            ).save()

            loss_set.upload_data(loss_data_str)

            status, status_msg = self.check_loss_set_upload_status(loss_set)

            return (
                loss_set.id,
                loss_set.description,
                status,
                status_msg,
                unique_loss_set_name,
            )

        except Exception as e:
            raise Exception(
                "Error occurred while uploading LossSet {}: {}".format(
                    unique_loss_set_name, e
                )
            )

    def _upload_elt(self, loss_df, unique_loss_set_name):
        try:
            event_id = self.loss_set_columns.event_id
            loss = self.loss_set_columns.loss

            column_list = [event_id, loss]

            # Write Loss DataFrame contents to a string
            loss_data_str = self.write_loss_data_to_string(
                loss_df, column_list
            )

            # Fetch LossSet Currency from Layer terms file if it exists
            currency, _ = self.retrieve_loss_metadata_from_layer_terms(
                unique_loss_set_name
            )

            # Set default values for currency if the user hasn't provided one
            if not currency:
                currency = self.defaults.default_currency

            # Upload the ELT
            loss_set = analyzere.LossSet(
                type="ELTLossSet",
                description=unique_loss_set_name,
                event_catalogs=[self.catalog],
                currency=currency,
                loss_type=self.loss_position,
            ).save()

            loss_set.upload_data(loss_data_str)

            status, status_msg = self.check_loss_set_upload_status(loss_set)

            return (
                loss_set.id,
                loss_set.description,
                status,
                status_msg,
                unique_loss_set_name,
            )

        except Exception as e:
            raise Exception(
                "Error occurred while uploading LossSet {}: {}".format(
                    unique_loss_set_name, e
                )
            )

    def split_loss_sets(self, unique_loss_set_name):
        ls_df = self.loss_df[
            self.loss_df[self.loss_set_columns.loss_set_id]
            == unique_loss_set_name
        ]
        ls_uuid = ls_desc = status = status_msg = ls_id = None
        if self.loss_type.lower() == "elt":
            ls_uuid, ls_desc, status, status_msg, ls_id = self._upload_elt(
                ls_df, unique_loss_set_name
            )
        if self.loss_type.lower() == "yelt":
            ls_uuid, ls_desc, status, status_msg, ls_id = self._upload_yelt(
                ls_df, unique_loss_set_name
            )
        if self.loss_type.lower() == "ylt":
            ls_uuid, ls_desc, status, status_msg, ls_id = self._upload_ylt(
                ls_df, unique_loss_set_name
            )

        return ls_uuid, ls_desc, status, status_msg, ls_id

    def process_loss_df(self):
        print("Uploading LossSets")
        self.loss_mapping = {}
        self.uploaded_loss_set_details = {}

        unique_loss_sets = (
            self.loss_df[self.loss_set_columns.loss_set_id].unique().tolist()
        )

        processed = 0
        print("Splitting LossSets")
        with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
            for ls_uuid, ls_desc, status, status_msg, ls_id in executor.map(
                self.split_loss_sets, unique_loss_sets
            ):
                processed += 1
                self.uploaded_loss_set_details[ls_id] = [
                    ls_desc,
                    status,
                    status_msg,
                ]
                self.loss_mapping[ls_id] = ls_uuid

        print(f"Uploaded {len(self.loss_mapping)} LossSets")

    def get_are_resources(self):
        self.profile = analyzere.AnalysisProfile.retrieve(
            self.analysis_profile_uuid
        )
        self.catalog = self.profile.event_catalogs[0]

    def get_loss_set(self, row):
        loss_id = self.layer_columns.layer_id
        try:
            loss_uuid = self.loss_mapping[row[loss_id]]
            loss_set = analyzere.LossSet.retrieve(loss_uuid)
            return loss_set
        except KeyError as e:
            print(
                "Not able to find associated LossSet for Layer {}".format(
                    loss_id
                )
            )
        except Exception as e:
            print(e)

    def get_layer_term_value(self, row, layer_term):
        value = None
        if layer_term in self.layers_df.columns:
            value = row[layer_term]
        if layer_term == self.layer_columns.franchise:
            if not layer_term in self.layers_df.columns:
                value = analyzere.MonetaryUnit(0, "USD")
        return value

    def _upload_qs(self, row):
        try:
            inception = self.get_layer_term_value(
                row, self.layer_columns.inception
            )
            expiry = self.get_layer_term_value(row, self.layer_columns.expiry)
            participation = self.get_layer_term_value(
                row, self.layer_columns.participation
            )
            premium = self.get_layer_term_value(
                row, self.layer_columns.premium
            )
            event_limit = self.get_layer_term_value(
                row, self.layer_columns.event_limit
            )
            description = self.get_layer_term_value(
                row, self.layer_columns.description
            )
            meta_data = self.get_layer_term_value(
                row, self.layer_columns.meta_data
            )

            # Retrieve associated LossSet
            loss_set = self.get_loss_set(row)

            return analyzere.Layer(
                type="QuotaShare",
                description=description,
                loss_sets=[loss_set],
                inception_date=inception,
                expiry_date=expiry,
                premium=premium,
                participation=participation,
                event_limit=event_limit,
                meta_data=meta_data,
            ).save()

        except Exception as e:
            raise Exception(
                "Error uploading Layer {}: {}".format(
                    row[self.layer_columns.layer_id], e
                )
            )

    def _upload_aggxl(self, row):
        try:
            inception = self.get_layer_term_value(
                row, self.layer_columns.inception
            )
            expiry = self.get_layer_term_value(row, self.layer_columns.expiry)
            participation = self.get_layer_term_value(
                row, self.layer_columns.participation
            )
            premium = self.get_layer_term_value(
                row, self.layer_columns.premium
            )
            attachment = self.get_layer_term_value(
                row, self.layer_columns.attachment
            )
            limit = self.get_layer_term_value(row, self.layer_columns.limit)
            agg_att = self.get_layer_term_value(
                row, self.layer_columns.aggregate_attachment
            )
            agg_limit = self.get_layer_term_value(
                row, self.layer_columns.aggregate_limit
            )
            franchise = self.get_layer_term_value(
                row, self.layer_columns.franchise
            )
            description = self.get_layer_term_value(
                row, self.layer_columns.description
            )
            meta_data = self.get_layer_term_value(
                row, self.layer_columns.meta_data
            )

            # Retrieve associated LossSet
            loss_set = self.get_loss_set(row)

            return analyzere.Layer(
                type="AggXL",
                description=description,
                loss_sets=[loss_set],
                inception_date=inception,
                expiry_date=expiry,
                premium=premium,
                participation=participation,
                attachment=attachment,
                limit=limit,
                aggregate_attachment=agg_att,
                aggregate_limit=agg_limit,
                franchise=franchise,
                meta_data=meta_data,
            ).save()

        except Exception as e:
            raise Exception(
                "Error uploading Layer {}: {}".format(
                    row[self.layer_columns.layer_id], e
                )
            )

    def _upload_catxl(self, row):
        try:
            inception = self.get_layer_term_value(
                row, self.layer_columns.inception
            )
            expiry = self.get_layer_term_value(row, self.layer_columns.expiry)
            participation = self.get_layer_term_value(
                row, self.layer_columns.participation
            )
            premium = self.get_layer_term_value(
                row, self.layer_columns.premium
            )
            attachment = self.get_layer_term_value(
                row, self.layer_columns.attachment
            )
            limit = self.get_layer_term_value(row, self.layer_columns.limit)
            franchise = self.get_layer_term_value(
                row, self.layer_columns.franchise
            )
            reinstatements = self.get_layer_term_value(
                row, self.layer_columns.reinstatements
            )

            nth = None
            if self.get_layer_term_value(row, self.layer_columns.nth) is None:
                nth = 1
            else:
                nth = self.get_layer_term_value(row, self.layer_columns.nth)

            description = self.get_layer_term_value(
                row, self.layer_columns.description
            )
            meta_data = self.get_layer_term_value(
                row, self.layer_columns.meta_data
            )

            # Retrieve associated LossSet
            loss_set = self.get_loss_set(row)

            return analyzere.Layer(
                type="CatXL",
                description=description,
                loss_sets=[loss_set],
                inception_date=inception,
                expiry_date=expiry,
                reinstatements=reinstatements,
                premium=premium,
                participation=participation,
                attachment=attachment,
                limit=limit,
                nth=nth,
                franchise=franchise,
                meta_data=meta_data,
            ).save()

        except Exception as e:
            raise Exception(
                "Error uploading Layer {}: {}".format(
                    row[self.layer_columns.layer_id], e
                )
            )

    def _upload_generic(self, row):
        try:
            inception = self.get_layer_term_value(
                row, self.layer_columns.inception
            )
            expiry = self.get_layer_term_value(row, self.layer_columns.expiry)
            participation = self.get_layer_term_value(
                row, self.layer_columns.participation
            )
            premium = self.get_layer_term_value(
                row, self.layer_columns.premium
            )
            attachment = self.get_layer_term_value(
                row, self.layer_columns.attachment
            )
            limit = self.get_layer_term_value(row, self.layer_columns.limit)
            agg_att = self.get_layer_term_value(
                row, self.layer_columns.aggregate_attachment
            )
            agg_limit = self.get_layer_term_value(
                row, self.layer_columns.aggregate_limit
            )
            franchise = self.get_layer_term_value(
                row, self.layer_columns.franchise
            )
            reinstatements = self.get_layer_term_value(
                row, self.layer_columns.reinstatements
            )
            description = self.get_layer_term_value(
                row, self.layer_columns.description
            )
            meta_data = self.get_layer_term_value(
                row, self.layer_columns.meta_data
            )

            # Retrieve associated LossSet
            loss_set = self.get_loss_set(row)

            return analyzere.Layer(
                type="Generic",
                description=description,
                loss_sets=[loss_set],
                inception_date=inception,
                expiry_date=expiry,
                reinstatements=reinstatements,
                premium=premium,
                participation=participation,
                attachment=attachment,
                limit=limit,
                aggregate_attachment=agg_att,
                aggregate_limit=agg_limit,
                franchise=franchise,
                meta_data=meta_data,
            ).save()

        except Exception as e:
            raise Exception(
                "Error uploading Layer {}: {}".format(
                    row[self.layer_columns.layer_id], e
                )
            )

    def process_layer_record(self, layer_row):
        layer_type_column_key = self.layer_columns.layer_type
        if layer_type_column_key in self.layers_df:
            layer_type = layer_row[layer_type_column_key]
            layer_type = (
                layer_type.replace(" ", "").lower()
                if layer_type is not None
                else "generic"
            )
        else:
            layer_type = "generic"

        if layer_type == "generic":
            layer = self._upload_generic(layer_row)
        elif layer_type == "aggxl":
            layer = self._upload_aggxl(layer_row)
        elif layer_type == "catxl":
            layer = self._upload_catxl(layer_row)
        elif layer_type == "quotashare" or layer_type == "qs":
            layer = self._upload_qs(layer_row)

        self.layer_mapping[layer.id] = [layer.description, layer.loss_sets]

    def process_layer_df(self):
        print("Uploading Layers")
        self.layer_mapping = {}
        self.layers_df.apply(self.process_layer_record, axis=1)
        if len(self.layer_mapping) > 0:
            print(f"Uploaded {len(self.layer_mapping)} Layers")
        else:
            print("No layers were uploaded")

    def get_config_values(self):
        self.defaults = self.config_file_parser.get_defaults()
        self.layer_columns = self.config_file_parser.get_layer_term_columns()
        self.loss_set_columns = (
            self.config_file_parser.get_ARe_loss_set_columns()
        )

    def create_elt_loss_set(self, layer_id):
        return LossSet(
            type="ELTLossSet",
            description=f"Loss Set for Layer {layer_id}",
            event_catalogs=self.analysis_profile.event_catalogs,
            currency=self.config.defaults.currency,
            loss_type=self.config.defaults.loss_perspective,
            meta_data=dict(
                upload_batch_layer_id=layer_id, upload_batch_id=self.batch_id
            ),
        )

    def create_yelt_loss_set(self, layer_id):
        return LossSet(
            type="YELTLossSet",
            description=f"Loss Set for Layer {layer_id}",
            event_catalogs=self.analysis_profile.event_catalogs,
            currency=self.config.defaults.currency,
            loss_type=self.config.defaults.loss_perspective,
            start_date=self.config.defaults.start_date,
            trial_count=self.config.defaults.trial_count,
            meta_data=dict(
                upload_batch_layer_id=layer_id, upload_batch_id=self.batch_id
            ),
        )

    def create_ylt_loss_set(self, layer_id):
        return LossSet(
            type="YLTLossSet",
            description=f"Loss Set for Layer {layer_id}",
            event_catalogs=self.analysis_profile.event_catalogs,
            currency=self.config.defaults.currency,
            loss_type=self.config.defaults.loss_perspective,
            trial_count=self.config.defaults.trial_count,
            meta_data=dict(
                upload_batch_layer_id=layer_id, upload_batch_id=self.batch_id
            ),
        )

    def upload_loss_set(self, layer_id):
        loss_set_factories = dict(
            elt=self.create_elt_loss_set,
            yelt=self.create_yelt_loss_set,
            ylt=self.create_ylt_loss_set,
        )

        factory = loss_set_factories[self.loss_ext.loss_type]
        loss_set = factory(layer_id)
        loss_set.save()

        # Get the sorted loss set data
        data = self.loss_ext.get_loss_set(layer_id)

        # Convert data to CSV file stream
        with DataToCSVIO(data) as f:
            loss_set.upload_data(f)

        return loss_set

    def get_money(self, row, term, term_ccy=None, default=0):
        value, ccy = get_money(row, term, term_ccy)
        layer_ccy = get_str(row, self.layer_columns.currency)

        return MonetaryUnit(
            value or default,
            ccy if ccy else layer_ccy or self.config.defaults.currency,
        )

    def get_reinstatements(self, row):
        return [
            Reinstatement(premium=p, brokerage=b)
            for p, b in self.layer_ext.get_reinstatements(row)
        ]

    def get_metadata(self, layer_id, row):
        metadata = self.layer_ext.get_metadata(row)
        metadata.update(
            upload_batch_layer_id=layer_id, upload_batch_id=self.batch_id
        )
        LOG.info(f"Metadata: {metadata}")
        return metadata

    def create_catxl(self, layer_id, loss_set, row):
        return Layer(
            type="CatXL",
            description=get_str(row, self.layer_columns.description),
            loss_sets=[loss_set],
            inception_date=get_date(row, self.layer_columns.inception_date),
            expiry_date=get_date(row, self.layer_columns.expiry_date),
            reinstatements=self.get_reinstatements(row),
            premium=self.get_money(
                row,
                self.layer_columns.premium,
                self.layer_columns.premium_ccy,
                0,
            ),
            participation=get_float(row, self.layer_columns.participation),
            attachment=self.get_money(
                row,
                self.layer_columns.attachment,
                self.layer_columns.attachment_ccy,
                0,
            ),
            limit=self.get_money(
                row,
                self.layer_columns.limit,
                self.layer_columns.limit_ccy,
                sys.float_info.max,
            ),
            nth=get_int(row, self.layer_columns.nth) or 1,
            franchise=self.get_money(
                row,
                self.layer_columns.franchise,
                self.layer_columns.franchise_ccy,
                0,
            ),
            meta_data=self.get_metadata(layer_id, row),
        )

    def create_generic(self, layer_id, loss_set, row):
        return Layer(
            type="Generic",
            description=get_str(row, self.layer_columns.description),
            loss_sets=[loss_set],
            inception_date=get_date(row, self.layer_columns.inception_date),
            expiry_date=get_date(row, self.layer_columns.expiry_date),
            reinstatements=self.get_reinstatements(row),
            premium=self.get_money(
                row,
                self.layer_columns.premium,
                self.layer_columns.premium_ccy,
                0,
            ),
            participation=get_float(row, self.layer_columns.participation),
            attachment=self.get_money(
                row,
                self.layer_columns.attachment,
                self.layer_columns.attachment_ccy,
                0,
            ),
            limit=self.get_money(
                row,
                self.layer_columns.limit,
                self.layer_columns.limit_ccy,
                sys.float_info.max,
            ),
            franchise=self.get_money(
                row,
                self.layer_columns.franchise,
                self.layer_columns.franchise_ccy,
                0,
            ),
            aggregate_attachment=self.get_money(
                row,
                self.layer_columns.aggregate_attachment,
                self.layer_columns.aggregate_attachment_ccy,
                0,
            ),
            aggregate_limit=self.get_money(
                row,
                self.layer_columns.aggregate_limit,
                self.layer_columns.aggregate_limit_ccy,
                sys.float_info.max,
            ),
            meta_data=self.get_metadata(layer_id, row),
        )

    def batch_upload(self):

        layer_factories = {
            "catxl": self.create_catxl,
            #            "quotashare": self.create_quotashare,
            #            "aggxl": self.create_aggxl,
            "generic": self.create_generic,
        }

        layers = self.layer_ext.get_layers()

        def create_and_upload(layer_id):
            # Upload the loss set first
            loss_set = self.upload_loss_set(layer_id)

            # Construct layer definition from row
            row = self.layer_ext.get_layer_row(layer_id)
            layer_type = get_str(row, self.layer_columns.layer_type).lower()

            layer = layer_factories[layer_type](layer_id, loss_set, row)
            layer.save()

            LOG.info(f"Created layer {layer}")

            return (layer_id, layer.id, loss_set.id, layer.description)

        with ThreadPoolExecutor(4) as pool:
            results = pool.map(create_and_upload, layers)

        with open("layer_mapping.csv", "w", newline="\n") as out:
            writer = csv.writer(out)
            writer.writerow(
                [
                    "Layer ID",
                    "Loss Set ID",
                    "ARE Layer UUID",
                    "ARE Loss Set UUID",
                    "ARE Layer Description",
                ]
            )
            writer.writerows(
                [
                    (
                        layer_id,
                        # Duplicate this until we have support for multiple loss sets per layer
                        layer_id,
                        are_layer_uuid,
                        are_loss_set_uuid,
                        description,
                    )
                    for (
                        layer_id,
                        are_layer_uuid,
                        are_loss_set_uuid,
                        description,
                    ) in results
                ]
            )

    def __init__(
        self,
        layer_ext: LayerExtractor,
        loss_ext: LossSetExtractor,
        batch_id,
        config,
    ):
        self.layer_ext = layer_ext
        self.loss_ext = loss_ext
        self.batch_id = batch_id
        self.config = config
        self.analysis_profile = AnalysisProfile.retrieve(
            config.defaults.analysis_profile_uuid
        )
        self.layer_columns = self.config.layer_columns
