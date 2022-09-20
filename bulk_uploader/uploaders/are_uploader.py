from re import A
import sys
import csv
from concurrent.futures import ThreadPoolExecutor
from io import StringIO

from analyzere import (
    LossSet,
    Layer,
    AnalysisProfile,
    MonetaryUnit,
    Reinstatement,
)

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

    The main steps performanced by this class are:
        * Iterate over the list of layers in parallel
        * For each layer, upload its correponding loss set
        * Create a new Analyze Re layer object representing the layer
        * Creating a CSV file with mappings from uploaded Layer to Loss UUID
    """
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
            start_date=self.config.defaults.start_date,
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

        # Get the sorted loss set data first
        data = self.loss_ext.get_loss_set(layer_id)
        
        # Ensure the loss set has data
        if len(data) == 0:
            return None
        
        factory = loss_set_factories[self.loss_ext.loss_type]
        loss_set = factory(layer_id)
        loss_set.save()

        
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
        return metadata

    def create_catxl(self, layer_id, loss_sets, row):
        return Layer(
            type="CatXL",
            description=get_str(row, self.layer_columns.description),
            loss_sets=loss_sets,
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

    def create_quotashare(self, layer_id, loss_sets, row):
        return Layer(
            type="QuotaShare",
            description=get_str(row, self.layer_columns.description),
            loss_sets=loss_sets,
            inception_date=get_date(row, self.layer_columns.inception_date),
            expiry_date=get_date(row, self.layer_columns.expiry_date),
            premium=self.get_money(
                row,
                self.layer_columns.premium,
                self.layer_columns.premium_ccy,
                0,
            ),
            participation=get_float(row, self.layer_columns.participation),
            event_limit=self.get_money(
                row,
                self.layer_columns.event_limit,
                self.layer_columns.event_limit_ccy,
                0,
            ),
            meta_data=self.get_metadata(layer_id, row),
        )

    def create_aggxl(self, layer_id, loss_sets, row):
        return Layer(
            type="AggXL",
            description=get_str(row, self.layer_columns.description),
            loss_sets=loss_sets,
            inception_date=get_date(row, self.layer_columns.inception_date),
            expiry_date=get_date(row, self.layer_columns.expiry_date),
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

    def create_generic(self, layer_id, loss_sets, row):
        return Layer(
            type="Generic",
            description=get_str(row, self.layer_columns.description),
            loss_sets=loss_sets,
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
            "quotashare": self.create_quotashare,
            "aggxl": self.create_aggxl,
            "generic": self.create_generic,
        }

        layers = self.layer_ext.get_layers()

        def create_and_upload(layer_id):
            # Upload the loss set first
            loss_set = self.upload_loss_set(layer_id)

            # If loss sets are empty we don't upload them and don't have a
            # reference.
            loss_sets = [loss_set] if loss_set else []

            # Construct layer definition from row
            row = self.layer_ext.get_layer_row(layer_id)
            layer_type = get_str(row, self.layer_columns.layer_type).lower()

            layer = layer_factories[layer_type](layer_id, loss_sets, row)
            layer.save()

            LOG.info(f"Created layer {layer}")

            return (
                layer_id, 
                layer.id, 
                [ls.id for ls in loss_sets], 
                layer.description
            )

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
                        ';'.join(are_loss_set_uuids),
                        description,
                    )
                    for (
                        layer_id,
                        are_layer_uuid,
                        are_loss_set_uuids,
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
