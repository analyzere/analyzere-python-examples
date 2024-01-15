import sys
import multiprocessing
import logging
from concurrent.futures import ThreadPoolExecutor

import analyzere
from analyzere.base_resources import Reference
from analyzere import MonetaryUnit
import ipywidgets as widgets
from IPython.display import display, FileLink

from utils.alert import Alert as alert
from utils.are_resources import check_resource_upload_status
from utils.file_handler import (
    read_input_file,
    write_output_file,
    find_column,
    join,
    read_byte_stream_into_csv,
)


logger = logging.getLogger()


class LayerLossDuplicator:
    def __init__(
        self,
        output_dir,
        event_weights_df,
        config,
        analysis_profile_uuid,
        layer_ids_csv=None,
        portfolio_uuid=None,
    ):
        self.output_dir = output_dir
        self.event_weights_df = event_weights_df
        self.analysis_profile = self.retrieve_analysis_profile(
            analysis_profile_uuid
        )
        self.event_catalogs = self.analysis_profile.event_catalogs
        self.layer_ids_csv = layer_ids_csv
        self.portfolio_uuid = portfolio_uuid
        self.config = config

        self.layer_list = []  # List of LayerViews
        self.loss_set_mapping = {}  # Old LossSet UUID : New LossSet UUID

    def retrieve_analysis_profile(self, ap_uuid):
        try:
            analysis_profile = analyzere.AnalysisProfile.retrieve(ap_uuid)
        except Exception as e:
            alert.exception(
                f"Exception occured while retrieving Analysis Profile {ap_uuid}: {e}"
            )
        else:
            alert.info(
                f"Using Analysis Profile {ap_uuid} for updating LayerViews and LossSets"
            )
            return analysis_profile

    def extract_layers(self):
        if (
            not self.layer_ids_csv is None
            and len(str(self.layer_ids_csv)) > 0
        ):
            # get the list of LayerViews from the input CSV
            layer_list_df = read_input_file(self.layer_ids_csv)
            layer_column = find_column(
                "layer", layer_list_df.columns.tolist()
            )
            self.layer_list = layer_list_df[layer_column].unique().tolist()
        else:
            if not self.portfolio_uuid is None:
                # get the list of LayerViews from the PortfolioView UUID
                try:
                    are_portfolio = analyzere.PortfolioView.retrieve(
                        self.portfolio_uuid
                    )
                except Exception as e:
                    alert.exception(
                        f"Could not retrieve PortfolioView {self.portfolio_uuid}: {e}"
                    )
                else:
                    self.layer_list = [
                        layer_view.id
                        for layer_view in are_portfolio.layer_views
                    ]
                    alert.info(
                        f"Fetched {len(self.layer_list)} LayerView from PortfolioView {self.portfolio_uuid}"
                    )

    # Generic Layer to replace filter layer in structure
    def replace_filter_layer(self, loss_sets):
        unlimited = sys.float_info.max
        currency = self.config.get("defaults", "currency")
        generic_layer = analyzere.Layer(
            type="Generic",
            description="Generic Layer",
            loss_sets=[loss_sets],
            participation=1,
            aggregate_attachment=MonetaryUnit(0, currency),
            aggregate_limit=MonetaryUnit(unlimited, currency),
            attachment=MonetaryUnit(0, currency),
            franchise=MonetaryUnit(0, currency),
            limit=MonetaryUnit(unlimited, currency),
        ).save()
        return generic_layer

    def scale_elt(self, elt_df, loss_set):
        alert.debug(f"Scaling loss_set {loss_set.id}")
        # Normalize the EventId column
        event_id_column_in_elt = find_column("event", elt_df.columns.tolist())
        event_id_column_in_weights = find_column(
            "event", self.event_weights_df.columns.tolist()
        )
        self.event_weights_df = self.event_weights_df.rename(
            columns={event_id_column_in_weights: event_id_column_in_elt}
        )

        # Perform left-join of weights table and loss set table.
        # Only events that occur in the weights table will remain.
        weighted_elt_df = join(
            self.event_weights_df,
            elt_df,
            how="left",
            on=event_id_column_in_elt,
        )
        weighted_elt_df = weighted_elt_df.rename(
            mapper=lambda name: name.upper(), axis=1
        )
        weighted_elt_df.dropna(inplace=True)

        try:
            if len(weighted_elt_df.columns) < 6:
                # ELT without secondary uncertainty (maybe AIR)
                alert.debug(
                    f"ELT {loss_set.id} without secondary uncertainty"
                )
                # Scale mean loss value
                weighted_elt_df["LOSS"] = (
                    weighted_elt_df.LOSS * weighted_elt_df.WEIGHT
                )
                weighted_elt_df["STDDEVC"] = 0
                weighted_elt_df["STDDEVI"] = 0
                # Set exposure value equal to the scaled loss
                weighted_elt_df["EXPVALUE"] = (
                    weighted_elt_df.LOSS * weighted_elt_df.WEIGHT
                )
                # Remove EventID and Weight columns
                weighted_elt_df = weighted_elt_df.drop(
                    ["EVENTID", "WEIGHT"], axis=1
                )
                # Set EventId for all entries to 1
                # NOTE: The platform will automatically combine multiple entries with the same event ID into a single occurrence.
                weighted_elt_df["EVENTID"] = 1
            else:
                # ELT with secondary uncertainty (likely RMS)
                alert.debug(f"ELT {loss_set.id} with secondary uncertainty")
                # Scale mean loss value
                weighted_elt_df["PERSPVALUE"] = (
                    weighted_elt_df.PERSPVALUE * weighted_elt_df.WEIGHT
                )
                # Scale independent standard deviation
                weighted_elt_df["STDDEVI"] = (
                    weighted_elt_df.STDDEVI * weighted_elt_df.WEIGHT
                )
                # Scale correlated standard deviation
                weighted_elt_df["STDDEVC"] = (
                    weighted_elt_df.STDDEVC * weighted_elt_df.WEIGHT
                )
                # Scale exposure value
                weighted_elt_df["EXPVALUE"] = (
                    weighted_elt_df.EXPVALUE * weighted_elt_df.WEIGHT
                )
                # Remove EventID and Weight columns
                weighted_elt_df = weighted_elt_df.drop(
                    ["EVENTID", "WEIGHT"], axis=1
                )
                # Set EventId for all entries to 1
                # NOTE: The platform will automatically combine multiple entries with the same event ID into a single occurrence.
                weighted_elt_df["EVENTID"] = 1
        except Exception as e:
            alert.exception(
                f"Exception occured while scaling Loss Set {loss_set.id}: {e}"
            )
        else:
            return weighted_elt_df

    def transform_loss_set(self, loss_set):
        alert.debug(f"Transforming loss_set {loss_set.id}")
        old_elt_data = loss_set.download_data()
        elt_df = read_byte_stream_into_csv(old_elt_data)
        # Scale and transform loss set data and save into string buffer
        scaled_df = self.scale_elt(elt_df, loss_set)
        if scaled_df is not None:
            new_description = f"ER_{loss_set.description}"
            new_loss_set = self.upload_elt(
                new_description,
                scaled_df.to_csv(index=False),
                loss_set.currency,
                self.event_catalogs,
            )
            return new_loss_set

    def upload_elt(self, description, elt, currency, catalogs):
        try:
            loss_set = analyzere.LossSet(
                type="ELTLossSet",
                description=description,
                event_catalogs=catalogs,
                currency=currency,
                loss_type=self.config.get("defaults", "loss_perspective"),
            ).save()
            loss_set.upload_data(elt)
            check_resource_upload_status(loss_set)
        except Exception as e:
            alert.exception(
                f"Exception occured while uploading ELT {loss_set.id}: {e}"
            )
        if loss_set.status == "processing_succeeded":
            alert.debug(f"Uploaded loss_set {loss_set.id}")
        else:
            alert.error(
                f"LossSet {loss_set.id} was uploaded, but failed while processing. {loss_set.status_message}"
            )

        return loss_set

    def modify_layer(self, layer):
        # Recursively process the layer structure and transform loss sets
        if hasattr(layer, "ref_id"):
            layer = analyzere.LayerView.retrieve(layer.ref_id).layer
        if hasattr(layer, "analysis_profile"):
            layer = layer.layer
        if type(layer) in [analyzere.LayerView, Reference]:
            layer = layer.layer
        if layer.type == "BackAllocatedLayer":
            logger.exception("Back Allocated layers are not supported")
            raise

        if layer.type == "NestedLayer":
            layer.sink = self.modify_layer(layer.sink)
            layer.sources = [self.modify_layer(s) for s in layer.sources]

        if hasattr(layer, "loss_sets"):
            try:
                # no losses to process
                if len(layer.loss_sets) == 0 and layer.type != "FilterLayer":
                    pass

                # Transform the loss sets in the loss set list in-place
                loss_sets = []

                for _, loss_set in enumerate(layer.loss_sets):
                    # Check if loss set is already processed
                    if loss_set.id in self.loss_set_mapping:
                        loss_sets.append(
                            analyzere.LossSet.retrieve(
                                self.loss_set_mapping[loss_set.id]
                            )
                        )
                    # Transform only ELTs
                    elif loss_set.type == "ELTLossSet":
                        transformed = self.transform_loss_set(loss_set)
                        if transformed:
                            self.loss_set_mapping[
                                loss_set.id
                            ] = transformed.id
                            loss_sets.append(transformed)

                    # If unknown loss set, skip it.
                    else:
                        alert.warning(
                            f"Encountered {loss_set.type} {loss_set.id}, skipping transformation"
                        )
                        loss_sets.append(loss_set)

                # Need to replace Filter Layers with unlimited Generic layer
                if layer.type == "FilterLayer":
                    layer = self.replace_filter_layer(loss_sets)
                else:
                    layer.loss_sets = loss_sets
            except Exception as e:
                alert.exception(f"Unable to process layer: {e}")
        return layer

    def process_layer(self, layer_uuid):
        try:
            old_layer_view = analyzere.LayerView.retrieve(layer_uuid)
            # Update LayerView
            new_layer_view = analyzere.LayerView(
                analysis_profile=self.analysis_profile,
                layer=self.modify_layer(old_layer_view),
            ).save()

            # Create PortfolioViews for aiding the computation of
            # share applied output metrics
            new_portfolio_view = analyzere.PortfolioView(
                analysis_profile=self.analysis_profile,
                layer_views=[new_layer_view],
            ).save()

            old_portfolio_view = analyzere.PortfolioView(
                analysis_profile=old_layer_view.analysis_profile,
                layer_views=[old_layer_view],
            ).save()

        except Exception as e:
            alert.exception(
                f"Exception occurred while modifying Layer {layer_uuid}: {e}"
            )
        else:
            return (
                old_layer_view.id,
                old_layer_view.el(),
                old_portfolio_view.el(),
                new_layer_view.id,
                new_layer_view.el(),
                new_portfolio_view.el(),
            )

    def display_links(self):
        output_file_path = f"{self.output_dir}/results.csv"
        print()
        display(
            FileLink(
                output_file_path,
                result_html_prefix="Click on the link to download the results: ",
            )
        )

    def write_results(self, results):
        column_names = [
            "Old LayerView UUID",
            "Old LayerView EL (100%)",
            "Old LayerView EL (Share Applied)",
            "New LayerView UUID",
            "New LayerView EL (100%)",
            "New LayerView EL (Share Applied)",
        ]
        results = write_output_file(
            results, column_names, "results.csv", self.output_dir
        )
        alert.info(
            f"Duplication successful!",
            success=True,
        )
        self.display_links()

    def modify_layer_loss_data(self):
        self.extract_layers()

        alert.info(f"Processing {len(self.layer_list)} Layers")
        with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
            results = list(executor.map(self.process_layer, self.layer_list))

        self.write_results(results)
