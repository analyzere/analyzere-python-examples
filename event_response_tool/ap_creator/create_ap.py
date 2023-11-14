import logging
from datetime import datetime
from math import ceil

import analyzere
import pytz

from utils.alert import Alert as alert
from utils.file_handler import find_column
from utils.are_resources import check_resource_upload_status

logger = logging.getLogger()


class AnalysisProfileCreator:
    def __init__(
        self,
        config,
        event_weights_df,
        total_num_of_events,
        trial_count,
        catalog_description,
        simulation_description,
        analysis_profile_description,
        old_analysis_profile_uuid,
    ):
        self.config = config
        self.event_weights_df = event_weights_df
        self.total_num_of_events = (
            int(total_num_of_events) if total_num_of_events else 0
        )
        self.trial_count = int(trial_count) if trial_count else 0
        self.catalog_description = catalog_description
        self.simulation_description = simulation_description
        self.analysis_profile_description = analysis_profile_description
        self.old_analysis_profile_uuid = old_analysis_profile_uuid

    def create_catalog(self):
        events_data = "\n".join(
            [
                "%s,1,%s" % (i, i)
                for i in range(1, self.total_num_of_events + 1)
            ]
        )
        catalog_data = """EventId,Rate,Sequence\n""" + events_data

        try:
            catalog = analyzere.EventCatalog(
                description=self.catalog_description,
                source=self.config.get(
                    "ap_creator", "default_catalog_source"
                ),
            )

            catalog.save()
            catalog.upload_data(catalog_data)
            check_resource_upload_status(catalog)
        except Exception as e:
            alert.exception(f"Exception occured while creating catalog: {e}")
        else:
            if catalog.status == "processing_succeeded":
                alert.info(
                    f"Event Catalog {catalog.id} has been created successfully",
                )
            else:
                alert.error(
                    f"Event Catalog {catalog.id} was uploaded, but failed while processing. {catalog.status_message}"
                )
            return catalog

    def upload_simulation(
        self, event_catalogs, start_date, trial_count, simulation_data
    ):
        try:
            simulation = analyzere.Simulation(
                name=self.simulation_description,
                description=self.simulation_description,
                event_catalogs=event_catalogs,
                start_date=start_date,
                trial_count=trial_count,
            )
            simulation.save()
            simulation.upload_data(simulation_data)
            check_resource_upload_status(simulation)
        except Exception as e:
            alert.exception(
                f"Exception occured while uploading simulation: {e}"
            )
        else:
            if simulation.status == "processing_succeeded":
                alert.info(
                    f"Simulation {simulation.id} has been created successfully"
                )
            else:
                alert.error(
                    f"Simulation {simulation.id} was uploaded, but failed while processing. {simulation.status_message}"
                )
            return simulation

    def build_simulation_data(self):
        alert.debug("Building simulation data")
        event_column = find_column(
            "event", self.event_weights_df.columns.tolist()
        )
        weight_column = find_column(
            "weight", self.event_weights_df.columns.tolist()
        )

        events = self.event_weights_df[event_column].to_list()
        weights = self.event_weights_df[weight_column].to_list()
        max_trials = self.trial_count
        trial_count_by_event = [ceil(max_trials * w) for w in weights]
        trial_id = 1

        simulation_data = """TrialId,EventId,Day\n"""

        for event_id, trial_count in zip(events, trial_count_by_event):
            if event_id < self.total_num_of_events:
                for _ in range(0, trial_count):
                    simulation_data += "%s,%s,%s\n" % (trial_id, event_id, 1)
                    trial_id += 1

        return simulation_data, sum(trial_count_by_event)

    def create_simulation(self, catalogs, start_date):
        simulation_data, trial_count = self.build_simulation_data()
        alert.debug("Simulation data built successfully")

        simulation = self.upload_simulation(
            event_catalogs=catalogs,
            start_date=start_date,
            trial_count=trial_count,
            simulation_data=simulation_data,
        )

        return simulation

    def create_loss_filters(self):
        loss_filters = [
            analyzere.LossFilter(
                type="AnyOfFilter",
                name="Event %s" % (i),
                description="Event %s" % (i),
                attribute="EventId",
                values=[i],
            ).save()
            for i in range(1, self.total_num_of_events + 1)
        ]
        alert.info(f"Created {len(loss_filters)} Loss Filters successfully")

        return loss_filters

    def retrieve_fx_profile(self):
        # Retrieve the latest FX Profile if the default FX profile
        # is not provided in the config
        fx_profile_uuid = self.config.get(
            "ap_creator", "default_fx_profile_id"
        )
        try:
            if fx_profile_uuid:
                alert.info(f"Fetching FX Profile {fx_profile_uuid}")
                return analyzere.ExchangeRateProfile.retrieve(fx_profile_uuid)
            else:
                fx_profile = analyzere.ExchangeRateProfile.list(
                    ordering="-created", limit=1
                )
                alert.info(f"Retrieved latest FX Profile {fx_profile[0].id}")
                return fx_profile[0]
        except Exception as e:
            alert.exception(
                f"Exception occured while retrieving fx_profile: {e}"
            )

    def update_analysis_profile(self):
        # Update the Simulation of the existing event response Analysis Profile
        try:
            alert.info(
                f"Updating Analysis Profile {self.old_analysis_profile_uuid} with new Simulation"
            )
            old_ap = analyzere.AnalysisProfile.retrieve(
                self.old_analysis_profile_uuid
            )
            del old_ap.id
            old_ap.description = self.analysis_profile_description

            new_simulation = self.create_simulation(
                old_ap.event_catalogs, old_ap.simulation.start_date
            )
            old_ap.simulation = new_simulation
            new_ap = old_ap.save()
            check_resource_upload_status(new_ap)
        except Exception as e:
            alert.exception(
                f"Exception occured while updating Analysis Profile {self.old_analysis_profile_uuid}: {e}"
            )
        else:
            if new_ap.status == "processing_succeeded":
                alert.info(
                    f"Successfully updated Analysis Profile {self.old_analysis_profile_uuid}, new Analysis Profile is {new_ap.id}",
                    success=True,
                )
            else:
                alert.error(
                    f"Analysis Profile {new_ap.id} was created, but failed while processing. {new_ap.status_message}"
                )

    def create_analysis_profile(self):
        # Create a new specialized Analysis Profile for event response
        try:
            catalog = self.create_catalog()
            start_date = datetime(datetime.now().year, 1, 1, tzinfo=pytz.utc)
            simulation = self.create_simulation([catalog], start_date)

            analysis_profile = analyzere.AnalysisProfile()
            analysis_profile.event_catalogs = [catalog]
            analysis_profile.simulation = simulation
            analysis_profile.description = self.analysis_profile_description
            analysis_profile.loss_filters = self.create_loss_filters()
            analysis_profile.exchange_rate_profile = (
                self.retrieve_fx_profile()
            )
            analysis_profile.save()
            check_resource_upload_status(analysis_profile)
        except Exception as e:
            alert.exception(
                f"Exception occured while creating Analysis Profile: {e}"
            )
        else:
            if analysis_profile.status == "processing_succeeded":
                alert.info(
                    f"Successfully created Analysis Profile {analysis_profile.id}",
                    success=True,
                )
            else:
                alert.error(
                    f"Analysis Profile {analysis_profile.id} was created, but failed while processing. {analysis_profile.status_message}"
                )

    def build_analysis_profile(self):
        if self.old_analysis_profile_uuid:
            self.update_analysis_profile()
        else:
            self.create_analysis_profile()
