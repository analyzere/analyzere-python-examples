import analyzere
import multiprocessing
import sys
import csv

from analyzere import InvalidRequestError
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
import pandas as pd


class BulkUploader:
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
            if loss_set.status in ['processing_failed',
                                   'processing_succeeded']:
                break
        return loss_set.status, loss_set.status_message

    def retrieve_loss_metadata_from_layer_terms(self, loss_set_id):
        layer_row = self.layers_df.loc[self.layers_df[self.bulk_layer_term_columns.layer_id] == loss_set_id]
        currency = None
        start_date = None

        if self.bulk_layer_term_columns.loss_set_currency in self.layers_df.columns:
            currency = layer_row[self.bulk_layer_term_columns.loss_set_currency].values[0]
    
        if self.bulk_layer_term_columns.loss_set_start_date in self.layers_df.columns:
            start_date = layer_row[self.bulk_layer_term_columns.loss_set_start_date].values[0]

        return currency, start_date

    def sort_loss_data(self, loss_df, columns):
        sorted_loss_df = loss_df.sort_values(by=columns)
        return sorted_loss_df

    def _upload_ylt(self, loss_df, unique_loss_set_name):
        try:
            trial_id = self.loss_set_columns.trial_id
            loss = self.loss_set_columns.loss
            reinstatement_premium = self.loss_set_columns.reinstatement_premium
            reinstatement_brokerage = self.loss_set_columns.reinstatement_brokerage
            has_reinstatements = False

            # Sort the YLT dataframe
            sorted_loss_df = self.sort_loss_data(loss_df, [trial_id])

            column_list = [trial_id, loss]

            if reinstatement_premium in loss_df.columns and \
                    reinstatement_brokerage in loss_df.columns:
                column_list.append(reinstatement_premium)
                column_list.append(reinstatement_brokerage)
                has_reinstatements = True

            # Write Loss DataFrame contents to a string
            loss_data_str = self.write_loss_data_to_string(sorted_loss_df, column_list)

            # Fetch LossSet Currency and StartDate from Layer terms file if they exist
            currency, start_date = self.retrieve_loss_metadata_from_layer_terms(unique_loss_set_name)

            # Set default values for currency and start_date if the user hasn't provided them
            if not currency:
                currency = self.defaults.default_currency
            if not start_date:
                start_date = self.defaults.default_start_date

            trial_count = self.defaults.default_trial_count

            # Upload the YLT
            loss_set = analyzere.LossSet(type='YLTLossSet',
                                         description=unique_loss_set_name,
                                         start_date=start_date,
                                         currency=currency,
                                         loss_type='LossNetOfAggregateTerms' if has_reinstatements else self.loss_position,
                                         trial_count=trial_count).save()

            loss_set.upload_data(loss_data_str)

            status, status_msg = self.check_loss_set_upload_status(loss_set)

            return loss_set.id, loss_set.description, status, status_msg, unique_loss_set_name

        except Exception as e:
            raise Exception('Error occurred while uploading LossSet {}: {}'.format(unique_loss_set_name,
                                                                                   e))

    def _upload_yelt(self, loss_df, unique_loss_set_name):
        try:
            trial_id = self.loss_set_columns.trial_id
            event_id = self.loss_set_columns.event_id
            sequence = self.loss_set_columns.sequence
            loss = self.loss_set_columns.loss
            reinstatement_premium = self.loss_set_columns.reinstatement_premium
            reinstatement_brokerage = self.loss_set_columns.reinstatement_brokerage
            has_reinstatements = False

            # Sort the YELT dataframe
            sorted_loss_df = self.sort_loss_data(loss_df, [trial_id, sequence, event_id])

            column_list = [trial_id, event_id, sequence, loss]

            if reinstatement_premium in loss_df.columns and \
                    reinstatement_brokerage in loss_df.columns:
                column_list.append(reinstatement_premium)
                column_list.append(reinstatement_brokerage)
                has_reinstatements = True

            # Write Loss DataFrame contents to a string
            loss_data_str = self.write_loss_data_to_string(sorted_loss_df, column_list)

            # Fetch LossSet Currency and StartDate from Layer terms file if they exist
    
            currency, start_date = self.retrieve_loss_metadata_from_layer_terms(unique_loss_set_name)

            # Set default values for currency and start_date if the user hasn't provided them
            if not currency:
                currency = self.defaults.default_currency
            if not start_date:
                start_date = self.defaults.default_start_date

            trial_count = self.defaults.default_trial_count


            # Upload the YELT
            loss_set = analyzere.LossSet(type='YELTLossSet',
                                         description=unique_loss_set_name,
                                         event_catalogs=[self.catalog],
                                         start_date=start_date,
                                         currency=currency,
                                         loss_type='LossNetOfAggregateTerms' if has_reinstatements else self.loss_position,
                                         trial_count=trial_count).save()

            loss_set.upload_data(loss_data_str)

            status, status_msg = self.check_loss_set_upload_status(loss_set)

            return loss_set.id, loss_set.description, status, status_msg, unique_loss_set_name

        except Exception as e:
            raise Exception('Error occurred while uploading LossSet {}: {}'.format(unique_loss_set_name,
                                                                                   e))

    def _upload_elt(self, loss_df, unique_loss_set_name):
        try:
            event_id = self.loss_set_columns.event_id
            loss = self.loss_set_columns.loss

            column_list = [event_id, loss]

            # Write Loss DataFrame contents to a string
            loss_data_str = self.write_loss_data_to_string(loss_df, column_list)

            # Fetch LossSet Currency from Layer terms file if it exists
            currency, _ = self.retrieve_loss_metadata_from_layer_terms(unique_loss_set_name)

            # Set default values for currency if the user hasn't provided one
            if not currency:
                currency = self.defaults.default_currency

            # Upload the ELT
            loss_set = analyzere.LossSet(type='ELTLossSet',
                                         description=unique_loss_set_name,
                                         event_catalogs=[self.catalog],
                                         currency=currency,
                                         loss_type=self.loss_position).save()

            loss_set.upload_data(loss_data_str)

            status, status_msg = self.check_loss_set_upload_status(loss_set)

            return loss_set.id, loss_set.description, status, status_msg, unique_loss_set_name

        except Exception as e:
            raise Exception('Error occurred while uploading LossSet {}: {}'.format(unique_loss_set_name,
                                                                                   e))

    def split_loss_sets(self, unique_loss_set_name):
        ls_df = self.loss_df[self.loss_df
                             [self.loss_set_columns.loss_set_id] == unique_loss_set_name]
        ls_uuid = ls_desc = status = status_msg = ls_id = None
        if self.loss_type.lower() == 'elt':
            ls_uuid, ls_desc, status, status_msg, ls_id = self._upload_elt(ls_df, unique_loss_set_name)
        if self.loss_type.lower() == 'yelt':
            ls_uuid, ls_desc, status, status_msg, ls_id = self._upload_yelt(ls_df, unique_loss_set_name)
        if self.loss_type.lower() == 'ylt':
            ls_uuid, ls_desc, status, status_msg, ls_id = self._upload_ylt(ls_df, unique_loss_set_name)

        return ls_uuid, ls_desc, status, status_msg, ls_id

    def process_loss_df(self):
        print('Uploading LossSets')
        self.loss_mapping = {}
        self.uploaded_loss_set_details = {}

        unique_loss_sets = self.loss_df[self.loss_set_columns.loss_set_id].unique().tolist()

        processed = 0
        print('Splitting LossSets')
        with ThreadPoolExecutor(multiprocessing.cpu_count()) as executor:
            for ls_uuid, ls_desc, status, status_msg, ls_id in executor.map(self.split_loss_sets,
                                                                            unique_loss_sets):
                processed += 1
                self.uploaded_loss_set_details[ls_id] = [ls_desc, status, status_msg]
                self.loss_mapping[ls_id] = ls_uuid

        print(f'Uploaded {len(self.loss_mapping)} LossSets')

    def get_are_resources(self):
        self.profile = analyzere.AnalysisProfile.retrieve(self.analysis_profile_uuid)
        self.catalog = self.profile.event_catalogs[0]

    def get_loss_set(self, row):
        loss_id = self.bulk_layer_term_columns.layer_id
        try:
            loss_uuid = self.loss_mapping[row[loss_id]]
            loss_set = analyzere.LossSet.retrieve(loss_uuid)
            return loss_set
        except KeyError as e:
            print('Not able to find associated LossSet for Layer {}'.format(loss_id))
        except Exception as e:
            print(e)

    def get_layer_term_value(self, row, layer_term):
        value = None
        if layer_term in self.layers_df.columns:
            value = row[layer_term]
        if layer_term == self.bulk_layer_term_columns.franchise:
            if not layer_term in self.layers_df.columns:
                value = analyzere.MonetaryUnit(0, 'USD')
        return value

    def _upload_qs(self, row):
        try:
            inception = self.get_layer_term_value(row, self.bulk_layer_term_columns.inception)
            expiry = self.get_layer_term_value(row, self.bulk_layer_term_columns.expiry)
            participation = self.get_layer_term_value(row, self.bulk_layer_term_columns.participation)
            premium = self.get_layer_term_value(row, self.bulk_layer_term_columns.premium)
            event_limit = self.get_layer_term_value(row, self.bulk_layer_term_columns.event_limit)
            description = self.get_layer_term_value(row, self.bulk_layer_term_columns.description)
            meta_data = self.get_layer_term_value(row, self.bulk_layer_term_columns.meta_data)

            # Retrieve associated LossSet
            loss_set = self.get_loss_set(row)

            return analyzere.Layer(type='QuotaShare',
                                   description=description,
                                   loss_sets=[loss_set],
                                   inception_date=inception,
                                   expiry_date=expiry,
                                   premium=premium,
                                   participation=participation,
                                   event_limit=event_limit,
                                   meta_data=meta_data).save()

        except Exception as e:
            raise Exception('Error uploading Layer {}: {}'.format(row[self.bulk_layer_term_columns.layer_id],
                                                                  e))

    def _upload_aggxl(self, row):
        try:
            inception = self.get_layer_term_value(row, self.bulk_layer_term_columns.inception)
            expiry = self.get_layer_term_value(row, self.bulk_layer_term_columns.expiry)
            participation = self.get_layer_term_value(row, self.bulk_layer_term_columns.participation)
            premium = self.get_layer_term_value(row, self.bulk_layer_term_columns.premium)
            attachment = self.get_layer_term_value(row, self.bulk_layer_term_columns.attachment)
            limit = self.get_layer_term_value(row, self.bulk_layer_term_columns.limit)
            agg_att = self.get_layer_term_value(row, self.bulk_layer_term_columns.aggregate_attachment)
            agg_limit = self.get_layer_term_value(row, self.bulk_layer_term_columns.aggregate_limit)
            franchise = self.get_layer_term_value(row, self.bulk_layer_term_columns.franchise)
            description = self.get_layer_term_value(row, self.bulk_layer_term_columns.description)
            meta_data = self.get_layer_term_value(row, self.bulk_layer_term_columns.meta_data)

            # Retrieve associated LossSet
            loss_set = self.get_loss_set(row)

            return analyzere.Layer(type='AggXL',
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
                                   meta_data=meta_data).save()

        except Exception as e:
            raise Exception('Error uploading Layer {}: {}'.format(row[self.bulk_layer_term_columns.layer_id],
                                                                  e))

    def _upload_catxl(self, row):
        try:
            inception = self.get_layer_term_value(row, self.bulk_layer_term_columns.inception)
            expiry = self.get_layer_term_value(row, self.bulk_layer_term_columns.expiry)
            participation = self.get_layer_term_value(row, self.bulk_layer_term_columns.participation)
            premium = self.get_layer_term_value(row, self.bulk_layer_term_columns.premium)
            attachment = self.get_layer_term_value(row, self.bulk_layer_term_columns.attachment)
            limit = self.get_layer_term_value(row, self.bulk_layer_term_columns.limit)
            franchise = self.get_layer_term_value(row, self.bulk_layer_term_columns.franchise)
            reinstatements = self.get_layer_term_value(row, self.bulk_layer_term_columns.reinstatements)

            nth = None
            if self.get_layer_term_value(row, self.bulk_layer_term_columns.nth) is None:
                nth = 1
            else:
                nth = self.get_layer_term_value(row, self.bulk_layer_term_columns.nth)
        
            description = self.get_layer_term_value(row, self.bulk_layer_term_columns.description)
            meta_data = self.get_layer_term_value(row, self.bulk_layer_term_columns.meta_data)

            # Retrieve associated LossSet
            loss_set = self.get_loss_set(row)

            return analyzere.Layer(type='CatXL',
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
                                   meta_data=meta_data).save()


        except Exception as e:
            raise Exception('Error uploading Layer {}: {}'.format(row[self.bulk_layer_term_columns.layer_id],
                                                                  e))

    def _upload_generic(self, row):
        try:
            inception = self.get_layer_term_value(row, self.bulk_layer_term_columns.inception)
            expiry = self.get_layer_term_value(row, self.bulk_layer_term_columns.expiry)
            participation = self.get_layer_term_value(row, self.bulk_layer_term_columns.participation)
            premium = self.get_layer_term_value(row, self.bulk_layer_term_columns.premium)
            attachment = self.get_layer_term_value(row, self.bulk_layer_term_columns.attachment)
            limit = self.get_layer_term_value(row, self.bulk_layer_term_columns.limit)
            agg_att = self.get_layer_term_value(row, self.bulk_layer_term_columns.aggregate_attachment)
            agg_limit = self.get_layer_term_value(row, self.bulk_layer_term_columns.aggregate_limit)
            franchise = self.get_layer_term_value(row, self.bulk_layer_term_columns.franchise)
            reinstatements = self.get_layer_term_value(row, self.bulk_layer_term_columns.reinstatements)
            description = self.get_layer_term_value(row, self.bulk_layer_term_columns.description)
            meta_data = self.get_layer_term_value(row, self.bulk_layer_term_columns.meta_data)

            # Retrieve associated LossSet
            loss_set = self.get_loss_set(row)

            return analyzere.Layer(type='Generic',
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
                                   meta_data=meta_data).save()

        except Exception as e:
            raise Exception('Error uploading Layer {}: {}'.format(row[self.bulk_layer_term_columns.layer_id],
                                                                  e))

    def process_layer_record(self, layer_row):
        layer_type_column_key = self.bulk_layer_term_columns.layer_type
        if layer_type_column_key in self.layers_df:
            layer_type = layer_row[layer_type_column_key]
            layer_type = layer_type.replace(' ', '').lower() if layer_type is not None else 'generic'
        else:
            layer_type = 'generic'

        if layer_type == 'generic':
            layer = self._upload_generic(layer_row)
        elif layer_type == 'aggxl':
            layer = self._upload_aggxl(layer_row)
        elif layer_type == 'catxl':
            layer = self._upload_catxl(layer_row)
        elif layer_type == 'quotashare' or layer_type == 'qs':
            layer = self._upload_qs(layer_row)

        self.layer_mapping[layer.id] = [layer.description, layer.loss_sets]

    def process_layer_df(self):
        print('Uploading Layers')
        self.layer_mapping = {}
        self.layers_df.apply(self.process_layer_record, axis=1)
        if len(self.layer_mapping) > 0:
            print(f'Uploaded {len(self.layer_mapping)} Layers')
        else:
            print('No layers were uploaded')

    def get_config_values(self):
        self.defaults = self.config_file_parser.get_defaults()
        self.bulk_layer_term_columns = self.config_file_parser.get_layer_term_columns()
        self.loss_set_columns = self.config_file_parser.get_ARe_loss_set_columns()

    def bulk_upload(self):
        try:
            self.get_config_values()
            self.get_are_resources()
            self.process_loss_df()
            self.process_layer_df()
        except InvalidRequestError as e:
            sys.exit(e)
        except Exception as e:
            sys.exit('Error occurred while processing layers: {}'.format(e))

    def write_output_files(self):
        file_name = 'layer_mapping.csv'
        if len(self.layer_mapping) > 0:
            with open(file_name, 'w', newline='\n') as output_file:
                writer = csv.DictWriter(output_file, fieldnames=['Layer UUID',
                                                                 'Description',
                                                                 'LossSets UUID'])
                writer.writeheader()
                for layer_uuid, layer_details in self.layer_mapping.items():
                    layer_desc = layer_details[0]
                    loss_sets = layer_details[1]
                    loss_sets_str = ''
                    if len(loss_sets) == 1:
                        loss_sets_str = str(loss_sets[0].id)

                    if len(loss_sets) > 1:
                        loss_sets_str = str(loss_sets[0].id)
                        for index, ls in enumerate(loss_sets):
                            if index == 0:
                                continue
                            loss_sets_str += f';{ls.id}'
                    writer.writerow({'Layer UUID': layer_uuid,
                                     'Description': layer_desc,
                                     'LossSets UUID': loss_sets_str})

            print(f'Mapping details can be found in {file_name} file')

    def __init__(self, layers_df, loss_df, loss_type, loss_position,
                 analysis_profile_uuid, analyzere, config_file_parser):
        self.layers_df = layers_df
        self.loss_df = loss_df
        self.loss_type = loss_type
        self.loss_position = loss_position
        self.analysis_profile_uuid = analysis_profile_uuid
        self.analyzere = analyzere
        self.config_file_parser = config_file_parser
