import json
import os
from datetime import datetime, timezone

from src.connections.http_connector import HttpConnector
from src.common.conbase_utility import ConbaseUtility
from src.common.log import Logger
from src.connections.s3 import S3Connector

class ConbaseIngest:
    """
    This class can be used to retrieve JSON files from the Conbase database and store them in an S3 bucket.
    """

    def __init__(self, http_connector: HttpConnector, config: dict, request_config: dict, s3_connector: S3Connector, url: str, logger: Logger, product_id: str):
        """
        Constructor for the ConbaseIngest class

        :param request_config: Dict containing config for requests to Conbase API
        :param s3_connector: S3Connector object for connection to S3
        """
        self._config = config
        self._http_connector = http_connector
        self._request_config = request_config
        self._today = datetime.date(datetime.now(timezone.utc))
        self._s3_connector = s3_connector
        self._logger = logger
        self._url = url
        self._product_id = product_id

    def fetch_product_ids(self):
        """
        Execute the ingest of Conbase data in three steps:
            Step 1 - POST request to Conbase API
            Step 2 - Validate received data to make sure it is valid JSON
            Step 3 - Returns the received data as a list
        """
        self._logger.info('Start of fetch product ids method')
        response = self._http_connector.get(self._url, headers=self._request_config)
        if not response.ok:
            self._logger.debug(f'Response:\n  {response.text}')
            raise ConnectionError('Connection to CONBASE Service failed!')

        self._logger.info('getting response as a text')
        data = response.text
        self._logger.info('Received converted as a text')
        try:
            baseline_overview = json.loads(data)
            content_of_baseline = baseline_overview.get("content")
            uuids = [a_dict["uuid"] for a_dict in content_of_baseline]
        except ValueError as err:
            raise ConnectionError(f'No valid JSON data received! Received content:\n{data}') from err

        valid_uuids = []
        failed_uuids = []
        for uuid in uuids:
            response = self._http_connector.get(f"{self._url}/{uuid}", headers=self._request_config)
            if response.status_code not in [404, 426, 500]:
                valid_uuids.append(uuid)
            else:
                failed_uuids.append(uuid)
                self._logger.info(f'Skipping UUID {uuid} due to response code {response.status_code}')

        self._logger.info(f'Failed UUIDs: {failed_uuids}')
        self._logger.info('End of fetch product ids method')
        return valid_uuids

    def ingest(self, uuid):
        """
        Execute the ingest of Conbase data in two steps:
            Step 1 - POST request to Conbase API
            Step 2 - Validate received data to make sure it is valid JSON
            Step 3 - Write received data to S3
        :return:
        """
        self._logger.info('Start of ingest method')
        response = self._http_connector.get(f"{self._url}/{uuid}", headers=self._request_config)
        self._logger.info('Received the response')
        if not response.ok:
            self._logger.debug(f'Response:\n{response.text}')
            if response.status_code in [404, 426, 500]:
                self._logger.info(f'Skipping UUID {uuid} due to response code {response.status_code}')
                self._logger.info(f'Failed UUID: {uuid}')
                return
            raise ConnectionError('Connection to CONBASE Service failed!')

        self._logger.debug('Getting response:')
        data = response.text
        self._logger.debug(f'Response:\n, {data}')
        try:
            json.loads(data)
        except ValueError as err:
            raise ConnectionError(f'No valid JSON data received! Received content:\n{data}') from err

        self._logger.info('Writing data to S3')
        self._s3_connector.write(data=data, key=f"{self._config.get('data').get('lz').get('conbase_folder')}{str(self._today)}/{self._product_id}_{str(self._today)}_{str(uuid)}")