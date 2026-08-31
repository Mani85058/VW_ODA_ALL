import os

from src.common.log import Logger
from src.connections.http_connector import HttpConnector
from src.ingestion.conbase_ingest import ConbaseIngest
from src.connections.s3 import S3Connector

class ConbaseHelper:
    """
    This class can be used to retrieve product ids and uuids from the Conbase API and store product informations in an S3 bucket.
    """

    def __init__(self, product_ids: list, config: dict, baseline_overview_url: str, baseline_url:str, logger: Logger, verify: str):
        self._product_ids = product_ids
        self._config = config
        self._baseline_overview_url = baseline_overview_url
        self._baseline_url = baseline_url
        self._logger = logger
        self._verify = verify

    def vehicle_information_collector(self):
        """
        Execute the Vehicle Info Collector of Conbase data in two steps:
            Step 1 - Build a s3 connector
            Step 2 - POST request to Conbase API with baseline overview url
            Step 3 - Returns the received data(uuid) as a list
            Step 4 - POST request to Conbase API with baseline url
            Step 5 - Write received data to S3 via ingest method
        """
        self._logger.info('instantiating S3Connector for ConbaseIngest...')
        s3_connector = S3Connector(key_id=self._config.get("s3").get("aws_access_key_id"),
                               secret_key=self._config.get("s3").get("aws_secret_access_key"),
                               endpoint_url=self._config.get("s3").get("endpoint_url"),
                               bucket=self._config.get("data").get("lz").get("landing_zone_bucket"))
        self._logger.info("Read product ids from S3..")

        for product_id in self._product_ids:
            self._logger.info('instantiating HttpConnector for Conbase fetch_product_ids for product_id: '+product_id)
            http_baseline_overview = self._baseline_overview_url.replace("{vpId}", product_id).replace("{requestParameters}", "productid="+product_id).replace("{api_version}",
            self._config.get("http").get("api_version_baseline_overview"))
            http_connector = HttpConnector(cert=self._config.get("conbase_cert"), verify= self._verify, url=http_baseline_overview, logger=self._logger)
            self._logger.info('instantiating ConbaseIngest...')
            conbase_ingest = ConbaseIngest(http_connector=http_connector, config=self._config, request_config={}, s3_connector=s3_connector, url=http_baseline_overview,
             logger=self._logger, product_id=product_id)
            self._logger.info('starting ingestion...')
            conbase_uuids = conbase_ingest.fetch_product_ids()
            self._logger.info('Returning the received data as a list')
            self.product_ingestor(conbase_uuids, product_id, s3_connector)
    
    def product_ingestor(self, conbase_uuids, product_id, s3_connector):   
        """
        Execute the Vehicle Info Collector of Conbase data in two steps:
            Step 1 - Build a s3 connector
            Step 2 - POST request to Conbase API with baseline overview url
            Step 3 - Returns the received data(uuid) as a list
            Step 4 - POST request to Conbase API with baseline url
            Step 5 - Write received data to S3 via ingest method
        """
        for uuid in conbase_uuids:
            self._logger.info('instantiating HttpConnector for ConbaseIngest for uuid: '+uuid)
            http_baseline = self._baseline_url.replace("{baseline_uuid}", uuid).replace("{api_version}", self._config.get("http").get("api_version_baseline"))
            http_connector = HttpConnector(cert=self._config.get("conbase_cert"), verify= self._verify, url=http_baseline, logger=self._logger)
            self._logger.info('instantiating ConbaseIngest...')
            conbase_ingest = ConbaseIngest(http_connector=http_connector, config=self._config, request_config={}, s3_connector=s3_connector, url=http_baseline,
            logger=self._logger, product_id=product_id)
            self._logger.info(f'starting ingestion for {uuid}...')
            conbase_ingest.ingest(uuid)
            self._logger.info('Data has been successfully extracted')