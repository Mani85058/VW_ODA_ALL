"""This module serves as an interface for http requests"""
from typing import List
import ssl
import certifi
import requests
from requests_oauthlib import OAuth2Session

from src.common.log import Logger

class HttpConnector:
    """
    This util class can be used to send http requests.
    It requires a certificate for authentication. Optional parameters are certificate verification and a default URL.
    """

    def __init__(self, cert: str = None, verify: str = None, url: str = None, logger: Logger = None):
        """
        Constructor for the HttpConnector class

        :param pkcs12_filename: Certificate for authentication
        :param pkcs12_password: password for certificate
        :param verify: Certificate verification. Deactivated by default
        :param url: Optional URL to use for post requests. None by default
        """
        self._logger = logger
        self._url = url
        self._cert = cert
        if verify:
            self._verify = verify
        else:
            self._verify = False
        self._oauth_session = None

    @staticmethod
    def get_ca_cert():
        """
        Get the path of the ca cert file
        """
        return certifi.where()
    
    def post(self, request_data, url: str = None, json: bool = False) -> requests.Response:
        """
        Make a POST request to some URL

        :param request_data: The request data
        :param url: Optional URL to send post request to. By default saved URL of Connector is used
        :param json: Optional bool parameter specifying whether request data is JSON or not.
        :param is_oauth_session: Optional bool parameter specifying whether oauth session should be used or not.
        :return: The Response of the POST request
        """
        if url is None:
            if self._url is None:
                raise ValueError('no URL provided and no default URL available!')
            url = self._url
        self._logger.info(f'sending POST request to {url}')
        if json:
            if self._oauth_session:
                response = self._oauth_session.post(url=url, json=request_data, cert=self._cert, verify=self._verify)
            else:
                response = requests.post(url=url, json=request_data, cert=self._cert, verify=self._verify)
        else:
            if self._oauth_session:
                response = self._oauth_session.post(url=url, data=request_data, cert=self._cert, verify=self._verify)
                self._logger.info('response created with oauth method')
            else:
                response = requests.post(url=url, data=request_data, cert=self._cert, verify=self._verify)
                self._logger.info('response created without oath session')
        self._logger.info(f'response code: {response.status_code}')
        if not response.ok:
            self._logger.error(response.text)
        return response

    def get(self, url: str, headers: dict = None, timeout: int = 30):
        """
        Make a GET request to some URL

        :param url: URL to get data from
        :param headers: Request headers as dict. None by default
        :param timeout: Request timeout. 30 seconds by default
        :return: The Response of the GET request
        """
        self._logger.info(f'sending GET request to {url}')

        if self._oauth_session:
            response = self._oauth_session.get(url=url, verify=self._verify, timeout=timeout)
            self._logger.info('response received with auth seesion')
        else:
            response = requests.get(url=url, cert=self._cert, verify=self._verify, headers=headers, timeout=timeout)
            self._logger.info('response received without auth seesion')
        self._logger.info(f'response code: {response.status_code}')
        if not response.ok:
            self._logger.error(response.text)
        return response

    def start_token_session(self, client_id: str, client_secret: str, token_refresh_url: str, token_url: str = None,
                            token_request_data: dict = None):
        """
        Start an oauth session to automatically refresh authentication tokens.

        :param client_id: Client ID for authentication with API
        :param client_secret: Client secret for authentication with API
        :param token_refresh_url: URL to request refresh token
        :param token_url: Optional URL to request initial token. If not provided, token_refresh_url is used
        :param token_request_data: Optional dict containing request data to be sent in the initial token request.
        Does not need to contain client_id nor client_secret as they are separate parameters.
        :return:
        """
        self._logger.info('starting oauth session for automatic token refresh')

        if not token_url:
            token_url = token_refresh_url
        if not token_request_data:
            token_request_data = {}
        token_request_data['client_id'] = client_id
        token_request_data['client_secret'] = client_secret
        token = self.__request_initial_token(token_request_data=token_request_data, token_url=token_url)

        self._logger.info('starting oauth session')
        refresh_auth = {'client_id': client_id, 'client_secret': client_secret}
        self._oauth_session = OAuth2Session(client_id, token=token, auto_refresh_url=token_refresh_url,
        auto_refresh_kwargs=refresh_auth, token_updater=self.__log_refresh)

    def __request_initial_token(self, token_request_data: dict, token_url: str) -> dict:
        self._logger.info('requesting initial authentication token')
        response = self.post(request_data=token_request_data, url=token_url)
        self._logger.info('response is taken')
        if not response.ok:
            raise ConnectionError(
                f'token request failed! Status code: {response.status_code}, Response text: {response.text}')
        token = response.json()
        self._logger.info('requested authentication token, expires in %i seconds', token.get('expires_in'))
        return token

    def __log_refresh(self, token):
        self._logger.info('refreshed authentication token, expires in %i seconds', token.get('expires_in'))