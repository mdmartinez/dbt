import pyodbc

import dbt.adapters.default
import dbt.exceptions

from dbt.logger import GLOBAL_LOGGER as logger


class ODBCAdapter(dbt.adapters.default.DefaultAdapter):

    @classmethod
    def get_odbc_connection_string(cls, credentials):
        raise dbt.exceptions.NotImplementedException(
            '`get_odbc_connection_string` is not implemented for '
            'this adapter!')

    @classmethod
    def get_status(cls, cursor):
        return cursor.rowcount

    @classmethod
    def open_connection(cls, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()

        try:
            credentials = connection.get('credentials', {})
            handle = pyodbc.connect(
                cls.get_odbc_connection_string(credentials),
                autocommit=False)

            result['handle'] = handle
            result['state'] = 'open'
        except Exception as e:
            logger.debug("Got an error when attempting to open a postgres "
                         "connection: '{}'"
                         .format(e))

            result['handle'] = None
            result['state'] = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return result

    @classmethod
    def cancel_connection(cls, profile, connection):
        pass
