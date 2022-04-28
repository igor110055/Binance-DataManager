from BinanceDataManager.constants import Colors


class WrongExchange(BaseException):
    """
    Exception to be raised when an exchange is not valid
    """

    def __init__(self, exchange):
        """
        Constructor
        :param exchange: exchange
        """
        super().__init__(f"{Colors.ERROR}[EZBT exception] '{exchange}' is not a valid exchange, use 'import ccxt' and "
                         f"then 'ccxt.binance()' or another one instead {Colors.END}")


class WrongMarket(BaseException):
    """
    Exception to be raised when an unknown market is given
    """

    def __init__(self, market):
        """
        Constructor
        :param market: market
        """
        super().__init__(f"{Colors.ERROR}[EZBT exception] '{market}' is not a valid market for this exchange "
                         f"{Colors.END}")


class WrongTimeframe(BaseException):
    """
    Exception to be raised when an unknown timeframe is given
    """

    def __init__(self, timeframe):
        """
        Constructor
        :param timeframe: timeframe
        """
        super().__init__(f"{Colors.ERROR}[EZBT exception] '{timeframe}' is not a valid timeframe for this exchange "
                         f"{Colors.END}")


class WrongLimit(BaseException):
    """
    Exception to be raised when a wrong value for limit is given
    """

    def __init__(self, limit):
        """
        Constructor
        :param limit: limit
        """
        super().__init__(f"{Colors.ERROR}[EZBT exception] '{limit}' is not a valid value for limit parameter"
                         f"{Colors.END}")


class WrongSince(BaseException):
    """
    Exception to be raised when a wrong value for since is given
    """

    def __init__(self, since):
        """
        Constructor
        :param since: since
        """
        super().__init__(f"{Colors.ERROR}[EZBT exception] '{since}' is not a valid value for since parameter, "
                         f"there's no candle for this timestamp{Colors.END}")


class TypeNotImplemented(BaseException):
    """
    Exception to be raised when a not implemented type is given to a function
    """

    def __init__(self, parameter_name, required_type, given_type):
        """
        Constructor
        :param parameter_name: name of the parameter involved in the exception
        :param required_type:
        """
        super().__init__(
            f"{Colors.ERROR}[EZBT exception] {given_type} is not implemented for this parameter, parameter "
            f"{parameter_name} must be {required_type}{Colors.END}")
