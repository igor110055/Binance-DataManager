from BinanceDataManager.exceptions import TypeNotImplemented


def only_implemented_types(func):
    """
    Decorator to raise an exception when a not implemented type is given as a parameter to a function
    :param func: function to be decorated
    :return: decorated function
    """
    import functools

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        parameters = func.__code__.co_varnames
        annotations = func.__annotations__
        arguments = {}
        # making arguments dictionary to put them with their parameter name
        for i in range(len(args)):
            arguments[parameters[i]] = args[i]
        for parameter in kwargs:
            arguments[parameter] = kwargs[parameter]

        # apply verification to all parameters in annotations
        for parameter in annotations:
            if parameter not in arguments:
                continue
            if not isinstance(arguments[parameter], annotations[parameter]):
                raise TypeNotImplemented(parameter, annotations[parameter], type(arguments[parameter]))

        return func(*args, **kwargs)

    return wrapper
