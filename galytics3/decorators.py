# -*- coding: utf-8 -*-
import logging
import time


def retry_wrap(retries=3, sleep=0.1, exceptions=(Exception, ConnectionError)):
    def wrapper1(func):
        def wrapper2(*args, **kwargs):
            for i in range(retries):
                try:
                    result = func(*args, **kwargs)
                except exceptions:
                    if i == retries - 1:
                        raise
                    logging.exception("Function error {}".format(func.__name__))
                    logging.warning(
                        "Retry functions {}, run number {}".format(func.__name__, i + 2)
                    )
                    time.sleep(sleep)
                else:
                    return result

        return wrapper2

    return wrapper1
