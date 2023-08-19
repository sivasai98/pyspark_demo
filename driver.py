import os
import sys
import logging as log
from src.commons.commons import parse_first_n_args
from src.constants.error_constants import EC_INVALID_ENV
from src.constants.property_constants import DEV, UAT, PROD
from src.modules.app import App

class Invoker:
    def __init__(self, env: str):
        self.env = env

    def run_module(self):
        with App(self.env) as app:
            app.do()

def main(sys_argv=sys.argv):
    """
    :param sys_argv:
    :return:
    """
    # env= parse_first_n_args(sys_argv, 1)
    env = DEV
    if env.upper() not in [DEV,UAT,PROD]:
        log.error(f"Invalid Env :{env}")
        sys.exit(EC_INVALID_ENV)

    invoker = Invoker(env)
    invoker.run_module()


if __name__ == '__main__':
    main()
