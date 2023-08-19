import os
import sys
import logging as log
from src.commons.commons import parse_first_n_args
from src.constants.error_constants import EC_INVALID_ENV
from src.constants.property_constants import DEV, UAT, PROD


class Invoker:
    def __int__(self, env: str):
        self.env = env

    def run_module(self):

        pass





def main(sys_argv=sys.argv):
    """
    :param sys_argv:
    :return:
    """
    env= parse_first_n_args(sys_argv, 1)
    if env.upper() not in [DEV,UAT,PROD]:
        log.error(f"Invalid Env :{env}")
        sys.exit(EC_INVALID_ENV)

    invoker = Invoker(env)
    invoker.run_module()


if __name__ == '__main__':
    main()
