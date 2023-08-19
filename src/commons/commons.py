import sys
import subprocess
import logging as log
from src.constants.error_constants import EC_PARSE_ERR


def sys_error(error_code, exit_fl=True):
    """
    Do System exit incase of error
    :param error_code: integer exit code
    :param exit_fl: exit on error flag(TRUE/FALSE)
    :return:
    """
    if exit_fl:
        log.error(f"Error code:{error_code} Exiting!...")
        sys.exit(error_code)
    else:
        return None


def parse_args(args_list, num_args, exit_fl=True):
    """
    return tuple of passed list value
    :param args_list:
    :param num_args:
    :param exit_fl:
    :return:
    """
    args_list_len = len(args_list) - 1
    if args_list_len != num_args:
        log.error(f"Required Arguments:{num_args}, Passed Arguments:{args_list_len}")
        return sys_error(EC_PARSE_ERR)
    tup = tuple(args_list[1:])
    log.info(f"Parsed Arguments:{tup}")
    return tup


def parse_first_n_args(args_list, num_args, exit_fl=True):
    """
    return first n elements of list in form of tuple
    :param args_list:
    :param num_args:
    :param exit_fl:
    :return:
    """
    return parse_args(args_list[0:num_args + 1], num_args, exit_fl)
