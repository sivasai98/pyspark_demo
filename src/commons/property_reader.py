import ast
import configparser as cp
import logging
from pathlib import Path
from src.constants.error_constants import EC_PROPERTY_FILE, EC_PROPERTY
from src.commons.commons import sys_error
from src.constants.property_constants import PROPERTY_FILE_NAME

config_parser = cp.ConfigParser
config_parser.optionxform = str

try:
    prop_file_path = open(PROPERTY_FILE_NAME)
    prop_file_path.close()
    config_parser.read(filenames=PROPERTY_FILE_NAME, encoding="ISO-8859-1")
except Exception as e:
    print(f"Error while reading property file {PROPERTY_FILE_NAME} , Exception:{e}")
    sys_error(EC_PROPERTY_FILE)


def get_property(sec_name, prop_name, exit_fl=True):
    """
    Returns the property value of the given section name and property name
    :param sec_name:
    :param prop_name:
    :param exit_fl:
    :return:
    """
    try:
        prop_val = config_parser.get(sec_name, prop_name)
    except Exception as e:
        print(f"Error get Property : {prop_name} from section : {sec_name} , Error msg : {e}")
        return sys_error(EC_PROPERTY)
    print(f"property {prop_name}:{prop_val}")
    return prop_val


def get_property_int(sec_name, prop_name, exit_fl=True):
    """
    Returns the property value of the given section name and property name
    :param sec_name:
    :param prop_name:
    :param exit_fl:
    :return:
    """
    try:
        prop_val = config_parser.getint(sec_name, prop_name)
    except Exception as e:
        print(f"Error get Property : {prop_name} from section : {sec_name} , Error msg : {e}")
        return sys_error(EC_PROPERTY)
    print(f"property {prop_name}:{prop_val}")
    return prop_val

def get_section_config(sec_name, exit_fl=True):
    """
    Returns the property value of the given section name and property name
    :param sec_name:
    :param exit_fl:
    :return:
    """
    try:
        prop_data = config_parser.items(sec_name)
    except Exception as exp:
        print(f"Error while getting the section {sec_name} properties , Error is {exp}")
    return prop_data