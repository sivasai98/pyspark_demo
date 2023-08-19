import ast
import configparser as cp
import logging
from pathlib import Path
from src.constants.error_constants import EC_PROPERTY_FILE
from src.commons.commons import sys_error
from src.constants.property_constants import PROPERTY_FILE_NAME

config_parser = cp.ConfigParser
config_parser.optionxform = str

try:
    prop_file_path = open(PROPERTY_FILE_NAME)
    prop_file_path.close()
    config_parser.read(filenames=prop_file_path,encoding="ISO-8859-1")
except Exception as e:
    print(f"Error while reading property file {PROPERTY_FILE_NAME} , Exception:{e}")
    sys_error(EC_PROPERTY_FILE)
