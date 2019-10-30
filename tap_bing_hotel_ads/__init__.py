#!/usr/bin/env python3


import datetime
import os
import sys
import io
import csv
import time
import json
import copy


import singer
from singer import metrics
from singer import bookmarks
from singer import utils
from singer import metadata


LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def main_impl():
	pass



def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == "__main__":
    main()
