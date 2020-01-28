#!/usr/bin/env python3

import asyncio
import copy
import csv
import io
import json
import os
import sys
import time
from datetime import datetime
from zipfile import ZipFile

import arrow
import requests
from requests_oauthlib import OAuth2Session
import singer
from singer import bookmarks, metadata, metrics, utils

from tap_bing_hotel_ads import reports

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
  'start_date',
  'customer_id',
  'account_id',
  'oauth_access_token',
  'oauth_refresh_token',
  'ms_client_id',
]

DATE_FORMAT = '%Y-%m-%d'
SESSION = requests.session()
CONFIG = {}
DEFAULT_COLS = reports.REPORTING_FIELD_TYPES
KEYS = ['HotelId']
STREAM_NAME = 'bha_report'

# ~10 min polling timeout
MAX_NUM_REPORT_POLLS = 120
REPORT_POLL_SLEEP = 5


def get_abs_path(path):
  return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
  return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


DEFAULT_USER_AGENT = 'Singer.io Bing Hotel Ads Tap'

def get_oauth_client():
  token = {
    'access_token': CONFIG['oauth_access_token'],
    'refresh_token': CONFIG['oauth_refresh_token'],
    'token_type': 'Bearer',
    'expires_in': '0',
  }
  refresh_url = 'https://login.microsoftonline.com/common/oauth2/v2.0/token'
  extra = {'client_id': CONFIG['ms_client_id']}
  client_id = CONFIG['ms_client_id']

  def token_saver(new_token):
    token = new_token
    CONFIG['oauth_access_token'] = token['access_token']
    CONFIG['oauth_refresh_token'] = token['refresh_token']

  session = OAuth2Session(client_id,
                          token=token,
                          auto_refresh_url=refresh_url,
                          auto_refresh_kwargs=extra,
                          token_updater=token_saver)
  return session


async def poll_report(customer_id, account_id, job_id, start_date, end_date):
  download_url = None
  poll_url = "https://partner.api.bingads.microsoft.com/Travel/v1/Customers({})/Accounts({})/ReportJobs('{}')".format(
    customer_id, account_id, job_id)
  with metrics.job_timer('generate_report'):
    for i in range(1, MAX_NUM_REPORT_POLLS + 1):
      LOGGER.info('Polling report job %d/%d - %s - from %s to %s',
                  i, MAX_NUM_REPORT_POLLS, job_id, start_date, end_date)
      response = SESSION.get(poll_url)
      resp_obj = response.json()
      if resp_obj['Status'] == 'Completed':
        if resp_obj['Url']:
          download_url = resp_obj['Url']
        else:
          LOGGER.info("No results for report: %s - from %s to %s", job_id, start_date, end_date)
        break

      if i == MAX_NUM_REPORT_POLLS:
        LOGGER.info("Generating report timed out: %s - from %s to %s", job_id, start_date, end_date)
        return False, ''
      await asyncio.sleep(REPORT_POLL_SLEEP)

  return True, download_url


async def do_sync(start_date, end_date, cols):
  customer_id = CONFIG.get('customer_id')
  account_id = CONFIG.get('account_id')
  url = "https://partner.api.bingads.microsoft.com/Travel/v1/Customers({})/Accounts({})/ReportJobs".format(
    customer_id, account_id)
  data = {
    'ReportType': 'Performance',
    'StartDate': start_date,
    'EndDate': end_date,
    'Columns': cols,
    'Compression': 'ZIP',
  }
  global SESSION
  SESSION = get_oauth_client()
  response = SESSION.post(url, data=data)
  job_id = response.json()['value']
  success, download_url = await poll_report(customer_id, account_id, job_id, start_date, end_date)
  if success and download_url:
    LOGGER.info("Streaming report: %s for customer %s, account %s - from %s to %s",
                job_id, customer_id, account_id, start_date, end_date)

    stream_report(download_url, job_id, end_date)
    return True
  return False


def stream_report(url, job_id, end_date):
  with metrics.http_request_timer('download_report'):
    response = requests.get(url)

  if response.status_code != 200:
    raise Exception("Non-200 ({}) response downloading report".format(response.status_code))

  with ZipFile(io.BytesIO(response.content)) as zip_file:
    with zip_file.open(zip_file.namelist()[0]) as binary_file:
      with io.TextIOWrapper(binary_file, encoding='utf-8') as csv_file:
        # skip first 3 lines of meta text
        for _ in range(0, 3):
          header_line = next(csv_file)
        header_line = next(csv_file)[:-1]
        headers = header_line.replace('"', '').split(',')

        reader = csv.DictReader(csv_file, fieldnames=headers)

        schema = {'properties': {}}
        for h in headers:
          if not h in reports.REPORTING_FIELDNAME_MAP:
            continue
          f = reports.REPORTING_FIELDNAME_MAP[h]
          _type = reports.REPORTING_FIELD_TYPES[f]
          field_data = {'type': _type}
          if _type in ['date', 'datetime']:
            field_data = {'type': 'string', 'format': 'date-time'}
          if f in KEYS:
            field_data['key'] = True
          schema['properties'][f] = field_data
        singer.write_schema(STREAM_NAME, schema, KEYS)

        with metrics.record_counter(job_id) as counter:
          for row in reader:
            singer.write_record(STREAM_NAME, type_report_row(row))
            counter.increment()
        singer.write_state({'start_date': end_date})


def type_report_row(row):
  output = {}
  for field_name, value in row.items():
    value = value.strip()
    if value == '':
      value = None

    if value is not None and field_name in reports.REPORTING_FIELDNAME_MAP:
      colname = reports.REPORTING_FIELDNAME_MAP[field_name]
      _type = reports.REPORTING_FIELD_TYPES[colname]
      if _type == 'integer':
        value = int(value.replace(',', ''))
      elif _type == 'number':
        value = float(value.replace('%', '').replace(',', ''))
      elif _type in ['date', 'datetime']:
        value = arrow.get(value).isoformat()
      output[colname] = value
  return output


def main_impl():
  args = utils.parse_args(REQUIRED_CONFIG_KEYS)
  config = args.config if args.config else {}
  state = args.state if args.state else {}
  CONFIG.update(config)

  start_date = state.get('start_date', config.get('start_date', datetime.utcnow().strftime(DATE_FORMAT)))
  end_date = state.get('end_date', config.get('end_date', datetime.utcnow().strftime(DATE_FORMAT)))

  cols = config.get('cols', DEFAULT_COLS)

  loop = asyncio.get_event_loop()
  loop.run_until_complete(do_sync(start_date, end_date, cols))


def main():
  try:
    main_impl()
  except Exception as exc:
    LOGGER.critical(exc)
    raise exc


if __name__ == "__main__":
  main()
