#!/usr/bin/env python

from setuptools import setup

setup(name='tap-bing-hotel-ads',
      version='1.0.0',
      description='Singer.io tap for extracting data from the Bing Hotel Ads API,'\
      ' calling endpoint https://partner.api.bingads.microsoft.com/Travel/v1/Customers()/Accounts()/ReportJobs',
      author='Snaptravel',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_bing_hotel_ads'],
      install_requires=[
        'arrow==0.15.1',
        'pytz==2018.4',
        'requests_oauthlib==1.3.0',
        'requests==2.20.0',
        'singer-python==5.8.0',
      ],
      extras_require={
          'dev': [
              'ipdb==0.11',
              'pylint'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-bing-hotel-ads=tap_bing_hotel_ads:main
      ''',
      packages=['tap_bing_hotel_ads'],
      package_data={
          'tap_bing_hotel_ads/schemas': [
          ],
          'tap_bing_hotel_ads/metadata': [
          ],
      },
      include_package_data=True,
     )
