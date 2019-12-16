#!/usr/bin/env python

from setuptools import setup

setup(name='tap-bing-hotel-ads',
      version="1.0.0",
      description='Singer.io tap for extracting data from the Bing hotel ads api',
      author='Snaptravel',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_bha'],
      install_requires=[
          'arrow==0.12.0',
          'singer-python==5.1.5',
          'requests==2.20.0',
          'pytz==2018.4',
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
      packages=['tap_bha'],
      package_data = {
          'tap_bha/schemas': [
          ],
          'tap_bha/metadata': [
          ],
      },
      include_package_data=True,
)