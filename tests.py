# -*- coding: utf-8 -*-
from datetime import datetime

from pandas import set_option

from galytics3 import GoogleAnalytics

set_option('display.max_columns', 100)
set_option('display.width', 1500)

api = GoogleAnalytics(refresh_token='',
                      client_id='',
                      client_secret='')


def test_get_accounts():
    r = api.get_accounts()
    print(r)


def test_get_goals():
    r = api.get_goals()
    print(r)


def test_get_report_mcf_without_transform():
    r = api.get_report(id=104194259,
                       source='mcf',
                       date1=datetime(2018, 10, 1),
                       date2=datetime(2018, 10, 10),
                       dimensions=['mcf:sourceMediumPath', 'mcf:conversionDate', 'mcf:ConversionType', 'mcf:source'],
                       metrics=['mcf:totalConversions', 'mcf:totalConversionValue'],
                       sort='mcf:source',
                       filters='mcf:ConversionType==Transaction',
                       is_transform_dataframe=False)
    print(r)


def test_get_report_mcf():
    r = api.get_report(id=104194259,
                       source='mcf',
                       date1=datetime(2018, 10, 1),
                       date2=datetime(2018, 10, 10),
                       dimensions=['mcf:sourceMediumPath', 'mcf:conversionDate', 'mcf:ConversionType', 'mcf:source'],
                       metrics=['mcf:totalConversions', 'mcf:totalConversionValue'],
                       sort='mcf:source',
                       filters='mcf:ConversionType==Transaction')
    print(r)


def test_get_report_ga():
    r = api.get_report(id=108886513,
                       source='GA',
                       date1=datetime(2018, 10, 1),
                       date2=datetime(2018, 10, 10),
                       dimensions=['ga:date'],
                       metrics=['ga:percentNewSessions'],
                       level_group_by_date='date')
    print(r)


def test_get_report_as_df():
    r = api.get_report(id=108886513,
                       source='GA',
                       date1=datetime(2018, 10, 1),
                       date2=datetime(2018, 10, 10),
                       dimensions=['ga:date'],
                       metrics=['ga:percentNewSessions'],
                       sort='ga:date',
                       level_group_by_date='date')
    print(r)


def test_next_page_request():
    r = api.get_report(id=108886513, source='ga',
                       date1=datetime(2018, 10, 1),
                       date2=datetime(2018, 10, 10),
                       dimensions=['ga:date'],
                       metrics=['ga:percentNewSessions'],
                       limit=3)
    print(r)


def test_sampling():
    r = api.get_report(id=130339206, source='ga',
                       date1=datetime(2018, 1, 1),
                       date2=datetime(2018, 1, 31),
                       dimensions=['ga:date', 'ga:userType', 'ga:keyword'],
                       metrics=['ga:percentNewSessions'],
                       limit=9000)
    print(len(r))
    print(r)
    assert len(r.drop_duplicates('ga:date')) == 31
