# -*- coding: utf-8 -*-
import logging
import re

import daterangepy
import pandas as pd
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
from pandas.io.json import json_normalize

logging.basicConfig(level=logging.INFO)


class MaxLevelSamplingError(Exception):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Увеличение интервалов для обхода семплирования ' \
               'достигло максимума, до одного.' \
               'Но семплирование все равно есть.' \
               'Разделять периоды по часам, не умею.'


class GoogleAnalytics:
    def __init__(self, refresh_token=None,
                 client_id=None, client_secret=None,
                 token_uri='https://www.googleapis.com/oauth2/v3/token',
                 credentials=None, service=None):
        if service:
            self.service = service
        else:
            if not credentials:
                credentials = GoogleCredentials(
                    refresh_token=refresh_token,
                    token_uri=token_uri,
                    client_id=client_id,
                    client_secret=client_secret,
                    user_agent='Python client library',
                    access_token=None,
                    token_expiry=None,
                    revoke_uri=None)

            self.service = build('analytics', 'v3',
                                 credentials=credentials,
                                 cache_discovery=False)

    def _generate_body(self, body, date1, date2,
                       level_group_by_date, delta):
        """Создаются несколько конфигов для получения данных."""
        intervals = daterangepy.period_range(
            date1, date2, frequency=level_group_by_date, delta=delta)
        body_list_ = []
        for date in intervals:
            body_ = body.copy()
            body_['start_date'] = date['date1_str']
            body_['end_date'] = date['date2_str']
            body_list_.append(body_)

        return body_list_

    def _transform_dataframe(self, df, source):
        # Дополнительная обработка, если источник данных MCF.
        try:
            if source.lower() == 'mcf':
                # Раскрытие вложенных столбцов.
                df = json_normalize(df.to_dict(orient='records'))
                # Раскрытие вложенных столбцов.
                columns_for_parsing = [i for i in df.columns if i.find('conversionPathValue') > -1]
                df[columns_for_parsing] = df[columns_for_parsing].applymap(lambda x: x[0])
                data_json = df.to_dict(orient='records')
                df = json_normalize(data_json)

                df.columns = [i.replace('.conversionPathValue.nodeValue', '') for i in df.columns]
                df.columns = [i.replace('.primitiveValue', '') for i in df.columns]

                # Преобразуется формат даты. Здесь он специфичный.
                if 'mcf:conversionDate' in df.columns:
                    df['mcf:conversionDate'] = pd.to_datetime(df['mcf:conversionDate']).dt.strftime('%Y-%m-%d')
        except Exception:
            raise Exception('Возникла ошибка при трансформации dataframe. '
                            'Вы можете выключить ее задав is_transform_dataframe=False')

        return df

    def _to_df(self, results_list):
        try:
            df = pd.DataFrame()
            for result in results_list:
                columns = [i['name'] for i in result['columnHeaders']]
                df = df.append(pd.DataFrame(columns=columns,
                                            data=result.get('rows', [])))
        except Exception:
            raise TypeError('Не смог преобразовать в dataframe')
        else:
            return df.reset_index(drop=True)

    def _get_next_page_body(self, next_page_link, body):
        logging.debug('Получены не все данные. Будет сделан еще разпрос')
        # Извлечение индекса из ссылки для запроса данных.
        # Как сделать запрос через эту ссылку не разобрался.
        search = re.search('start-index.[^&]*', next_page_link).group()
        next_index = re.sub(r'[^0-9]', '', search)
        # Меняется индекс строки от которой будут запрошены данные.
        if not next_index:
            raise ValueError('Не смог извлечь номер строки, '
                             'от которой запросить следующую пачку данных. '
                             'Пытался извлечь параметр start-index из "{}".'
                             'И число из значения этого параметра "{}"'
                             .format(next_page_link, search))
        body['start_index'] = next_index
        return body

    def _execute(self, body, source):
        if source.lower() == 'ga':
            request_config = self.service.data().ga().get(**body)
        elif source.lower() == 'mcf':
            request_config = self.service.data().mcf().get(**body)
        else:
            raise ValueError('Неверное значение source')

        return request_config.execute()

    def _request(self, body, date1, date2, source,
                 level_group_by_date, max_results=None):
        """
        Запрашивает данные.
        Генерирует новые запросы на ходу, по мере необходимости.
        Когда есть семплирование, чтобы обойти его
        и когда все данные не поместились в одном ответе
        (макс 100к строк в ответе).
        """
        body_list = [body]
        results_list = []
        sampling_level = 2  # на сколько частей будет делить период
        delta = (date2-date1).days
        while body_list:
            iter_body = body_list[0]

            result = self._execute(iter_body, source)

            if result.get('containsSampledData'):
                logging.debug('Есть семплирование')
                # Кол-во дней в одном интервале.
                new_delta = int(delta / (sampling_level))
                new_delta = new_delta+1 if new_delta > 1 else new_delta
                if new_delta < 1:
                    raise MaxLevelSamplingError

                # Генерируются новые конфиги с меньшим интервалом дат.
                body_list = self._generate_body(
                    body, date1, date2, level_group_by_date, new_delta)
                # Интервал между датами уменьшется каждый раз в 2 раза.
                sampling_level *= 2
                continue
            else:
                results_list.append(result)
                # Удаляем успешно выполненый конфиг запроса из списка конфигов.
                body_list.remove(iter_body)

                # Если получен не все данные, то добавляем еще конфиг,
                next_page_link = result.get('nextLink')
                if next_page_link and max_results is None:
                    next_body = self._get_next_page_body(next_page_link, iter_body)
                    body_list.append(next_body)

        return results_list

    def get_accounts(self, as_dataframe=True):
        """
        Запрашивает всю информацию об аккаунтах, ресурсах и представлениях.
        :return:
        """
        accounts = []

        all_accounts = self.service.management() \
            .accounts().list().execute()
        for account in all_accounts.get('items', []):

            all_webproperties = self.service.management() \
                .webproperties().list(accountId=account['id']).execute()

            for webpropert in all_webproperties.get('items', []):
                all_profiles = self.service.management().profiles() \
                    .list(accountId=account['id'],
                          webPropertyId=webpropert['id']) \
                    .execute()

                for profile in all_profiles.get('items', []):
                    settings_account = all_accounts.copy()
                    settings_resource = all_webproperties.copy()
                    settings_view = all_profiles.copy()

                    settings_account.pop('items')
                    settings_resource.pop('items')
                    settings_view.pop('items')

                    data = {
                        'settings_account': settings_account,
                        'settings_resource': settings_resource,
                        'settings_view': settings_view,
                        'account': account,
                        'resource': webpropert,
                        'view': profile,
                    }
                    accounts.append(data)

        return json_normalize(accounts) if as_dataframe else accounts

    def get_goals(self, as_dataframe=True):
        """Запрашивает все цели всех представлений аккаунта."""
        df_accounts = self.get_accounts()
        ids_list = df_accounts[['account.id', 'resource.id', 'view.id']] \
            .drop_duplicates().to_dict(orient='records')

        result = []
        for i in ids_list:
            result_ = self.service.management().goals().list(
                accountId=i['account.id'],
                webPropertyId=i['resource.id'],
                profileId=i['view.id'],
            ).execute()
            result += result_.get('items', [])

        return json_normalize(result) if as_dataframe else result

    def get_report(self, id, source, date1, date2,
                   dimensions, metrics, sort=None,
                   filters=None, limit=10000, max_results=None,
                   level_group_by_date='date', as_dataframe=True,
                   is_transform_dataframe=True):
        """

        :param id: int, str : идентификатор аккаунта, например "123456789"
        :param source: str : ga|mcf
        :param date1: datetime
        :param date2: datetime
        :param metrics: str, list
        :param dimensions: str, list
        :param sort: str
        :param filters: str
        :param limit: int, максимальное кол-во строк в одном запросе.
        :param max_results: int, максимальное кол-во строк в отчете
        :param level_group_by_date: str : day|date|week|month|quarter|year
        :param as_dataframe: bool : возвращать ли в формате dataframe
        :param is_transform_dataframe: bool : трансформировать dataframe
        :return: [..., '{данные ответа}'], dataframe
        """
        source = source.lower()
        if source not in ('ga', 'mcf'):
            raise ValueError('Неизвестный источник данных {}'
                             .format(source))

        if isinstance(metrics, list):
            metrics = ','.join(map(str, metrics))
        if isinstance(dimensions, list):
            dimensions = ','.join(map(str, dimensions))

        body = dict(
            ids='ga:{}'.format(id),
            start_date=str(date1.date()),
            end_date=str(date2.date()),
            metrics=metrics,
            dimensions=dimensions,
            start_index='1',
            samplingLevel='HIGHER_PRECISION',
        )
        if max_results:
            body['max_results'] = max_results
        else:
            body['max_results'] = limit

        if sort:
            body['sort'] = sort
        if filters:
            body['filters'] = filters

        results_list = self._request(body, date1, date2, source,
                                     level_group_by_date,
                                     max_results=max_results)

        if as_dataframe:
            df = self._to_df(results_list)
            if is_transform_dataframe:
                df = self._transform_dataframe(df, source)
            return df
        else:
            return results_list
