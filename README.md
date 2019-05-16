# Обертка над стандартной библиотекой google_api_python_client для легкой работы с API Google Analytics v3

Написано на версии python 3.5

Умеет запрашивать данные маленькими порциями, 
чтобы обойти семплирование. 
Также если в один ответ не поместятся все строки (макс 10000 строк), 
сделает дополнительные запросы.

### Установка
```
# Установите эту штуку, она будет генерировать маленькие интервалы в случае семплирования.
# без нее работать не будет
pip install git+https://github.com/pavelmaksimov/daterangepy#egg=daterangepy-2019.4.9
pip install --upgrade git+https://github.com/pavelmaksimov/galytics3
```

### Как пользоваться

Указание авторизационных данных.

Эта обертка не умеет получать токен, он у вас уже должен быть. 
Как получить? Гуглите.

##### Вариант 1
```python
from galytics3 import GoogleAnalytics

api = GoogleAnalytics(refresh_token='{refresh_token}',
                      client_id='{client_id}',
                      client_secret='{client_secret}')
```

##### Вариант 2
Если у вас объект credential создается другим образом. 
Через файл или еще как-то.
```python
from galytics3 import GoogleAnalytics

credentials = credentials_object  # Ваш объект credential

api = GoogleAnalytics(credentials=credentials)
```

##### Вариант 3
Объявление дополнительных настроек, типа кеширования.
```python
from googleapiclient.discovery import build
from galytics3 import GoogleAnalytics

credentials = credentials_object  # Ваш объект credential
# В build можно объявить дополнительные настройки, вроде кеширования и т.д.
service = build('analytics', 'v3', credentials=credentials_object)
api = GoogleAnalytics(service=service)
```

#### Получаем данные
```python
from datetime import datetime
from galytics3 import GoogleAnalytics

api = GoogleAnalytics(refresh_token='{refresh_token}',
                      client_id='{client_id}',
                      client_secret='{client_secret}')

# Получит все аккаунты, ресурсы и представления.
df = api.get_accounts(as_dataframe=True)
# По умолчанию данные возвращаются в формате dataframe
print(df)

# Вернуть в JSON
data = api.get_accounts(as_dataframe=False)
print(data)

# Получит все цели всех представлений.
df = api.get_goals()
print(df)

# Запросить стандартный отчет
df = api.get_report(
    id=12345789,
    source='GA',
    date1=datetime(2019, 1, 1),
    date2=datetime(2019, 1, 10),
    dimensions=['ga:date'],
    metrics=['ga:percentNewSessions'],
    sort='ga:date')
print(df)

# Запросить отчет MCF
df = api.get_report(
    id=12345789,
    source='mcf',
    date1=datetime(2019, 1, 1),
    date2=datetime(2019, 1, 10),
    dimensions=['mcf:sourceMediumPath', 'mcf:conversionDate, mcf:source'],
    metrics=['mcf:totalConversions', 'mcf:totalConversionValue'],
    sort='mcf:source',
    filters='mcf:ConversionType==Transaction')
print(df)

```


## Зависимости
- pandas
- [daterangepy](https://github.com/pavelmaksimov/daterangepy)

## Автор
Павел Максимов

Связаться со мной можно в 
[Телеграм](https://t.me/pavel_maksimow) 
и в 
[Facebook](https://www.facebook.com/pavel.maksimow)

Удачи тебе, друг! Поставь звездочку ;)
