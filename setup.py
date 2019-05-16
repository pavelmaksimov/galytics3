# -*- coding: utf-8 -*-
import setuptools

VERSION = '2019.4.9'

setuptools.setup(
    name='galytics3',
    version=VERSION,
    description="Обертка над библиотекой google_api_python_client для работы с API Google Analytics v3",
    packages=setuptools.find_packages(),
    install_requires=['pandas', 'google_api_python_client', 'oauth2client']
)
