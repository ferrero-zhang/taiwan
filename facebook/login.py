# -*- coding: utf-8 -*-
from selenium import webdriver

USER_NAME = '714445945@qq.com'
USER_PWD = 'zxcv1234'

def login():
    client = webdriver.Firefox()
    login_url = 'https://mobile.facebook.com'
    client.get(login_url)

    username = client.find_element_by_name("email")
    pwd = client.find_element_by_name("pass")
    submit = client.find_element_by_name("login")

    USER = USER_NAME
    PWD = USER_PWD
    username.send_keys(USER)
    pwd.send_keys(PWD)