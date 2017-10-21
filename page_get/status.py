# -*-coding:utf-8 -*-
import re
import json
from utils import filters
from page_get.basic import get_page


base_url = 'http://weibo.com/p/aj/mblog/getlongtext?ajwvr=6&mid={}'


def get_cont_of_weibo(mid):
    """
    :param mid: weibo's mid
    :return: all cont of the weibo
    """
    url = base_url.format(mid)
    html = get_page(url, user_verify=False)

    if html:
        try:
            html = json.loads(html, encoding='utf-8').get('data').get('html')
            location = re.search(r'.*<a  suda-uatrack.*title="(.+?)"', html).group(1).strip()
            html = re.search(r'(.*)<a  suda-uatrack.*title=".+?"', html).group(1)
            cont = filters.text_filter(html)
        except AttributeError:
            location = ''
            cont = ''
        return location, cont

__all__ = ['get_cont_of_weibo']
