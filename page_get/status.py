import re
import json

from utils import text_filter
from .basic import get_page

BASE_URL = 'https://weibo.com/p/aj/mblog/getlongtext?mid={}'


def get_cont_of_weibo(mid):
    """
    :param mid: weibo's mid
    :return: all cont of the weibo
    """
    url = BASE_URL.format(mid)
    html = get_page(url, auth_level=0, is_ajax=True)

    if html:
        try:
            html = json.loads(html, encoding='utf-8').get('data').get('html')
            location_str = re.search(r'.*<a  suda-uatrack.*title="(.+?)"', html)
            if location_str:
                location = location_str.group(1).strip()
                html = re.search(r'(.*)<a  suda-uatrack.*title=".+?"', html).group(1)
            else:
                location = ''
            cont = text_filter(html)
        except AttributeError:
            location = ''
            cont = ''
        return location, cont
    else:
        return '', ''
