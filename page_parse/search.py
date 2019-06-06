# -*-coding:utf-8 -*-
import re
import urllib.parse
from datetime import datetime

from bs4 import BeautifulSoup

from logger import parser
from page_get import basic, status
from page_parse.status import get_create_time
from utils import url_filter
from db.models import WeiboData
from decorators import parse_decorator
from tasks.workers import app
from config import (
    get_crawling_mode, get_images_allow, get_images_path)

CRAWLING_MODE = get_crawling_mode()
IMG_ALLOW = get_images_allow()
IMG_PATH = get_images_path()


@parse_decorator('')
def _search_page_parse(html):
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all('script')
    pattern = re.compile(r'view\((.*)\)')
    for script in scripts:
        m = pattern.search(str(script))
        # 这个判断不全面，对于json编码的可以成功，对于直接返回的不会成功
        if m and 'pl_weibo_direct' in script.string and 'S_line1' in script.string:
            search_cont = m.group(1)
            pattern2 = re.compile(r'"html":"(.*)"}$')
            m2 = pattern2.search(search_cont)
            if m2:
                return m2.group(1).encode('utf-8', 'ignore').decode('unicode-escape', 'ignore').replace('\\', '')
    return ''


def get_feed_info(feed_infos, goal):
    info_num = None
    for info in feed_infos:
        if goal in info.text:
            info_num = info.text.replace(goal, '')
            break
    if info_num is None:
        parser.error('unexcept template:{}'.format(feed_infos))
    return int(info_num)


@parse_decorator(None)
def get_weibo_info(each, html):
    wb_data = WeiboData()

    class_content = each.find(attrs={'class': 'content'})
    user_link = class_content.find(attrs={'class': 'name'})
    # this only for login user
    if not user_link:
        return None
    wb_data.uid = re.findall(r"//weibo.com/(\d+)\?refer_flag=.*", user_link.get('href', ''))[0]

    wb_data.weibo_id = each['mid']

    class_from = class_content.find(attrs={'class': 'from'})
    wb_data.weibo_url = class_from.a['href']

    feed_list_media = each.find(attrs={'node-type': 'feed_list_media_prev'})
    imgs = list()
    imgs_url = list()
    try:
        imgs = str(feed_list_media.find_all('li'))
        imgs_url = list(map(url_filter, re.findall(r"src=\"(.+?)\"", imgs)))
        wb_data.weibo_img = ';'.join(imgs_url)
    except Exception as why:
        parser.warn('Failed to get imgs, the error is {}'.format(why, each))
        wb_data.weibo_img = ''

    if IMG_ALLOW and imgs and imgs_url:
        app.send_task('tasks.downloader.download_img_task', args=(wb_data.weibo_id, imgs_url),
                      queue='download_queue', routing_key='for_download')
        wb_data.weibo_img_path = IMG_PATH
    else:
        wb_data.weibo_img_path = ''

    try:
        a_tag = str(feed_list_media.find_all('a'))
        extracted_url = urllib.parse.unquote(re.findall(r"full_url=(.+?)&amp;", a_tag)[0])
        wb_data.weibo_video = url_filter(extracted_url)
    except Exception as why:
        parser.warn('Failed to get video, the error is {}'.format(why, each))
        wb_data.weibo_video = ''

    try:
        wb_data.device = class_from.find(attrs={'rel': 'nofollow'}).string
    except AttributeError:
        wb_data.device = ''

    try:
        create_time_url = class_from.a['href']
        # 2018年国庆改版后搜索页面的时间只能从字符串转换
        create_time = float(get_create_time(create_time_url)) / 1000
        wb_data.create_time = datetime.fromtimestamp(create_time)
        # wb_data.create_time = create_time.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return None

    try:
        feed_action = each.find(attrs={'class': 'card-act'})
    except Exception as why:
        parser.error('Failed to get feed_action, the error is {},the page source is {}'.format(why, each))
    else:
        feed_infos = feed_action.find_all('li')
        try:
            wb_data.repost_num = get_feed_info(feed_infos, '转发')
        except (AttributeError, ValueError):
            wb_data.repost_num = 0
        try:
            wb_data.comment_num = get_feed_info(feed_infos, '评论')
        except (AttributeError, ValueError):
            wb_data.comment_num = 0
        try:
            wb_data.praise_num = int(feed_action.find(attrs={'action-type': 'feed_list_like'}).find('em').text)
        except (AttributeError, ValueError):
            wb_data.praise_num = 0

    class_txt = class_content.find(attrs={'class': 'txt'})
    try:
        location_icon = class_txt.find(attrs={'class': 'wbicon'}, string='2')
        location_icon.decompose()
        location_element = location_icon.parent
        wb_data.weibo_location = ''.join(list(location_element.stripped_strings))
    except Exception:
        wb_data.weibo_location = ''

    wbicons = class_txt.find_all(attrs={'class': 'wbicon'})
    for wbicon in wbicons:
        wbicon.parent.decompose()
    wb_data.weibo_cont = ''.join(list(class_txt.stripped_strings)[:-1])

    if '展开全文' in str(each):
        is_all_cont = 0
    else:
        is_all_cont = 1
    return wb_data, is_all_cont


@parse_decorator([])
def get_search_info(html):
    """
    :param html: response content for search with login
    :return: search results
    """
    # 搜索结果可能有两种方式，一种是直接返回的，一种是编码过后的
    content = _search_page_parse(html) if '投诉' not in html else html
    if content == '' or 'card-no-result' in html:
        return []
    # todo 这里用bs会导致某些信息不能被解析（参考../tests/fail.html），可参考使用xpath，考虑到成本，暂时不实现
    soup = BeautifulSoup(content.encode('utf-8', 'ignore').decode('utf-8'), "html.parser")
    feed_list = soup.find_all(attrs={'action-type': 'feed_list_item'})
    search_list = []
    for each in feed_list:
        r = get_weibo_info(each, html)
        if r is not None:
            wb_data = r[0]
            if r[1] == 0 and CRAWLING_MODE == 'accurate':
                weibo_location, weibo_cont = status.get_cont_of_weibo(wb_data.weibo_id)
                wb_data.weibo_location = weibo_location if weibo_location else wb_data.weibo_location
                wb_data.weibo_cont = weibo_cont if weibo_cont else wb_data.weibo_cont
            search_list.append(wb_data)
    return search_list
