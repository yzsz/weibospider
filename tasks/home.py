import time
from datetime import datetime

from logger import crawler
from .workers import app
from page_parse.user import public
from page_get import get_page
from config import (get_max_home_page, get_time_after, get_outdated_days)
from db.dao import (WbDataOper, SeedidsOper, HomeCollectionOper)
from db.redis_db import LastCache
from page_parse.home import (get_data, get_ajax_data, get_total_page)

# only crawls origin weibo
HOME_URL = 'http://weibo.com/u/{}?is_ori=1&is_tag=0&profile_ftype=1&page={}'
AJAX_URL = 'http://weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain={}&pagebar={}&is_ori=1&id={}{}&page={}' \
           '&pre_page={}&__rnd={}'
timeafter = datetime.strptime(get_time_after(), '%Y-%m-%d %H:%M:%S')


@app.task(ignore_result=True)
def crawl_ajax_page(url, auth_level):
    """
    :param url: user home ajax url
    :param auth_level: 1 stands for no login but need fake cookies, 2 stands for login
    :return: resp.text
    """
    ajax_html = get_page(url, auth_level, is_ajax=True)
    ajax_wbdata = get_ajax_data(ajax_html)
    if not ajax_wbdata:
        return ''

    for i in range(0, len(ajax_wbdata)):
        weibo_time = ajax_wbdata[i].create_time
        if weibo_time < timeafter:
            ajax_wbdata = ajax_wbdata[0:i]
            break

    WbDataOper.add_all(ajax_wbdata)
    return ajax_html


@app.task(ignore_result=True)
def crawl_ajax_page_newest(url, auth_level):
    """
    :param url: user home ajax url
    :param auth_level: 1 stands for no login but need fake cookies, 2 stands for login
    :return: resp.text
    """
    ajax_html = get_page(url, auth_level, is_ajax=True)
    ajax_wbdata = get_ajax_data(ajax_html)
    if not ajax_wbdata:
        return ''

    for i in range(0, len(ajax_wbdata)):
        weibo_time = ajax_wbdata[i].create_time
        if weibo_time < timeafter:
            ajax_wbdata = ajax_wbdata[0:i]
            break

    if WbDataOper.get_wb_by_mid(ajax_wbdata[0].weibo_id):
        return
    WbDataOper.add_all(ajax_wbdata)
    return ajax_html


@app.task(ignore_result=True)
def crawl_ajax_page_collection(url, auth_level, last_mid, last_updated):
    """
    :param url: user home ajax url
    :param auth_level: 1 stands for no login but need fake cookies, 2 stands for login
    :param last_mid: mid of user's last weibo in previous crawling
    :param last_updated: create_time of user's last weibo in previous crawling
    :return: resp.text, whether time limit or last_mid reached
    """
    ajax_html = get_page(url, auth_level, is_ajax=True)
    ajax_wbdata = get_ajax_data(ajax_html)
    if not ajax_wbdata:
        return ''

    for i in range(0, len(ajax_wbdata)):
        weibo_time = ajax_wbdata[i].create_time
        if weibo_time < timeafter \
                or last_mid and last_mid == ajax_wbdata[i].weibo_id \
                or last_updated and last_updated > ajax_wbdata[i].create_time:
            ajax_wbdata = ajax_wbdata[0:i]
            break
    if ajax_wbdata:
        WbDataOper.add_all(ajax_wbdata)
    return ajax_html


@app.task(ignore_result=True)
def crawl_weibo_data(uid):
    limit = get_max_home_page()
    cur_page = 1
    outdated = 1
    while cur_page <= limit:
        url = HOME_URL.format(uid, cur_page)
        if cur_page == 1:
            html = get_page(url, auth_level=1)
        else:
            html = get_page(url, auth_level=2)
        weibo_data = get_data(html)

        if not weibo_data:
            crawler.warning("user {} has no weibo".format(uid))
            return

        # Check whether weibo created after time in spider.yaml
        length_weibo_data = len(weibo_data)
        for i in range(0, len(weibo_data)):
            weibo_time = weibo_data[i].create_time
            if weibo_time < timeafter:
                weibo_data = weibo_data[0:i]
                break

        WbDataOper.add_all(weibo_data)
        # If the weibo isn't created after the given time, jump out the loop
        if i != length_weibo_data - 1:
            break

        domain = public.get_userdomain(html)
        cur_time = int(time.time() * 1000)
        ajax_url_0 = AJAX_URL.format(domain, 0, domain, uid, cur_page, cur_page, cur_time)
        ajax_url_1 = AJAX_URL.format(domain, 1, domain, uid, cur_page, cur_page, cur_time + 100)

        if cur_page == 1:
            if (datetime.now() - weibo_data[0].create_time).days < get_outdated_days():
                outdated = 0
            # here we use local call to get total page number
            limit = min(limit, get_total_page(crawl_ajax_page(ajax_url_1, 2)))
            auth_level = 1
        else:
            auth_level = 2

        app.send_task('tasks.home.crawl_ajax_page',
                      args=(ajax_url_0, auth_level),
                      queue='ajax_home_crawler',
                      routing_key='ajax_home_info')

        app.send_task('tasks.home.crawl_ajax_page',
                      args=(ajax_url_1, auth_level),
                      queue='ajax_home_crawler',
                      routing_key='ajax_home_info')
        cur_page += 1

    SeedidsOper.set_seed_home_crawled(uid, outdated)


@app.task(ignore_result=True)
def crawl_weibo_data_newest(uid):
    limit = get_max_home_page()
    cur_page = 1
    while cur_page <= limit:
        url = HOME_URL.format(uid, cur_page)
        if cur_page == 1:
            html = get_page(url, auth_level=1)
        else:
            html = get_page(url, auth_level=2)
        weibo_data = get_data(html)

        if not weibo_data:
            crawler.warning("user {} has no weibo".format(uid))
            return
        if WbDataOper.get_wb_by_mid(weibo_data[0].weibo_id):
            return

        # Check whether weibo created after time in spider.yaml
        length_weibo_data = len(weibo_data)
        for i in range(0, len(weibo_data)):
            weibo_time = weibo_data[i].create_time
            if weibo_time < timeafter:
                weibo_data = weibo_data[0:i]
                break

        WbDataOper.add_all(weibo_data)
        # If the weibo isn't created after the given time, jump out the loop
        if i != length_weibo_data - 1:
            break

        domain = public.get_userdomain(html)
        cur_time = int(time.time() * 1000)
        ajax_url_0 = AJAX_URL.format(domain, 0, domain, uid, cur_page, cur_page, cur_time)
        ajax_url_1 = AJAX_URL.format(domain, 1, domain, uid, cur_page, cur_page, cur_time + 100)

        if cur_page == 1:
            # here we use local call to get total page number
            limit = min(limit, get_total_page(crawl_ajax_page(ajax_url_1, 2)))
            auth_level = 1
        else:
            auth_level = 2

        app.send_task('tasks.home.crawl_ajax_page_newest',
                      args=(ajax_url_0, auth_level),
                      queue='ajax_home_newest_crawler',
                      routing_key='ajax_home_newest_info')
        app.send_task('tasks.home.crawl_ajax_page_newest',
                      args=(ajax_url_1, auth_level),
                      queue='ajax_home_newest_crawler',
                      routing_key='ajax_home_newest_info')
        cur_page += 1


@app.task(ignore_result=True)
def crawl_weibo_data_collection(uid):
    limit = get_max_home_page()
    cur_page = 1
    last_mid, last_updated = LastCache.get_home_last(uid)
    if not last_mid or not last_updated:
        last_mid, last_updated = HomeCollectionOper.get_last(uid)

    while cur_page <= limit:
        url = HOME_URL.format(uid, cur_page)
        if cur_page == 1:
            html = get_page(url, auth_level=1)
        else:
            html = get_page(url, auth_level=2)
        weibo_data = get_data(html)

        if not weibo_data:
            crawler.warning("user {} has no weibo".format(uid))
            return

        length_weibo_data = len(weibo_data)
        for i in range(0, len(weibo_data)):
            weibo_time = weibo_data[i].create_time
            if weibo_time < timeafter \
                    or last_mid and last_mid == weibo_data[i].weibo_id \
                    or last_updated and last_updated > weibo_data[i].create_time:
                weibo_data = weibo_data[0:i]
                break

        WbDataOper.add_all(weibo_data)
        if weibo_data and cur_page == 1:
            LastCache.set_home_last(uid, weibo_data[0].weibo_id, weibo_data[0].create_time)
        # If the weibo isn't created after the given time, jump out the loop
        if i != length_weibo_data - 1:
            break

        domain = public.get_userdomain(html)
        cur_time = int(time.time() * 1000)
        ajax_url_0 = AJAX_URL.format(domain, 0, domain, uid, cur_page, cur_page, cur_time)
        ajax_url_1 = AJAX_URL.format(domain, 1, domain, uid, cur_page, cur_page, cur_time + 100)

        if cur_page == 1:
            # here we use local call to get total page number
            limit = min(limit, get_total_page(crawl_ajax_page(ajax_url_1, 2)))
            auth_level = 1
        else:
            auth_level = 2

        app.send_task('tasks.home.crawl_ajax_page_collection',
                      args=(ajax_url_0, auth_level, last_mid, last_updated),
                      queue='ajax_home_collection_crawler',
                      routing_key='ajax_home_collection_info')
        app.send_task('tasks.home.crawl_ajax_page_collection',
                      args=(ajax_url_1, auth_level, last_mid, last_updated),
                      queue='ajax_home_collection_crawler',
                      routing_key='ajax_home_collection_info')
        cur_page += 1


@app.task
def execute_home_task():
    # you can have many strategies to crawl user's home page, here we choose table seed_ids's uid
    # whose home_crawl is 0
    id_objs = SeedidsOper.get_home_ids()
    for id_obj in id_objs:
        app.send_task('tasks.home.crawl_weibo_data', args=(id_obj.uid,), queue='home_crawler',
                      routing_key='home_info')


@app.task
def execute_home_newest_task():
    # you can have many strategies to crawl user's home page, here we choose table seed_ids's uid
    # whose home_crawl is 0
    id_objs = SeedidsOper.get_home_ids_active()
    for id_obj in id_objs:
        app.send_task('tasks.home.crawl_weibo_data_newest', args=(id_obj.uid,), queue='home_newest_crawler',
                      routing_key='home_newest_info')


@app.task
def execute_home_collection_task():
    uids = HomeCollectionOper.get_uids()
    for uid in uids:
        app.send_task('tasks.home.crawl_weibo_data_collection', args=(uid,), queue='home_collection_crawler',
                      routing_key='home_collection_info')
