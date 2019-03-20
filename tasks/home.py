import time
from datetime import datetime

from decorators import session_used
from logger import crawler
from .workers import app
from page_parse.user import public
from page_get import get_page
from config import (get_max_home_page, get_time_after, get_outdated_days)
from db.dao import (WbDataOper, SeedidsOper, HomeCollectionOper)
from db.redis_db import LastCache
from page_parse.home import (get_data, get_ajax_data, get_total_page)
from kafka import producer

# only crawls origin weibo
HOME_URL = 'http://weibo.com/u/{}?is_ori=1&is_tag=0&profile_ftype=1&page={}'
HOME_URL_2 = 'http://weibo.com/p/{}/home?is_ori=1&is_tag=0&profile_ftype=1&page={}'
AJAX_URL = 'http://weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain={}&pagebar={}&is_ori=1&id={}{}&page={}' \
           '&pre_page={}&__rnd={}'
TIMEAFTER = datetime.strptime(get_time_after(), '%Y-%m-%d %H:%M:%S')


@session_used
@app.task(ignore_result=True)
def crawl_ajax_page(domain, ajax_id, uid, cur_page, cur_time, auth_level,
                    use_cache: bool, last_mid=None, last_updated=None,
                    further_process: bool = False, further_topic=None):
    url = AJAX_URL.format(domain, ajax_id, domain, uid, cur_page, cur_page, cur_time)

    ajax_html = get_page(url, auth_level, is_ajax=True)
    ajax_weibo_data, outdated_data_encountered = __weibo_html_parse(ajax_html,
                                                                    use_cache, True, last_mid, last_updated)

    if ajax_weibo_data:
        if use_cache:
            if further_process:
                producer.produce_data_list(further_topic, ajax_weibo_data)
            WbDataOper.check_add_all(ajax_weibo_data)
        else:
            WbDataOper.add_all(ajax_weibo_data)
    else:
        crawler.warning('Home page fetching or parsing of user {} failed', uid)
        raise Exception('Cannot get or parse page')

    return ajax_html


@session_used
@app.task(ignore_result=True)
def crawl_weibo_data(uid):
    __home_crawl(uid, use_cache=False)


@session_used
@app.task(ignore_result=True)
def crawl_weibo_data_collection(uid):
    __home_crawl(uid, use_cache=True)


@session_used
@app.task(ignore_result=True)
def crawl_weibo_data_monitor(uid, monitor_topic):
    __home_crawl(uid, use_cache=True, further_process=True, further_topic=monitor_topic)


def __home_crawl(uid, use_cache: bool, further_process: bool = False, further_topic=None):
    limit = get_max_home_page()
    cur_page = 1
    outdated = 0
    last_mid, last_updated = None, None
    if use_cache:
        last_mid, last_updated = LastCache.get_home_last(uid)

    while cur_page <= limit:
        if len(uid) < 16:
            url = HOME_URL.format(uid, cur_page)
        else:
            url = HOME_URL_2.format(uid, cur_page)
        if cur_page == 1:
            html = get_page(url, auth_level=1)
        else:
            html = get_page(url, auth_level=2)

        weibo_data, outdated_data_encountered = __weibo_html_parse(html, use_cache, False, last_mid, last_updated)
        if weibo_data:
            if use_cache:
                if further_process:
                    producer.produce_data_list(further_topic, weibo_data)
                WbDataOper.check_add_all(weibo_data)
            else:
                WbDataOper.add_all(weibo_data)
            if weibo_data[0] and cur_page == 1:
                if use_cache:
                    LastCache.set_home_last(uid, weibo_data[0].weibo_id, weibo_data[0].create_time)
                if (datetime.now() - weibo_data[0].create_time).days >= get_outdated_days():
                    outdated = 1
            if outdated_data_encountered:
                break
        else:
            crawler.warning('Home page fetching or parsing of user {} failed', uid)
            raise Exception('Cannot get or parse page')

        domain = public.get_userdomain(html)
        cur_time = int(time.time() * 1000)
        if cur_page == 1:
            auth_level = 1
        else:
            auth_level = 2

        app.send_task('tasks.home.crawl_ajax_page',
                      args=(domain, 0, uid, cur_page, cur_time, auth_level,
                            use_cache, last_mid, last_updated,
                            further_process, further_topic),
                      queue='ajax_home_crawler',
                      routing_key='ajax_home_info')
        if cur_page == 1:
            # here we use local call to get total page number
            limit = min(limit, get_total_page(crawl_ajax_page(domain, 1, uid, cur_page, cur_time + 100, 2,
                                                              use_cache, last_mid, last_updated,
                                                              further_process, further_topic)))
        else:
            app.send_task('tasks.home.crawl_ajax_page',
                          args=(domain, 1, uid, cur_page, cur_time + 100, auth_level,
                                use_cache, last_mid, last_updated,
                                further_process, further_topic),
                          queue='ajax_home_crawler',
                          routing_key='ajax_home_info')

        cur_page += 1

    SeedidsOper.set_seed_home_crawled(uid, outdated)


def __weibo_html_parse(html, use_cache: bool, ajax_source: bool, last_mid=None, last_updated=None):
    outdated_data_encountered = False

    if not html:
        return None
    if ajax_source:
        weibo_data = get_ajax_data(html)
    else:
        weibo_data = get_data(html)
    if not weibo_data:
        return None

    for i in range(0, len(weibo_data)):
        weibo_time = weibo_data[i].create_time
        if weibo_time < TIMEAFTER or \
                use_cache and \
                (last_mid == weibo_data[i].weibo_id or
                 last_updated > weibo_time):
            weibo_data = weibo_data[0:i]
            outdated_data_encountered = True
            break

    return weibo_data, outdated_data_encountered


@app.task
def execute_home_task():
    # you can have many strategies to crawl user's home page, here we choose table seed_ids's uid
    # whose home_crawl is 0
    id_objs = SeedidsOper.get_home_ids()
    for id_obj in id_objs:
        app.send_task('tasks.home.crawl_weibo_data', args=(id_obj.uid,),
                      queue='home_crawler',
                      routing_key='home_info')


@app.task
def execute_home_newest_task():
    # you can have many strategies to crawl user's home page, here we choose table seed_ids's uid
    # whose home_crawl is 0
    id_objs = SeedidsOper.get_home_ids_active()
    for id_obj in id_objs:
        app.send_task('tasks.home.crawl_weibo_data', args=(id_obj.uid,),
                      queue='home_newest_crawler',
                      routing_key='home_newest_info')


@app.task
def execute_home_collection_task():
    id_objs = HomeCollectionOper.get_uids()
    for id_obj in id_objs:
        app.send_task('tasks.home.crawl_weibo_data_collection', args=(id_obj.uid,),
                      queue='home_collection_crawler',
                      routing_key='home_collection_info')


@app.task
def execute_home_eq_monitor_task():
    id_objs = HomeCollectionOper.get_uids_monitored('eq')
    for id_obj in id_objs:
        app.send_task('tasks.home.crawl_weibo_data_monitor', args=(id_obj.uid, 'eq_monitor'),
                      queue='home_monitor_crawler',
                      routing_key='home_monitor_info')
