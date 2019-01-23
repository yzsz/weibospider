from datetime import timedelta, datetime, time
from functools import partial
from urllib import parse as url_parse
from logger import crawler
from .workers import app
from page_get import get_page
from config import get_max_search_page
from page_parse import search as parse_search
from db.dao import (KeywordsOper, KeywordsDataOper, WbDataOper)
from db.redis_db import LastCache
from decorators import session_used
from kafka import producer
from config import read_provinces
import re

# This url is just for original weibos.
# If you want other kind of search, you can change the url below
# But if you change this url, maybe you have to rewrite some part of the parse code
URL = 'http://s.weibo.com/weibo/{}&scope=ori&suball=1&page={}'
URL_CITY = 'http://s.weibo.com/weibo/{}&region=custom:{}&scope=ori&suball=1&page={}'
URL_TIMERANGE = 'http://s.weibo.com/weibo/{}&scope=ori&suball=1&timescope=custom:{}:{}&page={}'
URL_TIMERANGE_CITY = 'http://s.weibo.com/weibo/{}&region=custom:{}&scope=ori&suball=1&timescope=custom:{}:{}&page={}'
LIMIT = get_max_search_page() + 1


@session_used
@app.task(ignore_result=True)
def search_keyword(keyword, keyword_id):
    crawler.info('Searching keyword "{}"'.format(keyword))
    cur_page = 1
    encode_keyword = url_parse.quote(keyword)
    while cur_page < LIMIT:
        cur_url = URL.format(encode_keyword, cur_page)
        # current only for login, maybe later crawling page one without login
        search_page = get_page(cur_url, auth_level=2)
        if not search_page:
            crawler.error(
                'Searching for keyword {} failed in page {}, the source page url is {}'.format(keyword, cur_page,
                                                                                               cur_url))
            raise Exception('Cannot get page')

        search_list = parse_search.get_search_info(search_page)

        # Because the search results are sorted by time, if any result has been stored in mysql,
        # We need not crawl the same keyword in this turn
        for wb_data in search_list:
            rs = WbDataOper.get_wb_by_mid(wb_data.weibo_id)
            producer.produce_data(topic='test', data=wb_data)
            # todo incremental crawling using time
            if rs:
                crawler.info('Weibo {} has been crawled, skip it.'.format(wb_data.weibo_id))
                continue
            else:
                WbDataOper.add_one(wb_data)
                KeywordsDataOper.insert_keyword_wbid(keyword_id, wb_data.weibo_id)
                # todo: only add seed ids and remove this task
                app.send_task('tasks.user.crawl_person_infos', args=(wb_data.uid,), queue='user_crawler',
                              routing_key='for_user_info')
        if 'class=\"next\"' in search_page:
            cur_page += 1
        else:
            crawler.info('Keyword {} has been crawled in this turn'.format(keyword))
            return


@session_used
@app.task(ignore_result=True)
def search_keyword_city(keyword, keyword_id, province_city_id):
    crawler.info('Searching keyword "{}"'.format(keyword))
    cur_page = 1
    encode_keyword = url_parse.quote(keyword)
    last_mid, last_updated = LastCache.get_search_last(keyword_id)

    while cur_page < LIMIT:
        cur_url = URL_CITY.format(encode_keyword, province_city_id, cur_page)
        # current only for login, maybe later crawling page one without login
        search_page = get_page(cur_url, auth_level=2)
        if not search_page:
            crawler.error(
                'Searching for keyword {} failed in page {}, the source page url is {}'.format(keyword, cur_page,
                                                                                               cur_url))
            raise Exception('Cannot get page')

        search_list = parse_search.get_search_info(search_page)

        search_list_length = len(search_list)
        last_index = search_list_length - 1
        keyword_wb_list = []
        for i in range(0, len(search_list)):
            if not KeywordsOper.get_searched_keyword_wbid(keyword_id, search_list[i].weibo_id):
                keyword_wb_list.append([keyword_id, search_list[i].weibo_id])
            app.send_task('tasks.user.crawl_person_infos', args=(search_list[i].uid,), queue='user_crawler',
                          routing_key='for_user_info')
            if last_mid and last_mid == search_list[i].weibo_id \
                    or last_updated and last_updated > search_list[i].create_time:
                search_list = search_list[0:i]
                last_index = i
                break
        wbdata_list = filter(lambda w: not WbDataOper.get_wb_by_mid(w.weibo_id), search_list)

        WbDataOper.add_all(wbdata_list)
        KeywordsDataOper.insert_keyword_wbid_list(keyword_wb_list)
        if search_list and cur_page == 1:
            LastCache.set_search_last(keyword_id, search_list[0].weibo_id, search_list[0].create_time)
        if last_index != search_list_length - 1:
            break

        if 'class=\"next\"' in search_page:
            cur_page += 1
        else:
            crawler.info('Keyword {} has been crawled in this turn'.format(keyword))
            return


@app.task(ignore_result=True)
@session_used
def search_keyword_timerange_all(keyword, keyword_id, date, hour1, hour2):
    task_url = partial(URL_TIMERANGE.format, url_parse.quote(keyword),
                       '%s-%i' % (date, hour1),
                       '%s-%i' % (date, hour2))
    __search_history(task_url, keyword_id, keyword, 'all')


@app.task(ignore_result=True)
@session_used
def search_keyword_timerange_city(keyword, keyword_id, date, hour1, hour2, province_city_id):
    task_url = partial(URL_TIMERANGE_CITY.format, url_parse.quote(keyword), province_city_id,
                       "%s-%i" % (date, hour1),
                       "%s-%i" % (date, hour2))
    __search_history(task_url, keyword_id, keyword, province_city_id)


def __search_history(task_url, keyword_id, keyword, area):
    cur_page = 1

    while cur_page < LIMIT:
        cur_url = task_url(cur_page)

        search_page = get_page(cur_url, auth_level=2)
        if not search_page:
            crawler.error('Searching for keyword {} failed in page {}, the source page url is {} ({})'
                          .format(keyword, cur_page, cur_url, area))
            raise Exception('Cannot get page')

        if cur_page == 1 and 'noresult_tit' in search_page:
            crawler.info('Keyword {} query has no result ({})'.format(keyword, area))
            return

        search_list = parse_search.get_search_info(search_page)

        # yzsz: Changed insert logic here for possible duplicate weibos from other tasks
        wb_data_list = []
        keyword_timerange_wbid_list = []
        for wb_data in search_list:
            rs = WbDataOper.get_wb_by_mid(wb_data.weibo_id)
            wid = KeywordsOper.get_searched_keyword_timerange_wbid(keyword_id, wb_data.weibo_id)
            if not rs:
                wb_data_list.append(wb_data)
            if not wid:
                keyword_timerange_wbid_list.append([keyword_id, wb_data.weibo_id])
            # send task for crawling user info
            app.send_task('tasks.user.crawl_person_infos', args=(wb_data.uid,), queue='user_crawler',
                          routing_key='for_user_info')
        WbDataOper.add_all(wb_data_list)
        KeywordsDataOper.insert_keyword_timerange_wbid_list(keyword_timerange_wbid_list)

        if 'class=\"next\"' in search_page:
            cur_page += 1
        else:
            crawler.info('Keyword {} has been crawled in this turn ({})'.format(keyword, area))
            return


@app.task(ignore_result=True)
def execute_search_task():
    keywords = KeywordsOper.get_search_keywords()
    for each in keywords:
        if not re.match(r"\d+:\d+", each[2]):
            app.send_task('tasks.search.search_keyword', args=(each[0], each[1]), queue='search_crawler',
                          routing_key='for_search_info')


@app.task(ignore_result=True)
def execute_search_city_task():
    keywords = KeywordsOper.get_search_keywords()
    for each in keywords:
        if re.match(r"\d+:\d+", each[2]):
            app.send_task('tasks.search.search_keyword_city', args=(each[0], each[1], each[2]),
                          queue='search_city_crawler',
                          routing_key='for_search_city_info')


def __dosth_timerange(time_start, time_end, f):
    time_cur = time_start
    while time_cur <= time_end:
        f(time_cur=time_cur)
        time_cur += timedelta(hours=1)


def __single_search_timerange_task_all(keyword, keyword_id, time_cur):
    app.send_task('tasks.search.search_keyword_timerange_all',
                  args=(keyword, keyword_id,
                        time_cur.date().isoformat(), time_cur.hour, (time_cur + timedelta(hours=1)).hour),
                  queue='search_timerange_crawler',
                  routing_key='for_search_timerange_info')


def __single_search_timerange_task_city(keyword, keyword_id, time_cur, province_city_id):
    app.send_task('tasks.search.search_keyword_timerange_city',
                  args=(keyword, keyword_id,
                        time_cur.date().isoformat(), time_cur.hour, (time_cur + timedelta(hours=1)).hour,
                        province_city_id),
                  queue='search_timerange_crawler',
                  routing_key='for_search_timerange_info')


def __execute_search_timerange_task_all(time_start, time_end, keyword, keyword_id):
    __dosth_timerange(time_start, time_end,
                      partial(__single_search_timerange_task_all,
                              keyword=keyword, keyword_id=keyword_id))


def __execute_search_timerange_task_any(time_start, time_end, keyword, keyword_id):
    provinces = read_provinces()
    for province_city_id in provinces:
        __execute_search_timerange_task_city(time_start, time_end, keyword, keyword_id, province_city_id)


def __execute_search_timerange_task_city(time_start, time_end, keyword, keyword_id, province_city_id):
    __dosth_timerange(time_start, time_end,
                      partial(__single_search_timerange_task_city,
                              keyword=keyword, keyword_id=keyword_id,
                              province_city_id=province_city_id))


@app.task(ignore_result=True)
def execute_search_timerange_task():
    __func = {'all': __execute_search_timerange_task_all,
              'any': __execute_search_timerange_task_any}
    keywords_timerange = KeywordsOper.get_search_keywords_timerange()
    for each_timerange in keywords_timerange:
        time_start = datetime.combine(each_timerange[2], time(hour=each_timerange[3]))
        time_end = datetime.combine(each_timerange[4], time(hour=each_timerange[5]))
        keyword = each_timerange[0]
        keyword_id = each_timerange[1]
        area = each_timerange[6]
        if re.match(r"\d+:\d+", area):
            __execute_search_timerange_task_city(time_start, time_end, keyword, keyword_id, area)
        else:
            __execute_search_timerange = __func.get(area, __execute_search_timerange_task_all)
            __execute_search_timerange(time_start, time_end, keyword, keyword_id)


@app.task(ignore_result=True)
def temp_search_timerange():
    app.send_task('tasks.search.search_keyword_timerange_all',
                  args=('林芝', 5, '2017-11-18', 6, 7),
                  queue='search_timerange_crawler',
                  routing_key='for_search_timerange_info')
