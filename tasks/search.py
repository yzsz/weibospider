from datetime import timedelta
from urllib import parse as url_parse
from logger import crawler
from .workers import app
from page_get import get_page
from config import get_max_search_page
from page_parse import search as parse_search
from db.dao import (KeywordsOper, KeywordsDataOper, WbDataOper)
from decorators import session_used
from config import read_provinces
import re

# This url is just for original weibos.
# If you want other kind of search, you can change the url below
# But if you change this url, maybe you have to rewrite some part of the parse code
URL = 'http://s.weibo.com/weibo/{}&scope=ori&suball=1&page={}'
URL_TIMERANGE = 'http://s.weibo.com/weibo/{}&scope=ori&suball=1&timescope=custom:{}:{}&page={}'
URL_TIMERANGE_CITY = 'http://s.weibo.com/weibo/{}&region=custom:{}&scope=ori&suball=1&timescope=custom:{}:{}&page={}'
LIMIT = get_max_search_page() + 1


@app.task(ignore_result=True)
def search_keyword(keyword, keyword_id):
    crawler.info('We are searching keyword "{}"'.format(keyword))
    cur_page = 1
    encode_keyword = url_parse.quote(keyword)
    while cur_page < LIMIT:
        cur_url = URL.format(encode_keyword, cur_page)
        # current only for login, maybe later crawling page one without login
        search_page = get_page(cur_url, auth_level=2)
        if not search_page:
            crawler.warning('No search result for keyword {}, the source page is {}'.format(keyword, search_page))
            return

        search_list = parse_search.get_search_info(search_page)

        # Because the search results are sorted by time, if any result has been stored in mysql,
        # We need not crawl the same keyword in this turn
        for wb_data in search_list:
            rs = WbDataOper.get_wb_by_mid(wb_data.weibo_id)
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
        if cur_page == 1:
            cur_page += 1
        elif 'noresult_tit' not in search_page:
            cur_page += 1
        else:
            crawler.info('Keyword {} has been crawled in this turn'.format(keyword))
            return


@app.task(ignore_result=True)
@session_used
def search_keyword_timerange_all(keyword, keyword_id, date, hour):
    cur_page = 1
    encode_keyword = url_parse.quote(keyword)

    while cur_page < LIMIT:
        cur_url = URL_TIMERANGE.format(encode_keyword,
                                       '%s-%i' % (date, hour),
                                       '%s-%i' % (date, hour),
                                       cur_page)

        search_page = get_page(cur_url, auth_level=2)
        if not search_page:
            crawler.error('Searching for keyword {} failed in page {}, the source page url is {} (all)'
                          .format(keyword, cur_page, cur_url))
            raise Exception('Cannot get page')

        if cur_page == 1 and 'noresult_tit' in search_page:
            crawler.info('keyword {} has been crawled in this turn (all)'.format(keyword))
            return

        search_list = parse_search.get_search_info(search_page)

        # yzsz: Changed insert logic here for possible duplicate weibos from other tasks
        for wb_data in search_list:
            rs = WbDataOper.get_wb_by_mid(wb_data.weibo_id)
            wid = KeywordsOper.get_searched_keyword_timerange_wbid(keyword_id, wb_data.weibo_id)
            if not rs:
                WbDataOper.add_one(wb_data)
            if not wid:
                KeywordsDataOper.insert_keyword_timerange_wbid(keyword_id, wb_data.weibo_id)
            # send task for crawling user info
            app.send_task('tasks.user.crawl_person_infos', args=(wb_data.uid,), queue='user_crawler',
                          routing_key='for_user_info')

        if 'page next S_txt1 S_line1' in search_page:
            cur_page += 1
        else:
            crawler.info('keyword {} has been crawled in this turn (all)'.format(keyword))
            return


@app.task(ignore_result=True)
@session_used
def search_keyword_timerange_city(keyword, keyword_id, date, hour, province_city_id):
    cur_page = 1
    encode_keyword = url_parse.quote(keyword)

    while cur_page < LIMIT:
        cur_url = URL_TIMERANGE_CITY.format(encode_keyword, province_city_id,
                                            "%s-%i" % (date, hour),
                                            "%s-%i" % (date, hour),
                                            cur_page)

        search_page = get_page(cur_url, auth_level=2)
        if not search_page:
            crawler.warning(
                'Searching for keyword {} failed in page {}, the source page url is {} ({})'.format(keyword, cur_page,
                                                                                                    cur_url,
                                                                                                    province_city_id))
            raise Exception("Cannot get page")

        if cur_page == 1 and 'noresult_tit' in search_page:
            crawler.info('keyword {} has been crawled in this turn ({})'.format(keyword, province_city_id))
            return

        search_list = parse_search.get_search_info(search_page)

        # yzsz: Changed insert logic here for possible duplicate weibos from other tasks
        for wb_data in search_list:
            rs = WbDataOper.get_wb_by_mid(wb_data.weibo_id)
            wid = KeywordsOper.get_searched_keyword_timerange_wbid(keyword_id, wb_data.weibo_id)
            if not rs:
                WbDataOper.add_one(wb_data)
            if not wid:
                KeywordsDataOper.insert_keyword_timerange_wbid(keyword_id, wb_data.weibo_id, province_city_id)
            # send task for crawling user info
            app.send_task('tasks.user.crawl_person_infos', args=(wb_data.uid,), queue='user_crawler',
                          routing_key='for_user_info')

        if 'page next S_txt1 S_line1' in search_page:
            cur_page += 1
        else:
            crawler.info('keyword {} has been crawled in this turn ({})'.format(keyword, province_city_id))
            return


@app.task(ignore_result=True)
def execute_search_task():
    keywords = KeywordsOper.get_search_keywords()
    for each in keywords:
        app.send_task('tasks.search.search_keyword', args=(each[0], each[1]), queue='search_crawler',
                      routing_key='for_search_info')


def __execute_search_timerange_task_all(each_timerange):
    date_cur = each_timerange[2]

    if (each_timerange[4] - date_cur).days == 0:
        for hour in range(each_timerange[3], each_timerange[5], 1):
            app.send_task('tasks.search.search_keyword_timerange_all',
                          args=(each_timerange[0], each_timerange[1],
                                date_cur.isoformat(), hour),
                          queue='search_timerange_crawler',
                          routing_key='for_search_timerange_info')

    else:
        delta = timedelta(days=1)
        for hour in range(each_timerange[3], 24, 1):
            app.send_task('tasks.search.search_keyword_timerange_all',
                          args=(each_timerange[0], each_timerange[1],
                                date_cur.isoformat(), hour),
                          queue='search_timerange_crawler',
                          routing_key='for_search_timerange_info')
        date_cur += delta

        while date_cur < each_timerange[4]:
            for hour in range(0, 24, 1):
                app.send_task('tasks.search.search_keyword_timerange_all',
                              args=(each_timerange[0], each_timerange[1],
                                    date_cur.isoformat(), hour),
                              queue='search_timerange_crawler',
                              routing_key='for_search_timerange_info')
            date_cur += delta

        for hour in range(0, each_timerange[5], 1):
            app.send_task('tasks.search.search_keyword_timerange_all',
                          args=(each_timerange[0], each_timerange[1],
                                date_cur.isoformat(), hour),
                          queue='search_timerange_crawler',
                          routing_key='for_search_timerange_info')


def __execute_search_timerange_task_any(each_timerange):
    provinces = read_provinces()
    for province_city_id in provinces:
        __execute_search_timerange_task_city(each_timerange, province_city_id)


def __execute_search_timerange_task_city(each_timerange, province_city_id):
    date_cur = each_timerange[2]

    if (each_timerange[4] - date_cur).days == 0:
        for hour in range(each_timerange[3], each_timerange[5], 1):
            app.send_task('tasks.search.search_keyword_timerange_city',
                          args=(each_timerange[0], each_timerange[1],
                                date_cur.isoformat(), hour, province_city_id),
                          queue='search_timerange_crawler',
                          routing_key='for_search_timerange_info')

    else:
        delta = timedelta(days=1)
        for hour in range(each_timerange[3], 24, 1):
            app.send_task('tasks.search.search_keyword_timerange_city',
                          args=(each_timerange[0], each_timerange[1],
                                date_cur.isoformat(), hour, province_city_id),
                          queue='search_timerange_crawler',
                          routing_key='for_search_timerange_info')
        date_cur += delta

        while date_cur < each_timerange[4]:
            for hour in range(0, 24, 1):
                app.send_task('tasks.search.search_keyword_timerange_city',
                              args=(each_timerange[0], each_timerange[1],
                                    date_cur.isoformat(), hour, province_city_id),
                              queue='search_timerange_crawler',
                              routing_key='for_search_timerange_info')
            date_cur += delta

        for hour in range(0, each_timerange[5], 1):
            app.send_task('tasks.search.search_keyword_timerange_city',
                          args=(each_timerange[0], each_timerange[1],
                                date_cur.isoformat(), hour, province_city_id),
                          queue='search_timerange_crawler',
                          routing_key='for_search_timerange_info')


@app.task(ignore_result=True)
def execute_search_timerange_task():
    __func = {'all': __execute_search_timerange_task_all,
              'any': __execute_search_timerange_task_any}
    keywords_timerange = KeywordsOper.get_search_keywords_timerange()
    for each_timerange in keywords_timerange:
        if re.match("\d+:\d+", each_timerange[6]):
            __execute_search_timerange_task_city(each_timerange, each_timerange[6])
        else:
            __execute_search_timerange = __func.get(each_timerange[6], __execute_search_timerange_task_all)
            __execute_search_timerange(each_timerange)


@app.task(ignore_result=True)
def temp_search_timerange():
    app.send_task('tasks.search.search_keyword_timerange_all',
                  args=('', 1, '2018-01-01', 0),
                  queue='search_timerange_crawler',
                  routing_key='for_search_timerange_info')
