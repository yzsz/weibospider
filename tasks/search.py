# coding:utf-8
from urllib import parse as url_parse
from logger.log import crawler
from tasks.workers import app
from page_get.basic import get_page
from config.conf import get_max_search_page
from page_parse import search as parse_search
from db.search_words import get_search_keywords, get_search_keywords_timerange
from db.keywords_wbdata import insert_keyword_wbid
from db.wb_data import insert_weibo_data, get_wb_by_mid

# This url is just for original weibos.
# If you want other kind of search, you can change the url below
url = 'http://s.weibo.com/weibo/{}&scope=ori&suball=1&page={}'
url_timerange = 'http://s.weibo.com/weibo/{}&scope=ori&suball=1&timescope=custom:{}:{}&page={}'
limit = get_max_search_page() + 1


@app.task(ignore_result=True)
def search_keyword(keyword, keyword_id):
    cur_page = 1
    encode_keyword = url_parse.quote(keyword)
    while cur_page < limit:
        cur_url = url.format(encode_keyword, cur_page)

        search_page = get_page(cur_url)
        if not search_page:
            crawler.warning('No result for keyword {}, the source page is {}'.format(keyword, search_page))
            return

        search_list = parse_search.get_search_info(search_page)

        # Because the search results are sorted by time, if any result has been stored in mysql,
        # we need not crawl the same keyword in this turn
        for wb_data in search_list:
            rs = get_wb_by_mid(wb_data.weibo_id)
            if rs:
                crawler.info('keyword {} has been crawled in this turn'.format(keyword))
                return
            else:
                insert_weibo_data(wb_data)
                insert_keyword_wbid(keyword_id, wb_data.weibo_id)
                # send task for crawling user info
                app.send_task('tasks.user.crawl_person_infos', args=(wb_data.uid,), queue='user_crawler',
                              routing_key='for_user_info')

        if 'page next S_txt1 S_line1' in search_page:
            cur_page += 1
        else:
            crawler.info('keyword {} has been crawled in this turn'.format(keyword))
            return


@app.task(ignore_result=True)
def excute_search_task():
    keywords = get_search_keywords()
    for each in keywords:
        app.send_task('tasks.search.search_keyword', args=(each[0], each[1]), queue='search_crawler',
                      routing_key='for_search_info')


@app.task(ignore_result=True)
def excute_search_timerange_task():
    keywords = get_search_keywords_timerange()
    for each in keywords:
        app.send_task('tasks.search.search_keyword_timerange',
                      args=(each[0], each[1], each[2], each[3], each[4], each[5]),
                      queue='search_crawler',
                      routing_key='for_search_info')
