# -*-coding:utf-8 -*-
from db.basic_db import db_session
from db.models import KeywordsWbdata, KeywordsTimerangeWbdata
from decorators.decorator import db_commit_decorator


@db_commit_decorator
def insert_keyword_wbid(keyword_id, wbid):
    keyword_wbdata = KeywordsWbdata()
    keyword_wbdata.wb_id = wbid
    keyword_wbdata.keyword_id = keyword_id
    db_session.add(keyword_wbdata)
    db_session.commit()


@db_commit_decorator
def insert_keyword_timerange_wbid(keyword_timerange_id, wbid):
    keyword_wbdata_timerange = KeywordsTimerangeWbdata()
    keyword_wbdata_timerange.wb_id = wbid
    keyword_wbdata_timerange.keyword_timerage_id = keyword_timerange_id
    db_session.add(keyword_wbdata_timerange)
    db_session.commit()
