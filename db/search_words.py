# -*-coding:utf-8 -*-
from sqlalchemy import text
from db.basic_db import db_session
from db.models import KeyWords, KeyWordsTimerange, KeywordsTimerangeWbdata
from decorators.decorator import db_commit_decorator


def get_search_keywords():
    return db_session.query(KeyWords.keyword, KeyWords.id).filter(text('enable=1')).all()


def get_search_keywords_timerange():
    return db_session.query(KeyWordsTimerange.keyword, KeyWordsTimerange.id,
                            KeyWordsTimerange.start_date, KeyWordsTimerange.start_hour,
                            KeyWordsTimerange.end_date, KeyWordsTimerange.end_hour)\
        .filter(text('enable=1')).all()


def get_searched_keyword_timerange_wbid(keyword_id, wbid):
    return db_session.query(KeywordsTimerangeWbdata.id)\
        .filter(KeywordsTimerangeWbdata.keyword_timerange_id == keyword_id)\
        .filter(KeywordsTimerangeWbdata.wb_id == wbid).all()


@db_commit_decorator
def set_useless_keyword(keyword):
    search_word = db_session.query(KeyWords).filter(KeyWords.keyword == keyword).first()
    search_word.enable = 0
    db_session.commit()
