from sqlalchemy import text
from sqlalchemy.exc import IntegrityError as SqlalchemyIntegrityError
from pymysql.err import IntegrityError as PymysqlIntegrityError
from sqlalchemy.exc import InvalidRequestError

from .basic import db_session
from .models import (
    LoginInfo, KeywordsWbdata, KeyWords, SeedIds, UserRelation,
    WeiboComment, WeiboRepost, User, WeiboData, WeiboPraise,
    KeyWordsTimerange, KeywordsTimerangeWbdata, HomeCollections,
    HomeIds
)
from decorators import db_commit_decorator


class CommonOper:
    @classmethod
    @db_commit_decorator
    def add_one(cls, data):
        db_session.add(data)
        db_session.commit()

    @classmethod
    @db_commit_decorator
    def add_all(cls, data_all):
        try:
            db_session.add_all(data_all)
            db_session.commit()
        except (SqlalchemyIntegrityError, PymysqlIntegrityError, InvalidRequestError):
            for data in data_all:
                cls.add_one(data)


class LoginInfoOper:
    @classmethod
    def get_login_info(cls):
        print("db_session hashkey: " + str(db_session.hash_key))
        return db_session.query(LoginInfo.name, LoginInfo.password, LoginInfo.enable) \
            .filter(text('enable=1')).all()

    @classmethod
    @db_commit_decorator
    def freeze_account(cls, name, rs):
        """
        :param name: login account
        :param rs: 0 stands for banned，1 stands for normal，2 stands for name or password is invalid
        :return:
        """
        account = db_session.query(LoginInfo).filter(LoginInfo.name == name).first()
        account.enable = rs
        db_session.commit()


class KeywordsDataOper:
    @classmethod
    @db_commit_decorator
    def insert_keyword_wbid(cls, keyword_id, wbid):
        keyword_wbdata = KeywordsWbdata()
        keyword_wbdata.wb_id = wbid
        keyword_wbdata.keyword_id = keyword_id
        db_session.add(keyword_wbdata)
        db_session.commit()

    @classmethod
    @db_commit_decorator
    def insert_keyword_timerange_wbid(cls, keyword_timerange_id, wbid, city=''):
        keyword_wbdata_timerange = KeywordsTimerangeWbdata()
        keyword_wbdata_timerange.wb_id = wbid
        keyword_wbdata_timerange.keyword_timerange_id = keyword_timerange_id
        keyword_wbdata_timerange.city = city
        db_session.add(keyword_wbdata_timerange)
        db_session.commit()

    @classmethod
    @db_commit_decorator
    def insert_keyword_timerange_wbid_list(cls, input_list):
        keyword_wbdata_timerange_list = [cls.__create_keyword_wbdata_timerange(item) for item in input_list]
        db_session.add_all(keyword_wbdata_timerange_list)
        db_session.commit()

    @classmethod
    def __create_keyword_wbdata_timerange(cls, item):
        keyword_wbdata_timerange = KeywordsTimerangeWbdata()
        keyword_wbdata_timerange.wb_id = item[0]
        keyword_wbdata_timerange.keyword_timerange_id = item[1]
        if len(item) >= 3:
            keyword_wbdata_timerange.city = item[2]
        else:
            keyword_wbdata_timerange.city = ''

    @classmethod
    def get_keyword_last(cls, keyword_id):
        return db_session.query(WeiboData.weibo_id, WeiboData.create_time.desc()) \
            .join(KeywordsWbdata).on(WeiboData.weibo_id == KeywordsWbdata.wb_id) \
            .join(KeyWords).on(KeywordsWbdata.keyword_id == KeyWords.id) \
            .filter(KeyWords.id == keyword_id).first()


class KeywordsOper:
    @classmethod
    def get_search_keywords(cls):
        return db_session.query(KeyWords.keyword, KeyWords.area, KeyWords.id).filter(text('enable=1')).all()

    @classmethod
    def get_search_keywords_timerange(cls):
        return db_session.query(KeyWordsTimerange.keyword, KeyWordsTimerange.id,
                                KeyWordsTimerange.start_date, KeyWordsTimerange.start_hour,
                                KeyWordsTimerange.end_date, KeyWordsTimerange.end_hour,
                                KeyWordsTimerange.area) \
            .filter(text('enable=1')).all()

    @classmethod
    def get_searched_keyword_timerange_wbid(cls, keyword_id, wbid):
        return db_session.query(KeywordsTimerangeWbdata.id) \
            .filter(KeywordsTimerangeWbdata.keyword_timerange_id == keyword_id) \
            .filter(KeywordsTimerangeWbdata.wb_id == wbid).first()

    @classmethod
    @db_commit_decorator
    def set_useless_keyword(cls, keyword):
        search_word = db_session.query(KeyWords).filter(KeyWords.keyword == keyword).first()
        search_word.enable = 0
        db_session.commit()


class SeedidsOper:
    @classmethod
    def get_seed_ids(cls):
        """
        Get all user ids to be crawled
        :return: user ids
        """
        return db_session.query(SeedIds.uid).filter(text('is_crawled=0')).all()

    @classmethod
    def get_home_ids(cls):
        """
        Get all user id who's home pages need to be crawled
        :return: user ids
        """
        return db_session.query(SeedIds.uid).filter(text('home_crawled=0')).all()

    @classmethod
    def get_home_ids_active(cls):
        """
        Get all active user ids
        :return: user ids
        """
        return db_session.query(SeedIds.uid).filter(text('home_crawled=1 and home_outdated=0')).all()

    @classmethod
    @db_commit_decorator
    def set_seed_crawled(cls, uid, result):
        """
        :param uid: user id that is crawled
        :param result: crawling result, 1 stands for succeed, 2 stands for fail
        :return: None
        """
        seed = db_session.query(SeedIds).filter(SeedIds.uid == uid).first()

        if seed and seed.is_crawled == 0:
            seed.is_crawled = result
        else:
            seed = SeedIds(uid=uid, is_crawled=result)
            db_session.add(seed)
        db_session.commit()

    @classmethod
    def get_seed_by_id(cls, uid):
        return db_session.query(SeedIds).filter(SeedIds.uid == uid).first()

    @classmethod
    @db_commit_decorator
    def insert_seeds(cls, ids):
        db_session.execute(SeedIds.__table__.insert().prefix_with('IGNORE'), [{'uid': i} for i in ids])
        db_session.commit()

    @classmethod
    @db_commit_decorator
    def set_seed_other_crawled(cls, uid):
        """
        update it if user id already exists, else insert
        :param uid: user id
        :return: None
        """
        seed = cls.get_seed_by_id(uid)
        if seed is None:
            seed = SeedIds(uid=uid, is_crawled=1, other_crawled=1, home_crawled=1)
            db_session.add(seed)
        else:
            seed.other_crawled = 1
        db_session.commit()

    @classmethod
    @db_commit_decorator
    def set_seed_home_crawled(cls, uid):
        """
        :param uid: user id
        :return: None
        """
        seed = cls.get_seed_by_id(uid)
        if seed is None:
            seed = SeedIds(uid=uid, is_crawled=0, other_crawled=0, home_crawled=1)
            db_session.add(seed)
        else:
            seed.home_crawled = 1
        db_session.commit()

    @classmethod
    @db_commit_decorator
    def set_seed_home_crawled(cls, uid, outdated):
        """
        :param uid: user id
        :param outdated: whether the user's home page is outdated
        :return: None
        """
        seed = cls.get_seed_by_id(uid)
        if seed is None:
            seed = SeedIds(uid=uid, is_crawled=0, other_crawled=0, home_crawled=1, home_outdated=outdated)
            db_session.add(seed)
        else:
            seed.home_crawled = 1
            seed.home_outdated = outdated
        db_session.commit()


class UserOper(CommonOper):
    @classmethod
    def get_user_by_uid(cls, uid):
        return db_session.query(User).filter(User.uid == uid).first()

    @classmethod
    def get_user_by_name(cls, user_name):
        return db_session.query(User).filter(User.name == user_name).first()


class UserRelationOper(CommonOper):
    @classmethod
    def get_user_by_uid(cls, uid, other_id, type):
        user = db_session.query(UserRelation).filter_by(user_id=uid, follow_or_fans_id=other_id).first()
        if user:
            return True
        else:
            return False


class WbDataOper(CommonOper):
    @classmethod
    def get_wb_by_mid(cls, mid):
        return db_session.query(WeiboData).filter(WeiboData.weibo_id == mid).first()

    @classmethod
    def get_weibo_comment_not_crawled(cls):
        return db_session.query(WeiboData.weibo_id).filter(text('comment_crawled=0')).all()

    @classmethod
    def get_weibo_praise_not_crawled(cls):
        return db_session.query(WeiboData.weibo_id).filter(text('praise_crawled=0')).all()

    @classmethod
    def get_weibo_repost_not_crawled(cls):
        return db_session.query(WeiboData.weibo_id, WeiboData.uid).filter(text('repost_crawled=0')).all()

    @classmethod
    def get_weibo_dialogue_not_crawled(cls):
        return db_session.query(WeiboData.weibo_id).filter(text('dialogue_crawled=0')).all()

    @classmethod
    @db_commit_decorator
    def set_weibo_comment_crawled(cls, mid):
        data = cls.get_wb_by_mid(mid)
        if data:
            data.comment_crawled = 1
            db_session.commit()

    @classmethod
    @db_commit_decorator
    def set_weibo_praise_crawled(cls, mid):
        data = cls.get_wb_by_mid(mid)
        if data:
            data.praise_crawled = 1
            db_session.commit()

    @classmethod
    @db_commit_decorator
    def set_weibo_repost_crawled(cls, mid):
        data = cls.get_wb_by_mid(mid)
        if data:
            data.repost_crawled = 1
            db_session.commit()

    @classmethod
    @db_commit_decorator
    def set_weibo_dialogue_crawled(cls, mid):
        data = cls.get_wb_by_mid(mid)
        if data:
            data.dialogue_crawled = 1
            db_session.commit()


class CommentOper(CommonOper):
    @classmethod
    def get_comment_by_id(cls, cid):
        return db_session.query(WeiboComment).filter(WeiboComment.comment_id == cid).first()


class PraiseOper(CommonOper):
    @classmethod
    def get_praise_by_id(cls, pid):
        return db_session.query(WeiboPraise).filter(WeiboPraise.weibo_id == pid).first()


class RepostOper(CommonOper):
    @classmethod
    def get_repost_by_rid(cls, rid):
        return db_session.query(WeiboRepost).filter(WeiboRepost.weibo_id == rid).first()


class HomeCollectionOper(CommonOper):
    @classmethod
    def get_uids(cls):
        return db_session.query(HomeIds.uid).join(HomeCollections) \
            .filter(HomeCollections.enabled) \
            .filter(HomeIds.home_collection_id == HomeCollections.id).all()

    @classmethod
    def get_home_last(cls, uid):
        return db_session.query(WeiboData.weibo_id, WeiboData.create_time.desc()) \
            .filter(WeiboData.uid == uid).first()
