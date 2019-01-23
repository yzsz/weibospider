import datetime
from .basic import Base
from .tables import *


class LoginInfo(Base):
    __table__ = login_info


class User(Base):
    __table__ = wbuser

    def __init__(self, uid):
        self.uid = uid


class SeedIds(Base):
    __table__ = seed_ids


class KeyWords(Base):
    __table__ = keywords


class KeyWordsTimerange(Base):
    __table__ = keywords_timerange


class WeiboData(Base):
    __table__ = weibo_data

    def __repr__(self):
        return 'weibo url:{};mid:{};uid:{};weibo content:{};' \
               'location:{};create_time:{};weibo_img:{};weibo_img_path:{};' \
               'weibo_video:{};repost_num:{};comment_num:{};praise_num:{};' \
               'is_origin:{};device:{}'.format(
            self.weibo_url, self.weibo_id, self.uid, self.weibo_cont,
            self.weibo_location, str(self.create_time), self.weibo_img, self.weibo_img_path,
            self.weibo_video, str(self.repost_num), str(self.comment_num), str(self.praise_num),
            str(self.is_origin), self.device)


class KeywordsWbdata(Base):
    __table__ = keywords_wbdata


class KeywordsTimerangeWbdata(Base):
    __table__ = keywords_wbdata_timerange


class WeiboComment(Base):
    __table__ = weibo_comment

    def __repr__(self):
        return 'weibo_id:{},comment_id:{},comment_cont:{}'.format(self.weibo_id, self.comment_id, self.comment_cont)


class WeiboPraise(Base):
    __table__ = weibo_praise

    def __repr__(self):
        return 'user_id:{},weibo_id:{}'.format(self.user_id, self.weibo_id)


class WeiboRepost(Base):
    __table__ = weibo_repost

    def __repr__(self):
        return 'id:{},user_id:{},user_name:{},parent_user_id:{},parent_user_name:{}, weibo_url:{},weibo_id:{},' \
               'repost_time:{},repost_cont:{}'.format(self.id, self.user_id, self.user_name, self.parent_user_id,
                                                      self.parent_user_name, self.weibo_url, self.weibo_id,
                                                      self.repost_time, self.repost_cont)


class UserRelation(Base):
    __table__ = user_relation

    def __init__(self, uid, other_id, type, from_where, crawl_time=True):
        self.user_id = uid
        self.follow_or_fans_id = other_id
        self.type = type
        self.from_where = from_where
        if crawl_time:
            self.crawl_time = datetime.datetime.now()
        else:
            self.crawl_time = None

    def __repr__(self):
        return 'user_id:{},follow_or_fans_id:{},type:{},from_where:{}'.format(self.user_id, self.follow_or_fans_id,
                                                                              self.type, self.from_where)


class WeiboDialogue(Base):
    __table__ = weibo_dialogue

    def __repr__(self):
        return 'weibo_id:{},dialogue_id:{},dialogue_cont:{}'.format(self.weibo_id, self.dialogue_id, self.dialogue_cont)


class HomeCollections(Base):
    __table__ = home_collections


class HomeIds(Base):
    __table__ = home_ids
