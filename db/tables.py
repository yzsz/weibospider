from sqlalchemy import (
    Table, Column, INTEGER, String, Text, TIMESTAMP, DATE, DateTime, BOOLEAN, func)

from .basic import metadata

# login table
login_info = Table("login_info", metadata,
                   Column("id", INTEGER, primary_key=True, autoincrement=True),
                   Column("name", String(100), unique=True),
                   Column("password", String(200)),
                   Column("enable", INTEGER, default=1, server_default='1'),
                   )

# weibo user info
wbuser = Table("wbuser", metadata,
               Column("id", INTEGER, primary_key=True, autoincrement=True),
               Column("uid", String(20), unique=True),
               Column("name", String(200), default='', server_default=''),
               Column("gender", INTEGER, default=0, server_default='0'),
               Column("birthday", String(200), default='', server_default=''),
               Column("location", String(100), default='', server_default=''),
               Column("description", String(500), default='', server_default=''),
               Column("register_time", String(200), default='', server_default=''),
               Column("verify_type", INTEGER, default=0, server_default='0'),
               Column("verify_info", String(2500), default='', server_default=''),
               Column("follows_num", INTEGER, default=0, server_default='0'),
               Column("fans_num", INTEGER, default=0, server_default='0'),
               Column("wb_num", INTEGER, default=0, server_default='0'),
               Column("level", INTEGER, default=0, server_default='0'),
               Column("tags", String(500), default='', server_default=''),
               Column("work_info", String(500), default='', server_default=''),
               Column("contact_info", String(300), default='', server_default=''),
               Column("education_info", String(300), default='', server_default=''),
               Column("head_img", String(500), default='', server_default=''),
               )

# seed ids for user crawling
seed_ids = Table('seed_ids', metadata,
                 Column("id", INTEGER, primary_key=True, autoincrement=True),
                 Column("uid", String(20), unique=True),
                 Column("is_crawled", INTEGER, default=0, server_default='0'),
                 Column("other_crawled", INTEGER, default=0, server_default='0'),
                 Column("home_crawled", INTEGER, default=0, server_default='0'),
                 Column("home_outdated", INTEGER, default=0, server_default='1'),
                 )

# search keywords table
keywords = Table('keywords', metadata,
                 Column("id", INTEGER, primary_key=True, autoincrement=True),
                 Column("keyword", String(200), unique=True),
                 Column("enable", INTEGER, default=1, server_default='1'),
                 )

# search keywords with time ranges
keywords_timerange = Table('keywords_timerange', metadata,
                           Column("id", INTEGER, primary_key=True, autoincrement=True),
                           Column("keyword", String(200), unique=True),
                           Column("start_date", DATE),
                           Column("start_hour", INTEGER),
                           Column("end_date", DATE),
                           Column("end_hour", INTEGER),
                           Column("area", String(50)),
                           Column("enable", INTEGER, default=1, server_default='1'),
                           )

# weibo info data
weibo_data = Table('weibo_data', metadata,
                   Column("id", INTEGER, primary_key=True, autoincrement=True),
                   Column("weibo_id", String(200), unique=True),
                   Column("weibo_cont", Text),
                   Column("weibo_location", String(200)),
                   Column("weibo_img", String(1000)),
                   Column("weibo_img_path", String(1000), server_default=''),
                   Column("weibo_video", String(1000)),
                   Column("repost_num", INTEGER, default=0, server_default='0'),
                   Column("comment_num", INTEGER, default=0, server_default='0'),
                   Column("praise_num", INTEGER, default=0, server_default='0'),
                   Column("uid", String(20)),
                   Column("is_origin", INTEGER, default=1, server_default='1'),
                   Column("device", String(200), default='', server_default=''),
                   Column("weibo_url", String(300), default='', server_default=''),
                   Column("create_time", TIMESTAMP, index=True),
                   Column("comment_crawled", INTEGER, default=0, server_default='0'),
                   Column("repost_crawled", INTEGER, default=0, server_default='0'),
                   Column("dialogue_crawled", INTEGER, default=0, server_default='0'),
                   Column("praise_crawled", INTEGER, default=0, server_default='0'),
                   )

# keywords and weibodata relationship
keywords_wbdata = Table('keywords_wbdata', metadata,
                        Column("id", INTEGER, primary_key=True, autoincrement=True),
                        Column("keyword_id", INTEGER),
                        Column("wb_id", String(200)),
                        )

# time-ranged keywords and weibodata relationship
keywords_wbdata_timerange = Table('keywords_wbdata_timerange', metadata,
                                  Column("id", INTEGER, primary_key=True, autoincrement=True),
                                  Column("keyword_timerange_id", INTEGER),
                                  Column("wb_id", String(200)),
                                  Column("city", String(50), server_default='')
                                  )

# comment table
weibo_comment = Table('weibo_comment', metadata,
                      Column("id", INTEGER, primary_key=True, autoincrement=True),
                      Column("comment_id", String(50), unique=True),
                      Column("comment_cont", Text),
                      Column("comment_screen_name", Text),
                      Column("weibo_id", String(200)),
                      Column("user_id", String(20)),
                      Column("create_time", String(200)),
                      )

# praise table
weibo_praise = Table('weibo_praise', metadata,
                     Column("id", INTEGER, primary_key=True, autoincrement=True),
                     Column("user_id", String(20)),
                     Column("weibo_id", String(200)),
                     Column("crawl_time", TIMESTAMP),
                     )

# repost table
weibo_repost = Table("weibo_repost", metadata,
                     Column("id", INTEGER, primary_key=True, autoincrement=True),
                     Column("user_id", String(20)),
                     Column("user_name", String(200)),
                     Column("weibo_id", String(200), unique=True),
                     Column("parent_user_id", String(20)),
                     Column("repost_time", String(200)),
                     Column("repost_cont", Text),
                     Column("weibo_url", String(200)),
                     Column("parent_user_name", String(200)),
                     Column("root_weibo_id", String(200)),
                     )

# relations about user and there fans and follows
user_relation = Table("user_relation", metadata,
                      Column("id", INTEGER, primary_key=True, autoincrement=True),
                      Column("user_id", String(20)),
                      Column("follow_or_fans_id", String(20)),
                      Column("type", INTEGER),  # 1 stands for fans, 2 stands for follows
                      Column("from_where", String(60)),
                      Column("crawl_time", DateTime(3))  # DATETIME(6) means save 6 digits milliseconds
                      # time is stored in UTC
                      )

# dialogue table
weibo_dialogue = Table("weibo_dialogue", metadata,
                       Column("id", INTEGER, primary_key=True, autoincrement=True),
                       Column("dialogue_id", String(50), unique=True),
                       Column("weibo_id", String(200)),
                       Column("dialogue_cont", Text),
                       Column("dialogue_rounds", INTEGER),
                       )

# home collections table
home_collections = Table("home_collections", metadata,
                         Column("id", INTEGER, primary_key=True, nullable=False),
                         Column("description", String(200), nullable=False, default=''),
                         Column("enabled", BOOLEAN, nullable=False, default=False))

# monitoring ids table
home_ids = Table("home_ids", metadata,
                 Column("uid", String(20), nullable=False),
                 Column("home_collection_id", INTEGER, nullable=False),
                 Column("last_mid", String(200), nullable=False),
                 Column("last_updated", TIMESTAMP, nullable=False))

# uid and weibodata relationship
home_wbdata = Table("home_wbdata", metadata,
                    Column("uid", String(20), nullable=False),
                    Column("mid", String(200), nullable=False))

__all__ = ['login_info', 'wbuser', 'seed_ids', 'keywords', 'weibo_data', 'keywords_wbdata', 'weibo_comment',
           'weibo_repost', 'user_relation', 'weibo_dialogue', 'weibo_praise', 'keywords_timerange',
           'keywords_wbdata_timerange', 'home_collections', 'home_ids', 'home_wbdata']
