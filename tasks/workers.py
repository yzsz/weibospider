import os
from datetime import timedelta

from celery import Celery, platforms
from kombu import Exchange, Queue

from config import (
    get_broker_and_backend,
    get_redis_master
)

platforms.C_FORCE_ROOT = True

worker_log_path = os.path.join(os.path.dirname(os.path.dirname(__file__)) + '/logs', 'celery.log')
beat_log_path = os.path.join(os.path.dirname(os.path.dirname(__file__)) + '/logs', 'beat.log')
broker_and_backend = get_broker_and_backend()

tasks = [
    'tasks.login', 'tasks.user', 'tasks.search', 'tasks.home', 'tasks.comment',
    'tasks.repost', 'tasks.downloader', 'tasks.praise'
]

if isinstance(broker_and_backend, list):
    broker, backend = broker_and_backend
    app = Celery('weibo_task', include=tasks, broker=broker, backend=backend)
else:
    master = get_redis_master()
    app = Celery('weibo_task', include=tasks, broker=broker_and_backend)
    app.conf.update(
        BROKER_TRANSPORT_OPTIONS={'master_name': master},
    )

app.conf.update(
    CELERY_TIMEZONE='Asia/Shanghai',
    CELERY_ENABLE_UTC=True,
    CELERYD_LOG_FILE=worker_log_path,
    CELERYBEAT_LOG_FILE=beat_log_path,
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_TASK_SERIALIZER='json',
    CELERY_RESULT_SERIALIZER='json',
    CELERYBEAT_SCHEDULE={
        'login_task': {
            'task': 'tasks.login.execute_login_task',
            'schedule': timedelta(hours=20),
            'options': {'queue': 'login_queue', 'routing_key': 'for_login'}
        },
        'user_task': {
            'task': 'tasks.user.execute_user_task',
            'schedule': timedelta(minutes=3),
            'options': {'queue': 'user_crawler', 'routing_key': 'for_user_info'}
        },
        'search_task': {
            'task': 'tasks.search.execute_search_task',
            'schedule': timedelta(minutes=1),
            'options': {'queue': 'search_crawler', 'routing_key': 'for_search_info'}
        },
        'search_city_task': {
            'task': 'tasks.search.execute_search_city_task',
            'schedule': timedelta(minutes=10),
            'options': {'queue': 'search_city_crawler', 'routing_key': 'for_search_city_info'}
        },
        'home_task': {
            'task': 'tasks.home.execute_home_task',
            'schedule': timedelta(hours=24),
            'options': {'queue': 'home_crawler', 'routing_key': 'home_info'}
        },
        'home_newest_task': {
            'task': 'tasks.home.execute_home_newest_task',
            'schedule': timedelta(hours=72),
            'options': {'queue': 'home_newest_crawler', 'routing_key': 'home_newest_info'}
        },
        'home_collection_task': {
            'task': 'tasks.home.execute_home_collection_task',
            'schedule': timedelta(minutes=10),
            'options': {'queue': 'home_collection_crawler', 'routing_key': 'home_collection_info'}
        },
        'home_eq_monitor_task': {
            'task': 'tasks.home.execute_home_eq_monitor_task',
            'schedule': timedelta(minutes=1),
            'options': {'queue': 'home_monitor_crawler', 'routing_key': 'home_monitor_info'}
        },
        'comment_task': {
            'task': 'tasks.comment.execute_comment_task',
            'schedule': timedelta(hours=10),
            'options': {'queue': 'comment_crawler', 'routing_key': 'comment_info'}
        },
        'repost_task': {
            'task': 'tasks.repost.execute_repost_task',
            'schedule': timedelta(hours=10),
            'options': {'queue': 'repost_crawler', 'routing_key': 'repost_info'}
        },
        'dialogue_task': {
            'task': 'tasks.dialogue.execute_dialogue_task',
            'schedule': timedelta(hours=10),
            'options': {'queue': 'dialogue_crawler', 'routing_key': 'dialogue_info'}
        },
    },
    CELERY_QUEUES=(
        Queue('login_queue', exchange=Exchange('login_queue', type='direct'), routing_key='for_login'),

        Queue('user_crawler', exchange=Exchange('user_crawler', type='direct'), routing_key='for_user_info'),
        Queue('fans_followers', exchange=Exchange('fans_followers', type='direct'), routing_key='for_fans_followers'),

        Queue('search_city_crawler', exchange=Exchange('search_city_crawler', type='direct'),
              routing_key='for_search_city_info'),
        Queue('search_crawler', exchange=Exchange('search_crawler', type='direct'), routing_key='for_search_info'),
        Queue('search_timerange_crawler', exchange=Exchange('search_timerange_crawler', type='direct'),
              routing_key='for_search_timerange_info'),

        Queue('home_crawler', exchange=Exchange('home_crawler', type='direct'), routing_key='home_info'),
        Queue('home_newest_crawler', exchange=Exchange('home_newest_crawler', type='direct'),
              routing_key='home_newest_info'),
        Queue('home_collection_crawler', exchange=Exchange('home_collection_crawler', type='direct'),
              routing_key='home_collection_info'),
        Queue('home_monitor_crawler', exchange=Exchange('home_monitor_crawler', type='direct'),
              routing_key='home_monitor_info'),
        Queue('ajax_home_crawler', exchange=Exchange('ajax_home_crawler', type='direct'), routing_key='ajax_home_info'),

        Queue('comment_crawler', exchange=Exchange('comment_crawler', type='direct'), routing_key='comment_info'),
        Queue('comment_page_crawler', exchange=Exchange('comment_page_crawler', type='direct'),
              routing_key='comment_page_info'),

        Queue('praise_crawler', exchange=Exchange('praise_crawler', type='direct'), routing_key='praise_info'),
        Queue('praise_page_crawler', exchange=Exchange('praise_page_crawler', type='direct'),
              routing_key='praise_page_info'),

        Queue('repost_crawler', exchange=Exchange('repost_crawler', type='direct'), routing_key='repost_info'),
        Queue('repost_page_crawler', exchange=Exchange('repost_page_crawler', type='direct'),
              routing_key='repost_page_info'),

        Queue('dialogue_crawler', exchange=Exchange('dialogue_crawler', type='direct'), routing_key='dialogue_info'),
        Queue('dialogue_page_crawler', exchange=Exchange('dialogue_page_crawler', type='direct'),
              routing_key='dialogue_page_info'),

        Queue('download_queue', exchange=Exchange('download_queue', type='direct'), routing_key='for_download'),
    ),
)
