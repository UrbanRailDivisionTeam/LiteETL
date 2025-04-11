import os
from utils import read_sql
from utils.config import DEBUG
from utils.connect import connect_data
from tasks.sync import init_warpper
from tasks.base import task, extract_increase, extract_increase_data
from tasks.process.interested_party import interested_party_process


@init_warpper
def init(connect_data: connect_data) -> list[task]:
    return [
        extract_increase(
            connect_data,
            extract_increase_data(
                name="相关方安全数据抽取",
                logger_name="interested_party",
                source="相关方数据库" if not DEBUG else "pgsql服务",
                source_sync_sql=read_sql(os.path.join("interested_party", "interested_party", "sync", "interested_party.sql")),
                source_increase_sql=read_sql(os.path.join("interested_party", "interested_party", "increase", "interested_party_source.sql")),
                target_table="interested_party",
                target_increase_sql=read_sql(os.path.join("interested_party", "interested_party", "increase", "interested_party_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="相关方安全随行人员数据抽取",
                logger_name="interested_party_entry",
                source="相关方数据库" if not DEBUG else "pgsql服务",
                source_sync_sql=read_sql(os.path.join("interested_party", "interested_party_entry", "sync", "interested_party_entry.sql")),
                source_increase_sql=read_sql(os.path.join("interested_party", "interested_party_entry", "increase", "interested_party_entry_source.sql")),
                target_table="interested_party_entry",
                target_increase_sql=read_sql(os.path.join("interested_party", "interested_party_entry", "increase", "interested_party_entry_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="相关方审批数据同步",
                logger_name="interested_party_review",
                source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
                source_sync_sql=read_sql(os.path.join("interested_party", "interested_party_review", "sync", "interested_party_review.sql")),
                source_increase_sql=read_sql(os.path.join("interested_party", "interested_party_review", "increase", "interested_party_review_source.sql")),
                target_table="interested_party_review",
                target_increase_sql=read_sql(os.path.join("interested_party", "interested_party_review", "increase", "interested_party_review_target.sql")),
            )
        ),
        interested_party_process(connect_data)
    ]
