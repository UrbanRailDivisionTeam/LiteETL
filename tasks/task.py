import polars as pl
from dataclasses import dataclass
from sqlalchemy import text, TextClause
from tasks.base import task
from utils.config import CONFIG
from utils.connect import CONNECTER


@dataclass
class sync_data:
    name: str
    source: str
    target: str
    source_sql: TextClause
    taget_table: str

class sync(task):
    def __init__(self, data: sync_data) -> None:
        super().__init__(data.name)
        self.data = data
        self.source = CONNECTER.get(data.source)
        self.target = CONNECTER.get(data.target)
        self.log.info(f"任务{self.data.name}初始化完成")

    def __del__(self):
        self.source.close()
        self.target.close()

    def task_main(self):
        df = pl.read_database(self.data.source_sql, self.source, batch_size=10000)
        index = df.write_database(self.data.taget_table, self.target, if_table_exists="replace")
        self.log.debug(f"已成功从{self.data.source}全量抽取并写入到{self.data.target}的{self.data.taget_table},抽取了{str(index)}行")


@dataclass
class increase_data:
    name: str
    source: str
    target: str
    source_sync_sql: TextClause
    source_increase_sql: TextClause
    source_entry_sync_sql: list
    taget_table: str
    taget_increase_sql: TextClause

class increase(task):
    def __init__(self, data: increase_data) -> None:
        super().__init__(data.name)
        self.data = data
        self.source = CONNECTER.get(data.source)
        self.target = CONNECTER.get(data.target)
        self.log.info(f"任务{self.data.name}初始化完成")

    def task_main(self):
        source_increase_df = pl.read_database(self.data.source_increase_sql, self.source, batch_size=10000)
        taget_increase_df = pl.read_database(self.data.taget_increase_sql, self.target, batch_size=10000)

        new_diff = source_increase_df.join(taget_increase_df, left_on="id")
        del_diff = taget_increase_df.join(source_increase_df, left_on="id")
        if source_increase_df.width != 1:
            change_diff = source_increase_df.join(taget_increase_df, on="id", how="inner").filter(
                pl.any_horizontal(pl.col(source_increase_df.columns[1:]) != pl.col(taget_increase_df.columns[1:]))
            ).select("id")
        else:
            change_diff = pl.DataFrame()
        
        # 如果超过要求大小，退化为全量同步
        change_len = new_diff.height + change_diff.height
        if change_len > CONFIG.MAX_INCREASE_CHANGE:
            self.log.debug(f"变更行数{str(change_len)}行，超过规定{str(CONFIG.MAX_INCREASE_CHANGE)},退化为全量同步")
            df = pl.read_database(self.data.source_sync_sql, self.source, batch_size=10000)
            index = df.write_database(self.data.taget_table, self.target, if_table_exists="replace")
            self.log.debug(f"已成功从{self.data.source}全量抽取并写入到{self.data.target}的{self.data.taget_table},抽取了{str(index)}行")
            return
        
        if del_diff.height + change_diff.height > 0:
            ids_to_delete = del_diff["id"].to_list() + change_diff["id"].to_list()
            delete_statement = text(f"DELETE FROM your_table WHERE id IN ({','.join(map(str, ids_to_delete))})")
            self.target.execute(delete_statement)
            self.target.commit()
            
        if new_diff.height + change_diff.height > 0:
            ids_to_select = new_diff["id"].to_list() + change_diff["id"].to_list()
            in_clause = ", ".join(map(str, ids_to_select))
            select_statement = text(f"SELECT * FROM {self.data.source_sync_sql} AS subquery WHERE subquery.id in ({in_clause})")
            increase_df = pl.read_database(select_statement, self.source)
            increase_df.write_database(self.data.taget_table, self.target, if_table_exists="append") # 这里一定能添加成功，因为重复的行数已经删除了
            
        self.log.debug("已成功完成该表的增量同步")
    