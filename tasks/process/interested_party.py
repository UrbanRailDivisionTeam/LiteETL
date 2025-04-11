from tasks.process import process
from utils.connect import connect_data


class interested_party_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "相关方数据处理", "interested_party_process")

    def task_main(self) -> None:
        temp0 = self.connect.sql(
            f"""
                SELECT 
                    COUNT(ip.id) as _count
                FROM ods.interested_party_review ip
                WHERE
                    ip."单据状态" = '已审核'
            """
        ).fetchdf()
        temp1 = self.connect.sql(
            f"""
                WITH current_month AS (
                SELECT 
                    DATE_TRUNC('month', CURRENT_DATE) as month_start,
                    DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day' as month_end
                )
                SELECT 
                    COUNT(ip.id) as _count
                FROM ods.interested_party_review ip 
                CROSS JOIN current_month
                WHERE 
                    ip."单据状态" = '已审核'
                    AND 
                    (
                        ip."计划开工日期" BETWEEN month_start AND month_end
                        OR ip."计划完工日期" BETWEEN month_start AND month_end
                    )
            """
        ).fetchdf()
        temp2 = self.connect.sql(
            f"""
                WITH current_month AS (
                SELECT 
                    DATE_TRUNC('month', ip."计划开工日期") as month_start,
                    DATE_TRUNC('month', ip."计划完工日期") + INTERVAL '1 month' - INTERVAL '1 day' as month_end
                )
                SELECT 
                    COUNT(ip.id)  as _count
                FROM ods.interested_party_review ip 
                CROSS JOIN current_month
                WHERE 
                    ip."单据状态" = '已审核'
                    AND CURRENT_DATE BETWEEN month_start AND month_end
            """
        ).fetchdf()
        temp3 = self.connect.sql(
            f"""
                SELECT 
                    COUNT(ip.id) as _count
                FROM ods.interested_party_review ip
                WHERE
                    ip."单据状态" = '已审核'
                    AND ip."作业状态" = '正在作业'
            """
        ).fetchdf()
        temp4 = self.connect.sql(
            f"""
                SELECT 
                    COUNT(ip.id) as _count
                FROM ods.interested_party_review ip
                WHERE
                    ip."单据状态" = '已审核'
                    AND ip."作业状态" = '临时外出'
            """
        ).fetchdf()
        top_car_dict = {
            "进入公司的相关方总人数": int(temp0["_count"][0]),
            "本月进入车间人数": int(temp1["_count"][0]),
            "今日进入车间人数": int(temp2["_count"][0]),
            "当前在车间人数": int(temp3["_count"][0]),
            "今日临时外出人数": int(temp4["_count"][0])
        }
        temp5 = self.connect.sql(
            f"""
                SELECT 
                    ip."申请人姓名",
                    ip."申请人联系电话",
                    ip."作业地点",
                    ip."台位/车道",
                    ip."具体作业内容"
                FROM ods.interested_party_review ip
                WHERE
                    ip."单据状态" = '已审核'
                    AND ip."作业状态" = '正在作业'
                    AND ip."是否危险作业" = '是'
            """
        ).fetchdf()
        temp6 = self.connect.sql(
            f"""
                WITH current_month AS (
                SELECT 
                    DATE_TRUNC('month', ip."计划开工日期") as month_start,
                    DATE_TRUNC('month', ip."计划完工日期") + INTERVAL '1 month' - INTERVAL '1 day' as month_end
                )
                SELECT 
                    ip."作业类型",
                    COUNT(ip.id) as "作业人数"
                FROM ods.interested_party_review ip
                CROSS JOIN current_month
                WHERE
                    ip."单据状态" = '已审核'
                    AND ip."作业状态" = '正在作业'
                    AND CURRENT_DATE BETWEEN month_start AND month_end
                GROUP BY
                    ip."作业类型"
            """
        ).fetchdf()
        temp7 = self.connect.sql(
            f"""
                WITH current_month AS (
                SELECT 
                    DATE_TRUNC('month', CURRENT_DATE) as month_start,
                    DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day' as month_end
                )
                SELECT 
                    ip."作业类型",
                    COUNT(ip.id) as "作业人数"
                FROM ods.interested_party_review ip
                CROSS JOIN current_month
                WHERE
                    ip."单据状态" = '已审核'
                    AND ip."作业状态" = '正在作业'
                    AND 
                    (
                        ip."计划开工日期" BETWEEN month_start AND month_end
                        OR ip."计划完工日期" BETWEEN month_start AND month_end
                    )
                GROUP BY
                    ip."作业类型"
            """
        ).fetchdf()
        temp8 = self.connect.sql(
            f"""
                WITH RECURSIVE months AS (
                    SELECT DATE_TRUNC('month', CURRENT_DATE - INTERVAL '11 months') as month_date
                    UNION ALL
                    SELECT month_date + INTERVAL '1 month'
                    FROM months
                    WHERE month_date < DATE_TRUNC('month', CURRENT_DATE)
                )
                SELECT 
                    EXTRACT(YEAR FROM m.month_date) as year,
                    EXTRACT(MONTH FROM m.month_date) as month,
                    COUNT(DISTINCT ip.id) as "作业人数"
                FROM months m
                LEFT JOIN ods.interested_party_review ip ON 
                    (ip."计划开工日期" BETWEEN m.month_date AND (m.month_date + INTERVAL '1 month' - INTERVAL '1 day')
                    OR ip."计划完工日期" BETWEEN m.month_date AND (m.month_date + INTERVAL '1 month' - INTERVAL '1 day'))
                WHERE
                    ip."单据状态" = '已审核'
                GROUP BY year, month
                ORDER BY year, month
            """
        ).fetchdf()
        temp9 = self.connect.sql(
            f"""
                WITH RECURSIVE days AS (
                    SELECT DATE_TRUNC('day', CURRENT_DATE - INTERVAL '29 days') as work_date
                    UNION ALL
                    SELECT work_date + INTERVAL '1 day'
                    FROM days
                    WHERE work_date < CURRENT_DATE
                )
                SELECT 
                    d.work_date,
                    COUNT(DISTINCT ip.id) as "作业人数"
                FROM days d
                LEFT JOIN ods.interested_party_review ip ON (ip."计划开工日期" <= d.work_date AND ip."计划完工日期" >= d.work_date)
                WHERE
                    ip."单据状态" = '已审核'
                GROUP BY d.work_date
                ORDER BY d.work_date
            """
        ).fetchdf()
