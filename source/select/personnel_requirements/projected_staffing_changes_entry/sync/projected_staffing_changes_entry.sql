SELECT
    bill.FEntryId as "id",
    bill.fid as "单据头id",
    bill_m.fmodifytime as "修改时间",
    CASE bill.fk_crrc_pre_trans
        WHEN 1 THEN '人员调动'
        WHEN 2 THEN '人员借出'
        WHEN 3 THEN '调休'
        WHEN 4 THEN '助勤'
        WHEN 5 THEN '人员借入'
        WHEN 6 THEN '回岗'
        WHEN 7 THEN '借入退回'
        WHEN 8 THEN '假勤等其他非作业情况'
    END AS "变动类别",
    CASE bill.fk_crrc_pre_contract
        WHEN 'A' THEN '正式员工'
        WHEN 'B' THEN '劳务派遣'
        WHEN 'C' THEN '劳务外包'
        WHEN 'D' THEN '临时借工'
    END AS "合同类别",
    CASE bill.fk_crrc_pre_trans
        WHEN 1 THEN '直接生产人员'
        WHEN 2 THEN '辅助生产人员'
        WHEN 3 THEN '辅助服务人员'
        WHEN 4 THEN '一般管理人员'
        WHEN 5 THEN '工艺技术人员'
    END AS "岗位类别",
    CASE bill.fk_crrc_pre_post_n
        WHEN 1 THEN '车辆电工'
        WHEN 2 THEN '车辆机械制修工'
        WHEN 3 THEN '粘接工'
        WHEN 4 THEN '落车工'
        WHEN 5 THEN '调车工'
        WHEN 6 THEN '调试工'
        WHEN 7 THEN '校线工'
        WHEN 8 THEN '辅料员'
        WHEN 9 THEN '交车员'
        WHEN 10 THEN '物料员'
        WHEN 11 THEN '检查工'
        WHEN 12 THEN '牵引台操作工'
        WHEN 13 THEN '仓储保管工'
        WHEN 14 THEN '叉车司机'
        WHEN 15 THEN '大巴司机'
        WHEN 16 THEN '监控员'
        WHEN 17 THEN '天车司机'
        WHEN 18 THEN '技术人员'
        WHEN 19 THEN '管理人员'
    END AS "岗位名称",
    bill.fk_crrc_number as "人数"
FROM
    crrc_secd.tk_crrc_entry_pre_transfe bill
    LEFT JOIN crrc_secd.tk_crrc_pre_transfer bill_m ON bill.fid = bill_m.fid