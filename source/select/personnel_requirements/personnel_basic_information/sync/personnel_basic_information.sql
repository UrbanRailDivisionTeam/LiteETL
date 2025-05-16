SELECT
    bill.fid AS "id",
    bill.fnumber AS "单据编码",
    bill.fmodifytime AS "修改时间",
    _user.fnumber AS "工号",
    _user.FTRUENAME AS "姓名",
    bill.fgroupid AS "人员所属班组id",
    bill.fk_crrc_job_type AS "岗位类别",
    CASE bill.fk_crrc_job_name
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
        WHEN 12 THEN '牵车台操作工'
        WHEN 13 THEN '仓储保管工'
        WHEN 14 THEN '叉车司机'
        WHEN 15 THEN '大巴司机'
        WHEN 16 THEN '监控员'
        WHEN 17 THEN '天车司机'
        WHEN 18 THEN '技术人员'
        WHEN 19 THEN '管理人员'
        ELSE bill.fk_crrc_job_name
    END AS "岗位名称",
    CASE bill.fk_crrc_contract_type
        WHEN 'a' THEN '正式员工'
        WHEN 'b' THEN '劳务派遣'
        WHEN 'c' THEN '劳务外包'
        WHEN 'd' THEN '临时借工'
        ELSE bill.fk_crrc_contract_type
    END AS "合同类别",
    bill.fk_crrc_onjob AS "是否在岗",
    bill.fk_crrc_attendance AS "是否助勤",
    CASE bill.fstatus
        WHEN 'A' THEN '暂存'
        WHEN 'B' THEN '已提交'
        WHEN 'C' THEN '已审核'
    END AS "是否助勤",
    create_user.fnumber AS "创建人工号",
    create_user.FTRUENAME AS "创建人姓名",
    CASE bill.fenable
        WHEN 0 THEN '禁用'
        WHEN 1 THEN '可用'
    END AS "使用状态",
    bill.fmasterid AS "主数据内码"
FROM
    crrc_secd.tk_crrc_base_user bill
    LEFT JOIN crrc_sys.t_sec_user _user ON bill.fk_crrc_number = _user.fid
    LEFT JOIN crrc_sys.t_sec_user create_user ON bill.fcreatorid = create_user.fid