SELECT
    bill.fid as "id",
    bill.fmodifytime as "修改时间",
    bill.fk_crrc_number as "工号对应id",
    bill.fk_crrc_month as "月份",
    bill.fk_crrc_datefield as "变动日期",
    CASE bill.fbillstatus
        WHEN 'A' THEN '暂存'
        WHEN 'B' THEN '已提交'
        WHEN 'A' THEN '已审核'
    END AS "单据状态",
    create_user.fnumber as "创建人工号",
    create_user.FTRUENAME as "创建人姓名"
FROM
    crrc_secd.tk_crrc_hr_transfer bill
    LEFT JOIN crrc_sys.t_sec_user create_user ON bill.fcreatorid = create_user.fid