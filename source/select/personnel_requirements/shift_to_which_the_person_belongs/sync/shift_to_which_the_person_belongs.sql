SELECT
    bill.fid as "id",
    bill.fnumber as "单据编码",
    bill.fmodifytime as "修改时间",
    bill.fname as "班组名称",
    CASE bill.fstatus
        WHEN 'A' THEN '暂存'
        WHEN 'B' THEN '已提交'
        WHEN 'C' THEN '已审核'
    END as "数据状态",
    create_user.fnumber as "创建人工号",
    create_user.FTRUENAME as "创建人姓名",
    bill.fenable as "使用状态",
    bill.fmasterid as "主数据内码"
FROM
    crrc_secd.tk_crrc_hrbz bill
    LEFT JOIN crrc_sys.t_sec_user create_user ON bill.fcreatorid = create_user.fid