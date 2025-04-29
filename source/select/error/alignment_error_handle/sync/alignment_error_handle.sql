select
    bill.fid as "id",
    bill.fbillno as "单据编号",
    bill.fmodifytime as "修改时间",
    project_.fnumber as "项目号",
    project_.fname as "项目名称",
    project_.fk_crrc_textfield2 as "项目简称",
    bill.fk_crrc_lcnum as "列车号",
    bill.fk_crrc_jcnums as "校线节车号",
    bill.fk_crrc_discrib as "现象描述",
    gxx.fname as "构型项名称",
    gxx.fnumber as "构型项编码",
    bill.fk_crrc_phenom1 as "现象分类",
    create_user.fnumber as "提报人工号",
    create_user.FTRUENAME as "提报人姓名",
    bill.fcreatetime as "提报时间",
    case bill.fk_crrc_corrective
        when 'crrc_metro_groupclass' then '内部责任(城轨事业部)'
        when 'bd_supplier' then '外部责任(供应商)'
        else bill.fk_crrc_corrective
    end as "整改类别",
    change_user.fnumber as "整改人工号",
    change_user.FTRUENAME as "整改人姓名",
    bill.fk_crrc_textfield1 as "所属班组",
    trans_user.fnumber as "转交人工号",
    trans_user.FTRUENAME as "转交人姓名",
    respond_user.fnumber as "响应人工号",
    respond_user.FTRUENAME as "响应人姓名",
    bill.fauditdate as "响应时间",
    case bill.fbillstatus
        when 'A' then '暂存'
        when 'B' then '待响应'
        when 'C' then '已响应'
        when 'D' then '转交中'
        when 'E' then '已处理'
        when 'F' then '已关闭'
        else bill.fbillstatus
    END AS "单据状态"
FROM
    crrc_secd.tk_crrc_unqualify bill
    LEFT JOIN crrc_secd.tk_crrc_project1 project_ ON bill.fk_crrc_basedatafield = project_.fid
    LEFT JOIN crrc_secd.tk_crrc_gxx gxx on bill.fk_crrc_gxnumber = gxx.fid
    LEFT JOIN crrc_sys.t_sec_user create_user ON bill.fcreatorid = create_user.fid
    LEFT JOIN crrc_sys.t_sec_user change_user ON bill.fk_crrc_userfield1 = change_user.fid
    LEFT JOIN crrc_sys.t_sec_user trans_user ON bill.fk_crrc_userfield3 = trans_user.fid
    LEFT JOIN crrc_sys.t_sec_user respond_user ON bill.fauditorid = respond_user.fid