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
    bill.fk_crrc_phenom as "现象分类",
    case bill.fk_crrc_corrective
        when 'crrc_metro_groupclass' then '内部责任(城轨事业部)'
        when 'bd_supplier' then '外部责任(供应商)'
        else bill.fk_crrc_corrective
    end as "整改类别",
    change_user.fnumber as "整改人工号",
    change_user.FTRUENAME as "整改人姓名",
    bill.fk_crrc_textfield5 as "整改班组",
    create_user.fnumber as "提报人工号",
    create_user.FTRUENAME as "提报人姓名",
    gxx1.fname as "下推构型项名称",
    gxx1.fnumber as "下推构型项编码",
    bill.fk_crrc_phenom1 as "下推现象分类",
    bill.fk_crrc_combofield as "初步异常诊断",
    bill.fk_crrc_combofield1 as "下一步诊断人",
    diagnosis_user.fnumber as "指定诊断人工号",
    diagnosis_user.FTRUENAME as "指定诊断人姓名",
    bill.fk_crrc_combofield7 as "构型名称",
    bill.fk_crrc_mode1 as "失效模式",
    bill.fk_crrc_combofield2 as "工序/工步",
    bill.fk_crrc_cause1 as "失效原因",
    bill.fk_crrc_textfield as "图文",
    bill.fk_crrc_textfield7 as "责任分类",
    bill.fk_crrc_textfield8 as "责任单位",
    bill.fk_crrc_textfield9 as "备注",
    bill.fk_crrc_record as "故障详细记录",
    bill.fk_crrc_textfield1 as "返工依据编号",
    deliver_user.fnumber as "转交处理人工号",
    deliver_user.FTRUENAME as "转交处理人姓名", 
    bill.fcreatetime as "响应时间",
    bill.fk_crrc_instructions as "整改返工描述",
    bill.fk_crrc_textfield4 as "任务跟踪人备注",
    bill.fauditdate as "验收时间",
    case bill.fbillstatus
        when 'A' then '处理中'
        when 'B' then '诊断中'
        when 'C' then '验收合格'
        when 'D' then '整改中'
        when 'F' then '验收中'
        else bill.fbillstatus
    END AS "单据状态"
FROM
    crrc_secd.tk_crrc_unqualifydeal bill
    LEFT JOIN crrc_secd.tk_crrc_project1 project_ ON bill.fk_crrc_projnum = project_.fid
    LEFT JOIN crrc_secd.tk_crrc_gxx gxx on bill.fk_crrc_gxnumber = gxx.fid
    LEFT JOIN crrc_sys.t_sec_user change_user ON bill.fk_crrc_user = change_user.fid
    LEFT JOIN crrc_sys.t_sec_user create_user ON bill.fk_crrc_userfield = create_user.fid
    LEFT JOIN crrc_secd.tk_crrc_gxx gxx1 on bill.fk_crrc_gxnumber1 = gxx1.fid
    LEFT JOIN crrc_sys.t_sec_user diagnosis_user ON bill.fk_crrc_userfield2 = diagnosis_user.fid
    LEFT JOIN crrc_sys.t_sec_user deliver_user ON bill.fk_crrc_userfield3 = deliver_user.fid
