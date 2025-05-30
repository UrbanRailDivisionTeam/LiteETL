SELECT
    bill.Fid AS "id",
    bill.fcreatetime as "创建时间",
    bill.fmodifytime as "修改时间",
    bill.fk_crrc_textfield AS "申请人姓名",
    bill.fk_crrc_textfield1 AS "申请人身份证号",
    bill.fk_crrc_textfield15 AS "申请人联系电话",
    bill.fk_crrc_textfield2 AS "公司名称",
    bill.fk_crrc_combofield7 AS "是否签订过安全承诺书",
    bill.fk_crrc_integerfield AS "随行人数",
    bill.fk_crrc_combofield9 AS "是否为作业负责人",
    CASE 
        bill.fbillstatus
        WHEN 'A'
        THEN '暂存'
        WHEN 'B'
        THEN '已提交'
        WHEN 'C'
        THEN '已审核'
        ELSE '未知'
    END AS "单据状态", 
    bill.fk_crrc_combofield8 AS "作业状态",
    bill.fk_crrc_combofield1 AS "申请作业时间",
    bill.fk_crrc_datefield AS "计划开工日期",
    bill.fk_crrc_combofield10 AS "计划开工日期上午/下午",
    bill.fk_crrc_datefield1 AS "计划完工日期",
    bill.fk_crrc_combofield11 AS "计划完工日期上午/下午",
    bill.fk_crrc_mulcombofield1 AS "作业地点",
    zylxcs.fk_crrc_textfield AS "作业类型",
    zylxcs.fname AS "具体作业内容",
    bill.fk_crrc_textfield16 AS "项目名称",
    bill.fk_crrc_textfield17 AS "车号",
    bill.fk_crrc_textfield18 AS "台位/车道",
    bill.fk_crrc_combofield3 AS "作业依据",
    bill.fk_crrc_textfield19 AS "NCR/开口项/设计变更编号",
    bill.fk_crrc_mulcombofield AS "作业危险性",
    bill.fk_crrc_combofield4 AS "是否危险作业",
    bill.fk_crrc_combofield5 AS "是否需要监护人",
    bill.fk_crrc_combofield6 AS "是否需要作业证",
    bill.fk_crrc_combofield12 AS "是否携带危化品",
    bill.fk_crrc_combofield12 AS "携带危化品类型",
    create_user.fnumber AS "事业部对接人",
    bill.fk_crrc_textfield6 AS "事业部对接人姓名",
    bill.fk_crrc_textfield7 AS "事业部对接人部门",
    bill.fk_crrc_textfield20 AS "事业部对接人工号"
FROM
    crrc_secd.tk_crrc_xgfgl bill
    LEFT JOIN crrc_secd.tk_crrc_zylxcs zylxcs ON bill.fk_crrc_basedatafield = zylxcs.Fid
    LEFT JOIN crrc_sys.t_sec_user create_user ON bill.fk_crrc_userfield = create_user.FID