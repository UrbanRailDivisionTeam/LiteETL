SELECT
    bill.fid as "id",
    bill.FNUMBER as "编码",
    bill.FNAME as "名称",
    bill.FDESCRIPTION as "描述",
    _org.FNAME as "所属组织",
    bill.FENTRABILL as "单据",
    _type.FNAME as "类别名称",
    _type.FDESCRIPTION as "类别描述",
    bill.FVERSION as "版本",
    bill.FOperation as "启动操作",
    CASE bill.ftype
        WHEN 'AuditFlow' THEN '审批流'
        WHEN 'BizFlow' THEN '业务流'
        ELSE bill.ftype
    END as "流程类型",
    create_user.fnumber as "创建人工号",
    create_user.ftruename as "创建人姓名",
    modeify_user.fnumber as "修改人工号",
    modeify_user.ftruename as "修改人姓名",
    bill.fcreatedate as "创建日期",
    bill.fmodifydate as "修改日期",
    bill.FPARENTPROCID as "父流程ID",
    bill.fentrabillid as "入口单据id",
    bill.FPNGID as "流程SVG资源ID",
    bill.FDEPLOYMENTID as "部署ID",
    bill.FBPMNXMLID as "BPMNXMLID",
    bill.FGRAPHID as "设计器图形ID",
    bill.fpublish as "是否发布",
    bill.fdiscard as "是否废弃",
    bill.fapplicationid as "应用ID",
    bill.fbusinessid as "流程标识",
    bill.forgviewid as "组织类型",
    bill.ftemplatenumber as "流程模板编码",
    bill.ftemplateversion as "流程模板版本"
FROM
    crrc_wfs.t_wf_model bill
    LEFT JOIN crrc_sys.t_org_org _org ON bill.FORGUNITID = _org.fid
    LEFT JOIN crrc_wfs.T_wf_proccate _type ON bill.FCATEGORY = _type.fid
    LEFT JOIN crrc_sys.t_sec_user create_user ON bill.FCREATORID = create_user.fid
    LEFT JOIN crrc_sys.t_sec_user modeify_user ON bill.FMODIFIERID = modeify_user.fid
    LEFT JOIN crrc_sys.t_meta_entitydesign ed ON bill.fentrabillid = ed.fid