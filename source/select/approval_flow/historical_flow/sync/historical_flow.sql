SELECT
    bill.fid as "id",
    bill.FPROCDEFID as "流程定义ID",
    bill.FPROCINSTID as "流程实例ID",
    bill.FBUSINESSKEY as "业务主键",
    bill.FENDTIME as "结束时间",
    bill.fduration as "总办理时长",
    bill.frealduration as "真正办理时长",
    start_user.fnumber as "发起人工号",
    start_user.ftruename as "发起人姓名",
    bill.fstartname as "发起人",
    bill.fstarusernameformat as "发起人姓名格式化",
    bill.FSTARTACTID as "开始节点ID",
    bill.FENDACTID as "结束节点ID",
    bill.FSUPERPROCINSTID as "父流程实例ID",
    bill.frootprocessinstanceid as "根流程实例ID",
    bill.FDELETEREASON as "删除原因",
    bill.FCreateDate as "创建时间",
    bill.FModifyDate as "最后修改时间",
    bill.fbillno as "单据编码",
    bill.fentitynumber as "实体编码",
    bill.FNAME as "名称",
    bill.FDESCRIPTION as "描述",
    bill.fsubject as "单据主题",
    bill.factivityname as "节点名称",
    bill.fentrabillname as "入口单据名称",
    _org.FNAME as "组织名称",
    bill.forgviewid as "组织类型",
    bill.fschemeid as "方案id",
    bill.ftestingplanid as "测试计划id",
    create_user.fnumber as "创建人工号",
    create_user.ftruename as "创建人姓名",
    modeify_user.fnumber as "修改人工号",
    modeify_user.ftruename as "修改人姓名",
    CASE bill.fendtype
        WHEN 10 THEN '正常结束'
        WHEN 20 THEN '提交撤回结束'
        WHEN 30 THEN '审批终止'
        WHEN 40 THEN '管理员强制终止'
        WHEN 50 THEN '例外终止'
        WHEN 60 THEN '整单撤回终止'
        WHEN 25 THEN '终止'
        WHEN 70 THEN '父流程跳转结束'
        WHEN 80 THEN '父流程终止结束'
        WHEN 90 THEN '父流程整单撤回结束'
        ELSE bill.fendtype
    END as "结束类型",
    bill.fbiztraceno as "业务跟踪号",
    bill.fbilltype as "业务单据类型",
    bill.fbusinessid as "流程标识",
    case bill.fprocesstype
        when 'AuditFlow' then '审批流'
        when 'BizFlow' then '业务流'
        ELSE bill.fprocesstype
    end as "流程类型",
    case bill.fpriorityshow
        when 'transfer' then '移交给我的'
        ELSE bill.fpriorityshow
    end as "标记显示"
FROM
    crrc_wfs.T_WF_HIPROCINST bill
    LEFT JOIN crrc_sys.t_sec_user start_user ON bill.FSTARTUSERID = start_user.fid
    LEFT JOIN crrc_sys.t_org_org _org ON bill.fmainorgid = _org.fid
    LEFT JOIN crrc_sys.t_sec_user create_user ON bill.fcreatorid = create_user.fid
    LEFT JOIN crrc_sys.t_sec_user modeify_user ON bill.fmodifierid = modeify_user.fid