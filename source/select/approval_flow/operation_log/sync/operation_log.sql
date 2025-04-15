SELECT
    bill.fid as "id",
    bill.fmodifydate as "修改日期",
    comment.fcurrentsubject as "任务执行结果的任务主题",
    comment.FEXECUTIONTYPE as "执行类型",
    bill.fbizidentifykey as "业务标识",
    bill.ftaskid as "任务ID",
    bill.factivityid as "活动实例ID",
    bill.fstep as "步骤",
    owner_user.fnumber as "处理人工号",
    owner_user.ftruename as "处理人姓名",
    bill.fowner as "处理人",
    assignee_user.fnumber as "目标人工号",
    assignee_user.ftruename as "目标人姓名",
    bill.fassignee as "目标人",
    bill.ftype as "操作类型",
    bill.fprocdefid as "流程定义ID",
    bill.fprocinstid as "流程实例ID",
    bill.fbusinesskey as "业务ID",
    bill.fbillno as "表单编号",
    bill.fterminalway as "终端类型",
    bill.factivityname as "活动实例名称",
    bill.fnopinion as "n意见",
    bill.fopinion as "意见",
    bill.fnote as "补充描述",
    bill.fbiznote as "业务关键信息",
    bill.fcreatedate as "创建日期",
    bill.fresultnumber as "结果编码",
    bill.fresultname as "结果名称",
    bill.fdecisiontype as "决策类型",
    bill.fispublic as "是否公开",
    bill.fnote_summary as "补充描述摘要"
FROM
    crrc_wfs.t_wf_operationlog bill
    LEFT JOIN crrc_wfs.T_WF_HICOMMENT comment ON bill.fcommentid = comment.fid
    LEFT JOIN crrc_sys.t_sec_user owner_user ON bill.fownerid = owner_user.fid
    LEFT JOIN crrc_sys.t_sec_user assignee_user ON bill.fassigneeid = assignee_user.fid