SELECT
    f.fk_crrc_proposal_no as "id",
    (
        case f.fk_crrc_proposal_status
            when "A" then "暂存"
            when "B" then "已提交"
            when "C" then "已审核"
            when "D" then "提前终止"
            else "未知的状态"
        end
    ) as "题案状态",
    f.fk_crrc_submit_date as "提交日期"
FROM
    crrc_secd.tk_crrc_jygs_evo_apl f
    left JOIN crrc_sys.t_sec_user creator_user on f.fk_crrc_creator = creator_user.FID
    left JOIN crrc_sys.t_sec_user f2nd_proposer_user on f.fk_crrc_2nd_proposer = f2nd_proposer_user.FID
    left JOIN crrc_sys.t_sec_user auditor_user on f.fk_crrc_auditor = auditor_user.FID
    left JOIN crrc_sys.t_sec_user f1st_unit_auditor_user on f.fk_crrc_1st_unit_auditor = f1st_unit_auditor_user.FID
    left JOIN crrc_sys.t_sec_user f2nd_unit_auditor_user on f.fk_crrc_2nd_unit_auditor = f2nd_unit_auditor_user.FID
    left JOIN crrc_sys.t_sec_user submit_user on f.fk_crrc_submit_user = submit_user.FID
    left JOIN crrc_sys.t_sec_user end_user on f.fk_crrc_end_user = end_user.FID
    left JOIN crrc_sys.t_org_org org1 on f.fk_crrc_org1 = org1.FID
    left JOIN crrc_sys.t_org_org org2 on f.fk_crrc_org2 = org2.FID
    left JOIN crrc_sys.t_org_org last_org on f.fk_crrc_last_org = last_org.FID