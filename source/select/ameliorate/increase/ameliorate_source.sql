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