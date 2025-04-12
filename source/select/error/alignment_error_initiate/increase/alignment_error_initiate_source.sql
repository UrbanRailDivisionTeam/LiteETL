select
    bill.fid as "id",
    bill.fmodifytime as "修改时间",
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
