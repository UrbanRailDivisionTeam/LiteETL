select
    bill.fid as "id",
    bill.fmodifytime as "修改时间",
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