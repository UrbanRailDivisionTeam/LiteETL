SELECT
       bill.FEntryId AS "id",
       bill_m.fmodifytime AS "单据头修改时间",
       bill.fk_crrc_datereleased AS "发放日期"
FROM
       crrc_secd.tk_crrc_designchgentry bill
       LEFT JOIN crrc_secd.tk_crrc_designchgcenter bill_m ON bill.fid = bill_m.fid