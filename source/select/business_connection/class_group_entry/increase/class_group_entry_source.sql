SELECT
       bill.FEntryId AS "id",
       bill_m.fmodifytime AS "单据头修改时间"
FROM
       crrc_secd.tk_crrc_classgroupentry bill
       LEFT JOIN crrc_secd.tk_crrc_classgroup bill_m ON bill.FID = bill_m.FID