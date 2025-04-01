SELECT
    bill.FEntryId AS "id",
    bill_m.fmodifytime AS "修改时间"
FROM
    crrc_secd.tk_crrc_chgbill_chgentry bill
    LEFT JOIN crrc_secd.tk_crrc_chgbill bill_m ON bill.FID = bill_m.FID