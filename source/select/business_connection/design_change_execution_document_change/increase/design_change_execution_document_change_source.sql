SELECT
    bill.FEntryId AS "id",
    bill_m.fmodifytime AS "修改时间"
FROM
    crrc_secd.tk_crrc_filechgentry bill
    LEFT JOIN crrc_secd.tk_crrc_chgbill bill_m ON bill_m.FId = bill.FId