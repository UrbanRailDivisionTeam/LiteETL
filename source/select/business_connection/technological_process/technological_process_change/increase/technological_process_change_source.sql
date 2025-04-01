SELECT
    bill.FEntryId AS "id",
    bill_m.fmodifytime AS "修改时间"
FROM
    crrc_secd.tk_crrc_craftchangeentry bill
    LEFT JOIN crrc_secd.tk_crrc_craftchangebill bill_m ON bill_m.fid = bill.fid