SELECT
    bill.FEntryId AS "id",
    bill.fk_crrc_flowtime AS "操作时间"
FROM
    crrc_secd.tk_crrc_flowentry bill