SELECT
    bill.FEntryId AS "id",
    bill.fk_crrc_enhandlerdate AS "经办日期"
FROM
    crrc_secd.tk_crrc_back_handleentry bill