SELECT
    bill.FEntryId AS "id",
    bill.fk_crrc_enauditdate AS "审核日期"
FROM
    crrc_secd.tk_crrc_auditentry bill