SELECT
    bill.FEntryId as "id",
    bill_m.fmodifytime as "修改时间"
FROM
    crrc_secd.tk_crrc_entry_pre_transfe bill
    LEFT JOIN crrc_secd.tk_crrc_pre_transfer bill_m ON bill.fid = bill_m.fid