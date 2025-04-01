SELECT  bill.FId         AS "id"
       ,bill_m.fmodifytime AS "修改时间"
       ,bill.FPKID       AS "多选基础资料id"
       ,bill.FBasedataId AS "对应基础资料id"
FROM crrc_secd.tk_crrc_book_copyunit bill
LEFT JOIN crrc_secd.tk_crrc_bizcontactbook bill_m ON bill.FId = bill_m.FId