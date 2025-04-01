SELECT
    bill.FEntryId AS "id",
    bill.fid AS "单据头id",
    bill_m.fmodifytime AS "修改时间",
    bill.FSeq AS "单据分录序号",
    bill.fk_crrc_groupnumber AS "所属组件编码",
    bill.fk_crrc_groupname AS "所属组件名称",
    bill.fk_crrc_blmaterialname AS "零件名称",
    bill.fk_crrc_materialnum AS "零件编码",
    bill.fk_crrc_chgafterdesc AS "更改描述",
    bill.fk_crrc_begintrack AS "开始车号",
    bill.fk_crrc_endtrack AS "结束车号",
    bill.fk_crrc_projectjch AS "节车号",
    bill.fk_crrc_changecar AS "变更车次"
FROM
    crrc_secd.tk_crrc_chgbill_chgentry bill
    LEFT JOIN crrc_secd.tk_crrc_chgbill bill_m ON bill.FID = bill_m.FID