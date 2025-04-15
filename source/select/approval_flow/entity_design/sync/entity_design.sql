SELECT
    bill.fid as "id",
    bill.FCREATEDATE as "创建时间",
    bill.FMODIFYDATE as "修改时间",
    bill.FNUMBER as "单据名称",
    bill.fdata as "单据xml数据"
FROM
    crrc_sys.t_meta_entitydesign bill