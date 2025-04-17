SELECT
    bill.FENTRYID as "id",
    bill.FID as "应用名称",
    bill.FSEQ as "序列",
    bill.FMAINENTITYID as "对应单据头id",
    bill.FENTITYID as "单据id",
    bill.FTABLENAME as "对应的数据库表名",
    bill.FTABLEID as "数据库表名id",
    bill.FISDELETED as "是否准备删除",
    ed.FMODIFYDATE as "修改时间"
FROM
    crrc_sys.t_meta_entityinfo bill
    left join crrc_sys.t_meta_entitydesign ed on bill.FID = ed.fnumber