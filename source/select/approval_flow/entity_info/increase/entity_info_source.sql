SELECT
    bill.FENTRYID as "id",
    bill.FISDELETED as "是否准备删除",
    ed.FMODIFYDATE as "修改时间"
FROM
    crrc_sys.t_meta_entityinfo bill
    left join crrc_sys.t_meta_entitydesign ed on bill.FID = ed.fnumber