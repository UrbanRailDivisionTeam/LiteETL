select
    re2.FEntryId as "id",
    re2.FK_CRRC_TEXTFIELD2 as "位置号",
    re2.FK_CRRC_TEXTFIELD2 as "连接器代号",
    tsu.FTRUENAME as "创建人",
    re.FMODIFYTIME as "修改时间"
from
    crrc_secd.tk_crrc_wire_number_re_e2 re2
    left join crrc_secd.tk_crrc_wire_number_re re on re.FID = re2.FID
    left join crrc_sys.t_sec_user tsu on re.FCREATORID = tsu.FID