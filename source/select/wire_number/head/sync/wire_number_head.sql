select
    re.FEntryId as "id",
    re.FBILLNO as "单据编号",
    re.FMODIFYTIME as "修改时间",
    tsu.FTRUENAME as "创建人",
    re.FK_CRRC_INTEGERFIELD as "列车号",
    pro.fk_crrc_textfield2 as "项目名称",
    re_e1.FK_CRRC_COMBOFIELD as "规格型号",
    re_e1.FK_CRRC_TEXTFIELD as "上标",
    re_e1.FK_CRRC_TEXTFIELD1 as "下标"
from
    crrc_secd.tk_crrc_wire_number_re_e1 re_e1
    left join crrc_secd.tk_crrc_wire_number_re re on re.FID = re_e1.FID
    left join crrc_sys.t_sec_user tsu on re.FCREATORID = tsu.FID
    left join crrc_secd.tk_crrc_project1 pro on re.FK_CRRC_BASEDATAFIELD = pro.FID
where
    not re_e1.FK_CRRC_COMBOFIELD = ''
    and not re_e1.FK_CRRC_COMBOFIELD is null