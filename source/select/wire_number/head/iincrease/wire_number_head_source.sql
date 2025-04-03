select
    re.FEntryId as "id",
    re.FMODIFYTIME as "修改时间"
from
    crrc_secd.tk_crrc_wire_number_re_e1 re_e1
    left join crrc_secd.tk_crrc_wire_number_re re on re.FID = re_e1.FID
where
    not re_e1.FK_CRRC_COMBOFIELD = ''
    and not re_e1.FK_CRRC_COMBOFIELD is null