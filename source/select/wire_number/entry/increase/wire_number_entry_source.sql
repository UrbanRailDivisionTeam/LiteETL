select
    re2.FEntryId as "id",
    re.FMODIFYTIME as "修改时间"
from
    crrc_secd.tk_crrc_wire_number_re_e2 re2
    left join crrc_secd.tk_crrc_wire_number_re re on re.FID = re2.FID