SELECT
    bill.FEntryId AS "id",
    bill.FID AS "单据头id",
    bill_m.fmodifytime AS "单据头修改时间",
    _user.fnumber AS "工号",
    _user.FTRUENAME AS "姓名",
    bill.fk_crrc_comedate AS "进入班组日期",
    bill.fk_crrc_leavedate AS "离开班组日期",
    case bill.fk_crrc_useperson
        WHEN 0 THEN '是'
        WHEN 1 THEN '否'
    end AS "配送领料接收人",
    case bill.fk_crrc_fzuser
        WHEN 0 THEN '是'
        WHEN 1 THEN '否'
    end AS "负责人"
FROM
    crrc_secd.tk_crrc_classgroupentry bill
    LEFT JOIN crrc_sys.t_sec_user _user ON bill.fk_crrc_userfield = _user.FID
    LEFT JOIN crrc_secd.tk_crrc_classgroup bill_m ON bill.FID = bill_m.FID