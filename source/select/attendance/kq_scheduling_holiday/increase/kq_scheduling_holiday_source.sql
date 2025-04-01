SELECT
    HolidayID AS "id",
    case ShiftAdjustDate
        WHEN cast('9999-12-31 00:00:00.000' AS date) THEN NULL
        else ShiftAdjustDate
    end AS "对应调整(换休)日期"
FROM
    YQ_KQ_Holiday