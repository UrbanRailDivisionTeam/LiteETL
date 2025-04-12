SELECT
    CONCAT (EmployeeID, TypeID) AS "id",
    StatisticsDate AS "调整时间"
FROM
    YQ_KQ_AS_MonthTotalDay
WHERE
    StatisticsDate >= DATEADD (year, -1, GETDATE ())