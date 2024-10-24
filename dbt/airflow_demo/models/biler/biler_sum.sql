SELECT
    vehicle_model,
    COUNT(*) AS SUM
FROM
{{ref('biler_view')}}
GROUP BY
    vehicle_model
ORDER BY
    SUM DESC
