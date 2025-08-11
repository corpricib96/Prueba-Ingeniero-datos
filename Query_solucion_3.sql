WITH HIST AS 
(
    /* BASE_INICIAL_HISTORIA */
    SELECT 
    identificacion,
    to_timestamp(corte_mes,'dd/MM/yyyy') AS corte_mes,
    SUM(CAST(saldo AS BIGINT)) AS saldo,
    CASE 
    WHEN SUM(CAST(saldo AS BIGINT)) >= 0 AND SUM(CAST(saldo AS BIGINT)) < 300000 THEN 'N0'
    WHEN SUM(CAST(saldo AS BIGINT)) >= 300000 AND SUM(CAST(saldo AS BIGINT)) < 1000000 THEN 'N1'
    WHEN SUM(CAST(saldo AS BIGINT)) >= 1000000 AND SUM(CAST(saldo AS BIGINT)) < 3000000 THEN 'N2'
    WHEN SUM(CAST(saldo AS BIGINT)) >= 3000000 AND SUM(CAST(saldo AS BIGINT)) < 5000000 THEN 'N3'
    WHEN SUM(CAST(saldo AS BIGINT)) >= 5000000 THEN 'N4'
    ELSE 'OJO'
    END AS nivel_deuda
    FROM temporal.bd_historia_prueba
    GROUP BY 1,2
),
RET AS 
(
    /* BASE_INICIAL_RETIROS */
    SELECT 
    identificacion,
    TO_TIMESTAMP(fecha_retiro,'yyyyMMdd') AS fecha_retiro
    FROM temporal.bd_retiros_prueba
),
BD1 AS 
(
    /* SE ESTABLECE LA FECHA DE REFERENCIA */
    SELECT 
    TO_TIMESTAMP('20240622','yyyyMMdd') AS fecha_base,
    HIST.identificacion,
    HIST.corte_mes,
    LAG(HIST.corte_mes,1) OVER(PARTITION BY HIST.identificacion ORDER BY HIST.corte_mes ASC) AS corte_mes_ant,
    HIST.saldo,
    HIST.nivel_deuda,
    LAG(HIST.nivel_deuda,1) OVER(PARTITION BY HIST.identificacion ORDER BY HIST.corte_mes ASC) AS nivel_deuda_ant,
    RET.fecha_retiro
    FROM HIST 
    LEFT JOIN RET ON HIST.identificacion = RET.identificacion
),
BD2 AS 
(
    /* SE VALIDA CUANDO EXISTE UN SALTO DE CORTE_MES */
    SELECT 
    *,
    INT_MONTHS_BETWEEN(corte_mes,corte_mes_ant) AS dist_meses,
    CASE 
    WHEN corte_mes < fecha_retiro AND INT_MONTHS_BETWEEN(corte_mes,corte_mes_ant) > 1 THEN INT_MONTHS_BETWEEN(corte_mes,corte_mes_ant)-1
    ELSE 0
    END AS suma_saldo_n0,
    IF(nivel_deuda = nivel_deuda_ant,1,0) AS meses_consec
    FROM BD1
),
BD3 AS 
(
    SELECT 
    *,
    IF(meses_consec = 1 ,LAG(meses_consec,1) OVER(PARTITION BY identificacion ORDER BY corte_mes),0) AS meses_consec_ant
    FROM BD2
),
BD4 AS 
(
    /* SE VALIDA QUE LAS RACHAS CUMPLAN CON UN NUMERO N , EN ESTE CASO = 1 */
    SELECT 
    fecha_base,
    identificacion,
    corte_mes,
    saldo,
    nivel_deuda,
    fecha_retiro,
    suma_saldo_n0+meses_consec+meses_consec_ant AS racha,
    CASE 
    WHEN suma_saldo_n0+meses_consec+meses_consec_ant >= 1 THEN 'SI'
    ELSE 'NO'
    END AS cumple_n_rachas
    FROM BD3
),
BD5 AS 
(
    /* SE USA ROW_NUMBER PARA JERARQUIZAR Y DEJAR REGISTROS UNICOS */
    SELECT 
    fecha_base,
    identificacion,
    corte_mes,
    saldo,
    nivel_deuda,
    fecha_retiro,
    racha,
    cumple_n_rachas,
    ROW_NUMBER() OVER(PARTITION BY identificacion ORDER BY cumple_n_rachas DESC ,racha DESC, DATEDIFF(fecha_base,corte_mes) ASC) AS rankin
    FROM BD4
)
/* SE FILTRA POR LOS CRITERIOS ESTABLECIDOS */
SELECT 
fecha_base,
identificacion,
corte_mes,
saldo,
nivel_deuda,
fecha_retiro,
racha
FROM BD5
WHERE rankin = 1