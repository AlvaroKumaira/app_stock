saldo_analitico = """
        SELECT DISTINCT
    P.B1_COD,
    P.B1_TIPO,
    P.B1_GRUPO,
    P.B1_DESC,
    D.BZ_LOCALI2,
    P.B1_UM,
    S.B2_FILIAL,
    S.B2_LOCAL,
    S.B2_QATU AS B2_QATU_COPY,
    '-' AS TEMP_1,
    S.B2_QATU,
    S.B2_CM1,
    S.B2_VATU1,
    '-' AS TEMP_2,
    P.B1_UCOM,
    S.B2_USAI,
    CASE
        WHEN DATEDIFF(DAY, CONVERT(DATE, SUBSTRING(CONVERT(VARCHAR, S.B2_DMOV), 1, 8), 112), GETDATE()) <= 40000
        THEN DATEDIFF(DAY, CONVERT(DATE, SUBSTRING(CONVERT(VARCHAR, S.B2_DMOV), 1, 8), 112), GETDATE())
        ELSE 0
    END AS DAYS_DIFF
    FROM
        SB1010 AS P
    LEFT JOIN
        SB2010 AS S ON RTRIM(P.B1_COD) = RTRIM(S.B2_COD) AND S.B2_FILIAL = ? AND S.B2_LOCAL = 'A01' AND S.D_E_L_E_T_ <> '*'
    LEFT JOIN   
        SBZ010 AS D ON RTRIM(P.B1_COD) = RTRIM(D.BZ_COD) AND D.BZ_FILIAL = ? AND D.D_E_L_E_T_ <> '*'
    WHERE
        P.D_E_L_E_T_ <> '*' AND
        P.B1_GRUPO NOT IN ('002', '001', '003') AND
        P.B1_TIPO IN ('ME', 'MI', 'KT', 'PA') AND
        S.B2_FILIAL IS NOT NULL AND
        S.B2_LOCAL IS NOT NULL
        """
pedidos = """
        SELECT DISTINCT
            SC7.C7_NUM,
            SC7.C7_FORNECE,
            SA.A2_LOJA AS A2_LOJA_COPY,
            SA.A2_NOME,
            SA.A2_TEL,
            SC7.C7_ITEM,
            SC7.C7_NUMSC,
            SC7.C7_PRODUTO,
            SC7.C7_DESCRI,
            SB.B1_GRUPO,
            CONVERT(DATE, STUFF(STUFF(CAST(SC7.C7_EMISSAO AS VARCHAR), 7, 0, '-'), 5, 0, '-')) AS EMI,
            SA.A2_LOJA,
            CONVERT(DATE, STUFF(STUFF(CAST(SC7.C7_DATPRF AS VARCHAR), 7, 0, '-'), 5, 0, '-')) AS ENT,
            SC7.C7_QUANT,
            SC7.C7_UM,
            SC7.C7_PRECO,
            ISNULL(SC7.C7_DESC1, 0) + ISNULL(SC7.C7_DESC2, 0) + ISNULL(SC7.C7_DESC3, 0) AS DE,
            SC7.C7_VALIPI,
            SC7.C7_TOTAL,
            SC7.C7_QUJE,
            ISNULL(SC7.C7_QUANT, 0) - ISNULL(SC7.C7_QUJE, 0) AS QRE,
            (ISNULL(SC7.C7_QUANT, 0) - ISNULL(SC7.C7_QUJE, 0)) * ISNULL(SC7.C7_PRECO, 0) AS SRE,
            SC7.C7_RESIDUO
            FROM SC7010 AS SC7
            INNER JOIN
                SA2010 AS SA ON SC7.C7_FORNECE = SA.A2_COD AND SC7.C7_LOJA = SA.A2_LOJA
            inner JOIN
                SB1010 AS SB ON SC7.C7_PRODUTO = SB.B1_COD
            WHERE SC7.D_E_L_E_T_ <> '*' 
            AND SC7.C7_EMISSAO >= ?
            AND SC7.C7_FILIAL = ?
            AND SA.D_E_L_E_T_ <> '*'
            AND SB.D_E_L_E_T_ <> '*'
        """
faturamento = """
        SELECT
            SD2.D2_TP,
            SD2.D2_COD,
            SB.B1_DESC,
            SD2.D2_QUANT,
            SD2.D2_QUANT AS QUANT_DUP,
            SD2.D2_UM,
            SD2.D2_CUSTO1,
            SD2.D2_PRCVEN,
            SD2.D2_TOTAL AS VFB,
            SD2.D2_TOTAL AS VF,
            '-' AS INF,
            SD2.D2_TOTAL AS VFINP,
            SD2.D2_MARGEM,
            '-' AS TEMP1,
            '-' AS TEMP2,
            '-' AS TEMP3,
            '-' AS TEMP4,
            '-' AS TEMP5
            FROM SD2010 AS SD2
            INNER JOIN
            SB1010 AS SB ON SD2.D2_COD = SB.B1_COD AND SB.D_E_L_E_T_ <> '*' 
            WHERE SD2.D_E_L_E_T_  <> '*'
            AND SD2.D2_EMISSAO >= ?
            AND SD2.D2_FILIAL = ?
        """
info_gerais = """
        SELECT
            P.B1_ZGRUPO,
            P.B1_COD,
            P.B1_DESC,
            S.B2_QATU
            FROM
                SB1010 AS P
            LEFT JOIN
                SB2010 AS S ON TRIM(P.B1_COD) = TRIM(S.B2_COD) AND S.B2_FILIAL = ? AND S.B2_LOCAL = 'A01' AND S.D_E_L_E_T_ <> '*'
            LEFT JOIN   
                SBZ010 AS D ON TRIM(P.B1_COD) = TRIM(D.BZ_COD) AND D.BZ_FILIAL = ? AND D.D_E_L_E_T_ <> '*'
            WHERE
                P.D_E_L_E_T_ <> '*' AND
                P.B1_GRUPO = '320' AND
                P.B1_TIPO IN ('ME', 'MI', 'KT', 'PA') AND
                S.B2_FILIAL IS NOT NULL AND
                S.B2_LOCAL IS NOT NULL
        """
historico_faturamento = """
        SELECT
            SB.B1_ZGRUPO,
            SD2.D2_COD,
            SB.B1_DESC,
            SD2.D2_QUANT,
            SD2.D2_EMISSAO
            FROM SD2010 AS SD2
            INNER JOIN
            SB1010 AS SB ON SD2.D2_COD = SB.B1_COD AND SB.D_E_L_E_T_ <> '*' 
            WHERE SD2.D_E_L_E_T_  <> '*'
            AND SD2.D2_LOCAL = 'A01'
            AND SD2.D2_FILIAL = ?
            AND YEAR(CONVERT(DATETIME, STUFF(STUFF(CAST(SD2.D2_EMISSAO AS VARCHAR), 7, 0, '-'), 5, 0, '-'))) = YEAR(GETDATE())
            AND MONTH(CONVERT(DATETIME, STUFF(STUFF(CAST(SD2.D2_EMISSAO AS VARCHAR), 7, 0, '-'), 5, 0, '-'))) >= (MONTH(GETDATE()) - 4)
            AND DAY(CONVERT(DATETIME, STUFF(STUFF(CAST(SD2.D2_EMISSAO AS VARCHAR), 7, 0, '-'), 5, 0, '-'))) >= 1
            ORDER BY SD2.D2_EMISSAO
        """
quantidade_receber = """
        SELECT
            SB.B1_ZGRUPO,
            ISNULL(SC7.C7_QUANT, 0) - ISNULL(SC7.C7_QUJE, 0) AS QRE
            FROM SC7010 AS SC7
            INNER JOIN SB1010 AS SB ON SC7.C7_PRODUTO = SB.B1_COD
            WHERE SC7.D_E_L_E_T_ <> '*'
            AND CONVERT(DATETIME, STUFF(STUFF(CAST(SC7.C7_EMISSAO AS VARCHAR), 7, 0, '-'), 5, 0, '-')) BETWEEN DATEADD(DAY, -59, GETDATE()) AND GETDATE()
            AND ISNULL(SC7.C7_QUANT, 0) - ISNULL(SC7.C7_QUJE, 0) > 0
            AND SC7.C7_FILIAL = ?
            AND SB.D_E_L_E_T_ <> '*'
        """
query_busca = """SELECT
P.B1_ZGRUPO,
P.B1_COD
FROM
    SB1010 AS P
WHERE
    P.D_E_L_E_T_ <> '*' AND
    P.B1_GRUPO NOT IN ('002', '001', '003') AND
    P.B1_TIPO IN ('ME', 'MI', 'KT', 'PA') AND
	P.B1_COD = ?
    """
query_resultado = """SELECT
P.B1_ZGRUPO,
P.B1_COD,
P.B1_DESC,
S.B2_QATU
FROM
    SB1010 AS P
INNER JOIN
    SB2010 AS S ON TRIM(P.B1_COD) = TRIM(S.B2_COD) AND S.B2_FILIAL = '0101' AND S.B2_LOCAL = 'A01' AND S.D_E_L_E_T_ <> '*'
WHERE
    P.D_E_L_E_T_ <> '*' AND
    P.B1_GRUPO NOT IN ('002', '001', '003') AND
    P.B1_TIPO IN ('ME', 'MI', 'KT', 'PA') AND
	P.B1_ZGRUPO = ? AND
    S.B2_FILIAL IS NOT NULL AND
    S.B2_LOCAL IS NOT NULL
    """