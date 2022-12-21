---- MYSQL View
---- Author: Adhir Dutta
---- Convert Multiple Rows' value into columns'
CREATE OR REPLACE VIEW v_pm12_pulp2(idate,col1,col2,col3,col4,col5,col6,col7,col8,col9)
AS
SELECT
  aa.ldate AS idate,
    aa.col1 AS col1,
     aa.col2 AS col2,
       aa.col3 AS col3,
         aa.col4 AS col4,
           aa.col5 AS col5,
             aa.col6 AS col6,
               aa.col7 AS col7,
                 aa.col8 AS col8,
                   aa.col9 AS col9
FROM(
SELECT 
tt.reg_date AS ldate,
MAX((case when (tt.id=1) then tt.col1 ELSE 0 END )) AS col1,
MAX((case when (tt.id=2) then tt.col1 ELSE 0 END )) AS col2,
MAX((case when (tt.id=3) then tt.col1 ELSE 0 END )) AS col3,
MAX((case when (tt.id=4) then tt.col1 ELSE 0 END )) AS col4,
MAX((case when (tt.id=5) then tt.col1 ELSE 0 END )) AS col5,
MAX((case when (tt.id=6) then tt.col1 ELSE 0 END )) AS col6,
MAX((case when (tt.id=7) then tt.col1 ELSE 0 END )) AS col7,
MAX((case when (tt.id=8) then tt.col1 ELSE 0 END )) AS col8,
MAX((case when (tt.id=9) then tt.col1 ELSE 0 END )) AS col9
FROM(
SELECT pm_pulp2.id AS id,pm_pulp2.reg_date, FORMAT(pm_pulp2.value,2) AS col1 FROM pm_pulp2 WHERE (pm_pulp2.id=1)
UNION ALL
SELECT pm_pulp2.id AS id,pm_pulp2.reg_date, FORMAT(pm_pulp2.value,2) AS col1 FROM pm_pulp2 WHERE (pm_pulp2.id=2)
UNION ALL
SELECT pm_pulp2.id AS id,pm_pulp2.reg_date, FORMAT(pm_pulp2.value,2) AS col1 FROM pm_pulp2 WHERE (pm_pulp2.id=3)
UNION ALL
SELECT pm_pulp2.id AS id,pm_pulp2.reg_date, FORMAT(pm_pulp2.value,2) AS col1 FROM pm_pulp2 WHERE (pm_pulp2.id=4)
UNION ALL
SELECT pm_pulp2.id AS id,pm_pulp2.reg_date, FORMAT(pm_pulp2.value,2) AS col1 FROM pm_pulp2 WHERE (pm_pulp2.id=5)
UNION ALL
SELECT pm_pulp2.id AS id,pm_pulp2.reg_date, FORMAT(pm_pulp2.value,2) AS col1 FROM pm_pulp2 WHERE (pm_pulp2.id=6)
UNION ALL
SELECT pm_pulp2.id AS id,pm_pulp2.reg_date, FORMAT(pm_pulp2.value,2) AS col1 FROM pm_pulp2 WHERE (pm_pulp2.id=7)
UNION ALL
SELECT pm_pulp2.id AS id,pm_pulp2.reg_date, FORMAT(pm_pulp2.value,2) AS col1 FROM pm_pulp2 WHERE (pm_pulp2.id=8)
UNION ALL
SELECT pm_pulp2.id AS id,pm_pulp2.reg_date, FORMAT(pm_pulp2.value,2) AS col1 FROM pm_pulp2 WHERE (pm_pulp2.id=9)
) tt
)aa;

---- MySQL Table(pm_pulp2) Data ( CSV)
---- ***************************************************************************
---- id,tag,tag_desc,value,quality,reg_date
---- 1,731TI-459,PM1 TEMPERATURE LP MAIN STEAM,148.77,good,2022-12-21 08:24:40
---- 2,731TID-459,TEMP LP MAIN STEAM DIFF,-0.55,good,2022-12-21 08:25:10
---- 3,731PI-399,PM1 LP STEAM PRESSURE,3.63,good,2022-12-21 08:24:40
---- 4,731PI-398,PM1 MP STEAM PRESSURE,12.68,good,2022-12-21 08:25:10
---- 5,731QI-3000,PM1 PRODUCTION t/h,26.15,good,2022-12-21 08:24:40
---- 6,732TI-459,PM2 TEMPERATURE LP MAIN STEAM,148.51,good,2022-12-21 08:24:40
---- 7,732TID-459,PM2 TEMP LP MAIN STEAM DIFF,0.34,good,2022-12-21 08:24:40
---- 8,732PI-399,PM2 LP STEAM PRESSURE,3.52,good,2022-12-21 08:25:10
---- 9,732QI-3000,PM2 PRODUCTION t/h,28.89,good,2022-12-21 08:24:40

---- Result MYSQL(v_pm12_pulp2)
---- ***************************************************************************
---- idate,col1,col2,col3,col4,col5,col6,col7,col8,col9
---- 2022-12-21 08:24:40,148.77,-0.55,3.63,12.68,26.15,148.51,0.34,3.52,28.89

