CREATE OR REPLACE VIEW ABBSG.V_AVG_8HR_SRC_FL_DAILY01 ("SHFT_START","SHFT_END", "SAMPLES", "S001", "S002", "S003", "S004", "S005", "S006", "S007", "S008", "S009", "S010", 
"S011", "S012", "S013", "S014", "S015", "S016", "S017", "S018", "S019", "S020", "S021", "S022", "S023", "S024", "S025", "S026", "S027", "S028", "S029", "S030", "S031", "S032", 
"S033", "S034", "S035", "S036", "S037", "S038", "S039", "S040", "S041", "S042", "S043", "S044", "S045", "S046", "S047", "S048")
AS
WITH date_range AS
   --- AUTHOR: ADHIR DUTTA
   --- DATE: 2023-FEB-02
  (SELECT TO_DATE(CONCAT(TO_CHAR(TRUNC(MIN (IDATE)),'YYYY-MM-DD'), ' 06:00'), 'YYYY-MM-DD HH24:MI') AS first_date,
    TO_DATE(CONCAT(TO_CHAR(TRUNC(MAX (IDATE)),'YYYY-MM-DD'), ' 05:59'), 'YYYY-MM-DD HH24:MI') + 1   AS last_date
  FROM ABBSG.SRC_FL_DAILY01
  ) ,
  periods AS
  (SELECT first_date + ((LEVEL - 1) / 3) AS shift_start ,
    first_date       + ( LEVEL / 3)      AS shift_end
  FROM date_range
    CONNECT BY LEVEL <= (last_date - first_date) * 3
  )
SELECT 
    p.shift_start AS SHFT_START ,
    p.shift_end AS SHFT_END,
    --  Total samples
    COUNT(RINDEX) AS SAMPLES,
    -- COOKING ZONE
    -- **********************************
    ROUND(AVG(C001),3) AS S001, -- 412LAST_ACTPROD
    ROUND(AVG(C002),3) AS S002, -- 412xu071
    ROUND(AVG(C003),3) AS S003, -- 412HX047
    ROUND(AVG(C004),3) AS S004, -- 412HX084
    ROUND(AVG(C005),3) AS S005, -- 412SUMAW
    ROUND(AVG(C006),3) AS S006, -- 412WLCONC
    -- Average(412FC043) x Average(412WLCONC) x 60 x 60 x 8 x 1/1000 x 1/ Average(412LAST ACTPROD)
    ROUND(AVG(C007)*AVG(C006)*3.6*8/(AVG(C001)),3) AS S007, -- 412FC043
    -- Average(412FC085) x Average(412WLCONC) x 60 x 60 x 8 x 1/1000 x 1/ Average(412LAST ACTPROD)
    ROUND(AVG(C008)*AVG(C006)*3.6*8/(AVG(C001)),3) AS S008, -- 412FC085
    -- WL charge, Average(412FC002) x Average(412WLCONC) x 60 x 60 x 8 x 1/1000 x 1/ Average(412LAST ACTPROD)
    ROUND(AVG(C009)*AVG(C006)*3.6*8/(AVG(C001)),3) AS S009, -- 412FC002
    -- (412FC043 + 412FC085 + 412FC002) Sum WL
    ROUND(AVG(C007)+AVG(C008)+AVG(C009),3) AS S010,
    ROUND(AVG(C010),3)                     AS S011, -- 412WLJUMBOCONC  WL jumbo conc.
    -- Average(412FC009) x  Average(412WLJUMBOCONC) x 60 x 60 x 8 x 1/1000 x1/Average(412LAST ACTPROD)
    ROUND(AVG(C011)*AVG(C010)*3.6*8/(AVG(C001)),3) AS S012, -- 412FC009 white liq from jumbo,
    ROUND(AVG(C012),3)                           AS S013, -- 412FC038  MP steam to digester
    -- Average(R293FI245) x 60 x 60 x 8 x 1/1000
    ROUND(AVG(C013)*3.6,3) AS S014, -- R293FI245  LP steam to F/L
    -- Brown stock ZONE
    -- **********************************
    -- Sum (O2_PULP_ADT )/3
    ROUND(AVG(C014),3) AS S015, -- O2_PULP_ADT production brown stock
    ROUND(AVG(C015),3) AS S016, -- 422QT013 kappa post, O2 Average(422QT013)
    ROUND(AVG(C016),3) AS S017, -- 422QT012 kappa pre O2, Average(422QT012)
    -- (422QT012-422QT013)/422QT012, % O2 delig
    ROUND((AVG(C016)-AVG(C015))/AVG(C016),3) AS S018,
    ROUND(AVG(C017),3)                       AS S019, -- 422QT014.1 PB brigthness, Average(422QT014.1)
    ROUND(AVG(C018),3)                       AS S020, -- 422QT014 PB kappa, Average(422QT014)
    -- Average(422FC309) x Average(422FC309_x4) x 60 x 60 x 8 x 1/1000 x 1/Average(O2_PULP_ADT)
    ROUND(AVG(C019)*AVG(C020)*3.6*8/AVG(C014),3) AS S021, -- 422FC309 OWL to MC- pump,
    ROUND(AVG(C020),3)                          AS S022, -- 422FC309_x4 OWL content, 422FC309_x4, Do Average
    ROUND(AVG(C021),3)                          AS S023, -- 422FC309_Y1 OWL actual, Average(422FC309_Y1)
    ROUND(AVG(C022),3)                          AS S024, -- 432FF106B NaoH density, 432FF106B Do Average
    --  Average(422FC323) x Average(432FF106B) x 60 x 60 x 8 x 1/1000 x 1/Average(O2_PULP_ADT)
    ROUND(AVG(C023)*AVG(C022)*3.6*8/(AVG(C014)),3) AS S025, -- 422FC323 NaoH to MCO2 shredding,
    ROUND(AVG(C024),3)                            AS S026, -- 422FC323_x4 content NaoH, 422FC323_x4, do Average
    ROUND(AVG(C025),3)                            AS S027, -- 422FC206_X2 O2 content, 422FC206_X2, DO Average
    ROUND(AVG(C026),3)                            AS S028, -- 422FC206_x1 O2 charge, Average(422FC206_x1)
    ROUND(AVG(C027),3)                            AS S029, -- 422FC305_x1 O2 charge, Average(422FC305_x1)
    ROUND(AVG(C028),3)                            AS S030, -- 422FC406_x1 O2 charge, Average(422FC406_x1)
    -- (422FC206_x1+422FC305_x1+422FC406_x1) Sum O2 charge, Average
    ROUND(AVG(C026)+AVG(C027)+AVG(C028),3) AS S031,
    -- BLEACHING ZONE
    -- ****************************************
    -- PB_PULP_ADT production bleaching Sum, (PB_PULP_ADT )/3
    ROUND(AVG(C029),3) AS S032,
    -- 422QT016.1, Brightness final, Average(422QT016.1)
    ROUND(AVG(C030),3) AS S033,
    -- 422QT014, kappa pre bleach, Average(422QT014)
    -- ROUND(AVG(C031),3) AS S034,
    ROUND(AVG(C018),3) AS S034,
    -- 432QC169B, CLO2 Bias 432QC169B
    ROUND(AVG(C031),3) AS S035,
    -- 432FT168, CLO2 to mc-mixer D1, Average(432FT168) x Average(432QC169B) x 60 x60 x 8 x 1/ Average(PB_PULP_ADT)
    ROUND(AVG(C032)*AVG(C032)*3.6*8/(AVG(C029)),3) AS S036,
    -- 432FC054_X2, CLO2 content, 432FC054_X2, Do Average
    ROUND(AVG(C033),3) AS S037,
    -- 432FT054, CLO2 to mc-mixer D0, Average(432FT054) x Average(432FC054_X2) x 60 x 60 x 8 x 1/1000 x 1/ Average(PB_PULP_ADT)
    ROUND(AVG(C034)*AVG(C033)*3.6*8/(AVG(C029)),3) AS S038,
    -- (432FT054+ 432FT168), Sum CLO2, Average(432FT054+ 432FT168)
    ROUND(AVG(C034)+AVG(C032),3) AS S039,
    -- 432FC106, NaoH to EOP stage, Average(432FC106) x Average(432FF106B) x 60 x 60 x 8 x 1/1000 x 1/ Average(PB_PULP_ADT)
    ROUND(AVG(C035)*AVG(C022)*3.6*8/(AVG(C029)),3) AS S040,
    -- 432FC079_X4, OWL content
    ROUND(AVG(C036),3) AS S041,
    -- 432FC079, OWL to D0 wash press, Average(432FC079) x Average(432FC079_X4) x 60 x 60 x 8 x 1/1000 x 1/ Average(PB_PULP_ADT)
    ROUND(AVG(C037)*AVG(C036)*3.6*8/(AVG(C029)),3) AS S042,
    -- 432FC105 oxygen to MPC mixer, Average(432FC105) x 60 x 60 x 8 x1/412LAST ACTPROD,
    ROUND(AVG(C038)*3600/(AVG(C001)),3) AS S043,
    -- 432FC105_Y1, O2 actual, Average(432FC105_Y1)
    ROUND(AVG(C039),3) AS S044,
    -- 432FC105_x2, O2 content, 432FC105_x2
    ROUND(AVG(C040),3) AS S045,
    -- 432FC073_Y1, H2O2 actual, Average(432FC073_Y1)
    ROUND(AVG(C041),3) AS S046,
    -- 432FC073_x2, H2O2 content, 432FC073_x2
    ROUND(AVG(C042),3) AS S047,
    -- 432FC073, H2O2 to T007, Average(432FC073) x Average(432FC073_x2) x 60x 60 x 8 x 1/1000 x 1/ Average(PB_PULP_ADT)
    ROUND(AVG(C043)*AVG(C042)*3.6*8/(AVG(C029)),3) AS S048

FROM periods p
LEFT OUTER JOIN ABBSG.SRC_FL_DAILY01 l
    ON l.IDATE >= p.shift_start
    AND l.IDATE < p.shift_end
GROUP BY p.shift_start , p.shift_end
ORDER BY p.shift_start ;

 
