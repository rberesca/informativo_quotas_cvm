drop table tests.tbl_TestMerge
CREATE TABLE tests.tbl_TestMerge
(
    EmpNumber INTEGER NOT NULL ,
    EmpName VARCHAR(50) NOT NULL,
    EmpColumn2 VARCHAR(50),
    PRIMARY KEY (EmpNumber, EmpName)
);
CREATE TABLE tests.tbl_TestMerge2
(
    EmpNumber INTEGER NOT NULL ,
    EmpName VARCHAR(50) NOT NULL,
    EmpColumn2 VARCHAR(50),
    PRIMARY KEY (EmpNumber, EmpName)
);
INSERT INTO tests.tbl_TestMerge
VALUES (1,'ABC', 'aaa'), (2,'XYZ', 'bbb'), (3, 'PQR', 'ccc'), (4,'EFG', 'ddd'), (5, 'JKL', 'eee')
; TRUNCATE TABLE tests.tbl_TestMerge2
INSERT INTO tests.tbl_TestMerge2
VALUES (2, 'XYZ', 'xxx'), (6, 'PR', 'cyycc'), (10,'UFG', 'kddd')
;
select * from tests.tbl_TestMerge
;
select * from tests.tbl_TestMerge2
;
INSERT INTO tests.tbl_TestMerge
(EmpNumber, EmpName, EmpColumn2)
select * from tests.tbl_TestMerge2
ON DUPLICATE KEY UPDATE
-- EmpNumber=VALUES(EmpNumber),EmpName=VALUES(EmpName),
EmpColumn2=values(EmpColumn2)
