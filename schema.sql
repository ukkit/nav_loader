-- SQL schema with partitioning
CREATE TABLE nav_data (
    id INT AUTO_INCREMENT,
    scheme_type VARCHAR(100),
    scheme_category VARCHAR(100),
    scheme_sub_category VARCHAR(100),
    scheme_code VARCHAR(20),
    isin_growth VARCHAR(30),
    isin_reinv VARCHAR(30),
    scheme_name TEXT,
    nav DECIMAL(10, 4),
    nav_date DATE NOT NULL,
    fund_structure VARCHAR(100),
    PRIMARY KEY (id, nav_date),
    INDEX(nav_date),
    INDEX(scheme_code),
    UNIQUE KEY unique_scheme_date (scheme_code, nav_date)
)
PARTITION BY RANGE (YEAR(nav_date)) (
    PARTITION p1990 VALUES LESS THAN (1991),
    PARTITION p1991 VALUES LESS THAN (1992),
    PARTITION p1992 VALUES LESS THAN (1993),
    PARTITION p1993 VALUES LESS THAN (1994),
    PARTITION p1994 VALUES LESS THAN (1995),
    PARTITION p1995 VALUES LESS THAN (1996),
    PARTITION p1996 VALUES LESS THAN (1997),
    PARTITION p1997 VALUES LESS THAN (1998),
    PARTITION p1998 VALUES LESS THAN (1999),
    PARTITION p1999 VALUES LESS THAN (2000),
    PARTITION p2000 VALUES LESS THAN (2001),
    PARTITION p2001 VALUES LESS THAN (2002),
    PARTITION p2002 VALUES LESS THAN (2003),
    PARTITION p2003 VALUES LESS THAN (2004),
    PARTITION p2004 VALUES LESS THAN (2005),
    PARTITION p2005 VALUES LESS THAN (2006),
    PARTITION p2006 VALUES LESS THAN (2007),
    PARTITION p2007 VALUES LESS THAN (2008),
    PARTITION p2008 VALUES LESS THAN (2009),
    PARTITION p2009 VALUES LESS THAN (2010),
    PARTITION p2010 VALUES LESS THAN (2011),
    PARTITION p2011 VALUES LESS THAN (2012),
    PARTITION p2012 VALUES LESS THAN (2013),
    PARTITION p2013 VALUES LESS THAN (2014),
    PARTITION p2014 VALUES LESS THAN (2015),
    PARTITION p2015 VALUES LESS THAN (2016),
    PARTITION p2016 VALUES LESS THAN (2017),
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION p2019 VALUES LESS THAN (2020),
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);