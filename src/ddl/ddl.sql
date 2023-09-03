-- DDS --

-- dds.calendar
DROP TABLE IF EXISTS dds.calendar CASCADE;
CREATE TABLE dds.calendar (
id SERIAL PRIMARY KEY,
timestamp_time TIMESTAMP UNIQUE, -- к 'timestamp' добавленно time
date_time DATE, -- аналогично для 'date'
years SMALLINT CHECK ("years" < 2090 AND "years" > 2000), 
months SMALLINT CHECK ("months" > 0 AND "months" < 13),
days SMALLINT CHECK ("days" > 0 AND "days" < 32) -- к заголовкам добавленно окончание -s
);

-- dds.couriers
DROP TABLE IF EXISTS dds.couriers CASCADE;
CREATE TABLE dds.couriers (
id VARCHAR PRIMARY KEY,
courier_name VARCHAR
);

-- dds.restaurants
DROP TABLE IF EXISTS dds.restaurants CASCADE;
CREATE TABLE dds.restaurants (
id VARCHAR PRIMARY KEY,
restaurant_name VARCHAR
);

-- dds.deliveries
DROP TABLE IF EXISTS dds.deliveries CASCADE;
CREATE TABLE dds.deliveries (
order_id VARCHAR PRIMARY KEY,
calendar_id INT REFERENCES dds.calendar(id),
courier_id VARCHAR REFERENCES dds.couriers(id),
rate SMALLINT CHECK ("rate" BETWEEN 1 AND 5),
total INT CHECK ("total" > 0), -- исправлено с "sum" на total
tip_sum INT
);

-- STG --

-- stg.couriers
DROP TABLE IF EXISTS stg.couriers CASCADE;
CREATE TABLE stg.couriers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

-- stg.deliveries
DROP TABLE IF EXISTS stg.deliveries CASCADE;
CREATE TABLE stg.deliveries (
    order_id TEXT PRIMARY KEY,
    order_ts TIMESTAMP NOT NULL,
    delivery_id TEXT NOT NULL,
    courier_id TEXT NOT NULL,
    address TEXT NOT NULL,
    delivery_ts TIMESTAMP NOT NULL,
    rate SMALLINT NOT NULL,
    total INT NOT NULL, -- -- исправлено с "sum" на total
    tip_sum INT NOT NULL
);

--stg.restaurants
DROP TABLE IF EXISTS stg.restaurants CASCADE;
CREATE TABLE stg.restaurants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

-- CDM --

-- cdm.dm_courier_ledger
DROP MATERIALIZED VIEW IF EXISTS cdm.dm_courier_ledger CASCADE;
CREATE MATERIALIZED VIEW cdm.dm_courier_ledger AS (
WITH fst AS (
SELECT
    d.courier_id,
    c.courier_name,
    cal.years AS settlement_year,
    cal.months AS settlement_month,
    COUNT(d.order_id) AS orders_count,
    SUM(d.total) AS orders_total_sum,
    AVG(d.rate) AS rate_avg,
    SUM(d.total) * 0.25 AS order_processing_fee,
    CASE
        WHEN AVG(d.rate) < 4 THEN GREATEST(SUM(d.total) * 0.05, 100)
        WHEN (AVG(d.rate) >= 4 AND AVG(d.rate) < 4.5) 
        THEN GREATEST(SUM(d.total) * 0.07, 150)
        WHEN (AVG(d.rate) >= 4.5 AND AVG(d.rate) < 4.9)
        THEN GREATEST(SUM(d.total) * 0.08, 175)
        WHEN AVG(d.rate) >= 4.9 THEN GREATEST(SUM(d.total) * 0.1, 200)
    END AS courier_order_sum,
    SUM(d.tip_sum) AS courier_tips_sum
FROM dds.deliveries d
JOIN dds.couriers c ON d.courier_id = c.id
JOIN dds.calendar cal ON d.calendar_id = cal.id
GROUP BY d.courier_id, c.courier_name, cal.years, cal.months)

SELECT courier_id,
		courier_name,
		settlement_year,
		settlement_month,
		orders_count,
		orders_total_sum,
		rate_avg,
		order_processing_fee,
		courier_order_sum,
		courier_tips_sum,
		((courier_order_sum + courier_tips_sum) * 0.95) AS courier_reward_sum
FROM fst
);

