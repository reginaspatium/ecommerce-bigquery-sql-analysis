-- CTE 1: Extracting email engagement metrics (sent, opened, visited) by date and country

WITH email AS(
SELECT
    DATE_ADD(ses.date, INTERVAL ems.sent_date DAY) AS date,
    0 AS account_cnt,
    COUNT(DISTINCT ems.id_message) AS sent_msg,
    COUNT(DISTINCT emo.id_message) AS open_msg,
    COUNT(DISTINCT emv.id_message) AS visit_msg,
    sespar.country AS country,
  acc.send_interval AS send_interval,
  acc.is_verified AS is_verified,
  acc.is_unsubscribed AS is_unsubscribed
FROM `DA.account` AS acc
LEFT JOIN `DA.email_sent` AS ems
ON acc.id = ems.id_account
LEFT JOIN `DA.email_open` AS emo
ON ems.id_message = emo.id_message
LEFT JOIN `DA.email_visit` AS emv
ON emo.id_message = emv.id_message
LEFT JOIN `DA.account_session` AS acs
ON acc.id = acs.account_id
LEFT JOIN `DA.session` AS ses
ON acs.ga_session_id = ses.ga_session_id
LEFT JOIN `DA.session_params` AS sespar
ON ses.ga_session_id = sespar.ga_session_id
GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),

-- CTE 2: Extracting account creation and session data per country and subscription status
account AS (
SELECT
    sess.date AS date,
 COUNT(DISTINCT acc.id) AS account_cnt,
 0 AS sent_msg,
 0 AS open_msg,
 0 AS visit_msg,
    sespar.country AS country,
    acc.send_interval AS send_interval,
    acc.is_verified AS is_verified,
    acc.is_unsubscribed AS is_unsubscribed
FROM `DA.account` AS acc
JOIN `DA.account_session` AS acs
ON acc.id = acs.account_id
JOIN `DA.session` AS sess
ON acs.ga_session_id = sess.ga_session_id
JOIN `DA.session_params` AS sespar
ON sess.ga_session_id = sespar.ga_session_id
GROUP BY sess.date, sespar.country, acc.send_interval, acc.is_verified, acc.is_unsubscribed
),

-- Combining account and email data into a single dataset
union_ AS (
 SELECT *
 FROM account

 UNION ALL

 SELECT *
 FROM email
),

-- Aggregating core metrics by date, location, and user status
additional_metrics AS(
SELECT
   date,
   country,
   send_interval,
   is_verified,
   is_unsubscribed,
   SUM(account_cnt) AS account_cnt,
   SUM(sent_msg) AS sent_msg,
   SUM(open_msg) AS open_msg,
   SUM(visit_msg) AS visit_msg
FROM union_
GROUP BY
   date, country, send_interval, is_verified, is_unsubscribed
),

-- Calculating total counts and performance rankings per country
country_total AS (
SELECT
   country,
   SUM(account_cnt) AS total_country_account_cnt,
   SUM(sent_msg) AS total_country_sent_cnt,
   DENSE_RANK() OVER (ORDER BY SUM(account_cnt) DESC) AS rank_total_country_account_cnt,
   DENSE_RANK() OVER (ORDER BY SUM(sent_msg) DESC) AS rank_total_country_sent_cnt
FROM additional_metrics
GROUP BY country
)

-- Final Output: Merging metrics with rankings and filtering for Top 10 countries
SELECT
date,
a.country AS country,
a.send_interval AS send_interval,
a.is_verified AS is_verified,
a.is_unsubscribed AS is_unsubscribed,
a.account_cnt AS account_cnt,
a.sent_msg AS sent_msg,
a.open_msg AS open_msg,
a.visit_msg AS visit_msg,
c.total_country_account_cnt AS total_country_account_cnt,
c.total_country_sent_cnt AS total_country_sent_cnt,
c.rank_total_country_account_cnt AS rank_total_country_account_cnt,
c.rank_total_country_sent_cnt AS rank_total_country_sent_cnt
FROM additional_metrics AS a
JOIN country_total AS c
ON a.country = c.country
WHERE c.rank_total_country_account_cnt <= 10 OR c.rank_total_country_sent_cnt <= 10;
