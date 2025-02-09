#Create Database
CREATE DATABASE job_analysis;
USE job_analysis;

#Create Tables for job
CREATE TABLE job_data (
    job_id VARCHAR(50),
    actor_id VARCHAR(50),
    event VARCHAR(50),
    language VARCHAR(50),
    time_spent INT,
    org VARCHAR(50),
    ds DATE,
    PRIMARY KEY (job_id, actor_id, ds)
);

#Create Tables for users
CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    created_at TIMESTAMP,
    company_id VARCHAR(50),
    language VARCHAR(50),
    activated_at TIMESTAMP,
    state VARCHAR(50)
);

CREATE TABLE events (
    event_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    event_type VARCHAR(50),
    event_date TIMESTAMP,
    device_type VARCHAR(50),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE email_events (
    email_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    sent_date TIMESTAMP,
    opened BOOLEAN,
    clicked BOOLEAN,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);


INSERT INTO job_data VALUES
('j1', 'a1', 'decision', 'English', 100, 'org1', '2020-11-01'),
('j2', 'a2', 'skip', 'Spanish', 150, 'org1', '2020-11-01'),
('j3', 'a3', 'transfer', 'French', 200, 'org2', '2020-11-02'),
('j4', 'a1', 'decision', 'English', 120, 'org1', '2020-11-02'),
('j5', 'a2', 'decision', 'Spanish', 180, 'org2', '2020-11-03'),
('j6', 'a3', 'skip', 'French', 160, 'org2', '2020-11-03'),
('j7', 'a4', 'transfer', 'German', 140, 'org3', '2020-11-04'),
('j8', 'a1', 'decision', 'English', 130, 'org1', '2020-11-04'),
('j9', 'a2', 'skip', 'Spanish', 110, 'org1', '2020-11-05'),
('j10', 'a3', 'decision', 'French', 190, 'org2', '2020-11-05');


INSERT INTO users VALUES
('u1', '2020-01-01 10:00:00', 'c1', 'English', '2020-01-02 10:00:00', 'active'),
('u2', '2020-01-02 11:00:00', 'c1', 'Spanish', '2020-01-03 11:00:00', 'active'),
('u3', '2020-01-03 12:00:00', 'c2', 'French', '2020-01-04 12:00:00', 'active'),
('u4', '2020-01-04 13:00:00', 'c2', 'German', '2020-01-05 13:00:00', 'inactive'),
('u5', '2020-01-05 14:00:00', 'c3', 'English', '2020-01-06 14:00:00', 'active');

INSERT INTO events VALUES
('e1', 'u1', 'login', '2020-01-02 10:30:00', 'mobile'),
('e2', 'u1', 'search', '2020-01-02 10:45:00', 'mobile'),
('e3', 'u2', 'login', '2020-01-03 11:30:00', 'desktop'),
('e4', 'u2', 'message', '2020-01-03 11:45:00', 'desktop'),
('e5', 'u3', 'login', '2020-01-04 12:30:00', 'tablet'),
('e6', 'u3', 'search', '2020-01-04 12:45:00', 'tablet'),
('e7', 'u4', 'login', '2020-01-05 13:30:00', 'mobile'),
('e8', 'u4', 'message', '2020-01-05 13:45:00', 'mobile'),
('e9', 'u5', 'login', '2020-01-06 14:30:00', 'desktop'),
('e10', 'u5', 'search', '2020-01-06 14:45:00', 'desktop');

INSERT INTO email_events VALUES
('m1', 'u1', '2020-01-02 10:15:00', true, true),
('m2', 'u2', '2020-01-03 11:15:00', true, false),
('m3', 'u3', '2020-01-04 12:15:00', false, false),
('m4', 'u4', '2020-01-05 13:15:00', true, true),
('m5', 'u5', '2020-01-06 14:15:00', true, false);

#Case Study 1 Queries
#Write an SQL query to calculate the number of jobs reviewed per hour for each day in November 2020.

SELECT 
    ds,
    COUNT(job_id)/(SUM(time_spent)/3600.0) as jobs_per_hour
FROM 
    job_data
WHERE 
    ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY 
    ds
ORDER BY 
    ds;

#Write an SQL query to calculate the 7-day rolling average of throughput. Additionally, explain whether you prefer using the daily metric or the 7-day rolling average for throughput, and why.
WITH daily_throughput AS (
    SELECT 
        ds,
        COUNT(*) as events,
        SUM(time_spent) as total_time,
        COUNT(*)/SUM(time_spent) as throughput
    FROM 
        job_data
    GROUP BY 
        ds
)
SELECT 
    ds,
    AVG(throughput) OVER (
        ORDER BY ds
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_avg_throughput
FROM 
    daily_throughput
ORDER BY 
    ds;

#Write an SQL query to calculate the percentage share of each language over the last 30 days.
WITH language_counts AS (
    SELECT 
        language,
        COUNT(*) as language_count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage_share
    FROM 
        job_data
    WHERE 
        ds >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
    GROUP BY 
        language
)
SELECT 
    language,
    language_count,
    ROUND(percentage_share, 2) as percentage_share
FROM 
    language_counts
ORDER BY 
    percentage_share DESC;

-- 4. Duplicate Rows Detection
SELECT 
    job_id,
    actor_id,
    event,
    language,
    time_spent,
    org,
    ds,
    COUNT(*) as duplicate_count
FROM 
    job_data
GROUP BY 
    job_id, actor_id, event, language, time_spent, org, ds
HAVING 
    COUNT(*) > 1;

-- Case Study 2 Queries

-- 1. Weekly User Engagement
SELECT 
    DATE_FORMAT(event_date, '%Y-%U') as week,
    COUNT(DISTINCT user_id) as weekly_active_users,
    COUNT(*) as total_events
FROM 
    events
GROUP BY 
    DATE_FORMAT(event_date, '%Y-%U')
ORDER BY 
    week;

-- 2. User Growth Analysis
WITH new_users AS (
    SELECT 
        DATE_FORMAT(created_at, '%Y-%m') as month,
        COUNT(*) as new_users
    FROM 
        users
    GROUP BY 
        DATE_FORMAT(created_at, '%Y-%m')
),
cumulative_users AS (
    SELECT 
        month,
        new_users,
        SUM(new_users) OVER (ORDER BY month) as total_users
    FROM 
        new_users
)
SELECT 
    month,
    new_users,
    total_users,
    ROUND(((total_users * 1.0 / 
        LAG(total_users, 1) OVER (ORDER BY month)) - 1) * 100, 2) as growth_rate
FROM 
    cumulative_users
ORDER BY 
    month;

-- 3. Weekly Retention Analysis
WITH cohort_users AS (
    SELECT 
        u.user_id,
        DATE_FORMAT(u.created_at, '%Y-%U') as cohort_week,
        DATE_FORMAT(e.event_date, '%Y-%U') as activity_week
    FROM 
        users u
        JOIN events e ON u.user_id = e.user_id
),
user_retention AS (
    SELECT 
        cohort_week,
        activity_week,
        COUNT(DISTINCT user_id) as users,
        TIMESTAMPDIFF(WEEK, 
            STR_TO_DATE(CONCAT(cohort_week, ' Sunday'), '%X-%V %W'),
            STR_TO_DATE(CONCAT(activity_week, ' Sunday'), '%X-%V %W')
        ) as week_number
    FROM 
        cohort_users
    GROUP BY 
        cohort_week,
        activity_week
)
SELECT 
    cohort_week,
    week_number,
    users,
    ROUND(users * 100.0 / FIRST_VALUE(users) 
        OVER (PARTITION BY cohort_week ORDER BY week_number), 2) as retention_rate
FROM 
    user_retention
WHERE 
    week_number >= 0
ORDER BY 
    cohort_week, 
    week_number;

-- 4. Weekly Engagement Per Device
SELECT 
    DATE_FORMAT(event_date, '%Y-%U') as week,
    device_type,
    COUNT(DISTINCT user_id) as active_users,
    COUNT(*) as total_events,
    COUNT(*) * 1.0 / COUNT(DISTINCT user_id) as events_per_user
FROM 
    events
GROUP BY 
    DATE_FORMAT(event_date, '%Y-%U'),
    device_type
ORDER BY 
    week,
    device_type;

-- 5. Email Engagement Analysis
SELECT 
    DATE_FORMAT(sent_date, '%Y-%m') as month,
    COUNT(*) as total_emails,
    SUM(opened) as opened_emails,
    SUM(clicked) as clicked_emails,
    ROUND(SUM(opened) * 100.0 / COUNT(*), 2) as open_rate,
    ROUND(SUM(clicked) * 100.0 / SUM(opened), 2) as click_through_rate
FROM 
    email_events
GROUP BY 
    DATE_FORMAT(sent_date, '%Y-%m')
ORDER BY 
    month;