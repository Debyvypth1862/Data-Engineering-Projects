--This is step for Delete data from table daily_report_sub5_output
DELETE FROM
    affise_data_fusion.daily_report_sub5_output
where
    datestamp BETWEEN CURRENT_DATE - INTERVAL '7' DAY
    AND CURRENT_DATE --This is step for Insert data to daily_report_sub5_output
INSERT INTO
    affise_data_fusion.daily_report_sub5_output WITH sub5_click AS (
        SELECT
            DISTINCT DATE(click.created_datetime) AS datestamp,
            click.affiliate_id,
            aff.login AS affiliate_name,
            CONCAT(
                aff.manager.first_name,
                ' ',
                aff.manager.last_name
            ) AS affiliate_manager,
            adv.title AS advertiser_name,
            CONCAT(
                adv.manager_obj.first_name,
                ' ',
                adv.manager_obj.last_name
            ) AS account_manager,
            offers.title AS offer_title,
            click.offer_id,
            offers.status AS offer_status,
            offers.privacy AS offer_privacy,
            click.country,
            click.sub5,
            click.sub6,
            click.os,
            click.os_version,
            COUNT(click_id) AS click_count,
            SUM(
                CASE
                    WHEN uniq = 'true' THEN 1
                    ELSE 0
                END
            ) AS host_count
        FROM
            "affise_data_fusion"."raw_clicks_sg" click
            LEFT JOIN affise_data_fusion.advertisers adv ON click.advertiser_id = adv.id
            LEFT JOIN affise_data_fusion.full_offers offers ON click.offer_id = offers.offer_id
            LEFT JOIN affise_data_fusion.affiliates aff ON click.affiliate_id = aff.id
        WHERE
            click_id is not null
            AND DATE(click.created_datetime) BETWEEN CURRENT_DATE - INTERVAL '7' DAY
            AND CURRENT_DATE
        GROUP BY
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15
    ),
    filtered_conversion as(
        SELECT
            DISTINCT date(created_datetime) AS datestamp,
            cov.advertiser_id,
            cov.affiliate_id,
            cov.offer_id,
            cov.country,
            cov.sub5,
            cov.sub6,
            cov.earnings,
            cov.STATUS,
            cov.payouts,
            cov.income,
            cov.conversion_id,
            cov.os,
            cov.os_version,
            ROW_NUMBER() OVER (
                PARTITION BY conversion_id
                ORDER BY
                    updated_datetime DESC
            ) as rn
        FROM
            "affise_data_fusion"."raw_conversions_sg" cov
    ),
    sub5_conversion AS (
        SELECT
            cov.datestamp,
            cov.affiliate_id,
            aff.login AS affiliate_name,
            CONCAT(
                aff.manager.first_name,
                ' ',
                aff.manager.last_name
            ) AS affiliate_manager,
            adv.title AS advertiser_name,
            CONCAT(
                adv.manager_obj.first_name,
                ' ',
                adv.manager_obj.last_name
            ) AS account_manager,
            offers.title AS offer_title,
            offers.status AS offer_status,
            offers.privacy AS offer_privacy,
            cov.offer_id,
            cov.country,
            cov.sub5,
            cov.sub6,
            cov.os,
            cov.os_version,
            sum(earnings) earnings,
            SUM(
                CASE
                    WHEN cov.STATUS = 'confirmed' THEN 1
                    else 0
                END
            ) approved,
            sum(payouts) payouts,
            SUM(
                CASE
                    WHEN cov.STATUS = 'confirmed' THEN cov.income
                END
            ) AS approved_revenue,
            SUM(
                CASE
                    WHEN cov.STATUS = 'declined' THEN cov.income
                END
            ) AS declined_revenue,
            COUNT(
                DISTINCT (
                    CASE
                        WHEN cov.conversion_id IS NOT NULL
                        AND cov.STATUS = 'confirmed' THEN cov.conversion_id
                    END
                )
            ) AS approved_conversions,
            COUNT(
                DISTINCT (
                    CASE
                        WHEN cov.conversion_id IS NOT NULL
                        AND cov.STATUS = 'declined' THEN cov.conversion_id
                    END
                )
            ) AS declined_conversions
        FROM
            filtered_conversion cov
            LEFT JOIN affise_data_fusion.advertisers adv ON cov.advertiser_id = adv.id
            LEFT JOIN affise_data_fusion.full_offers offers ON cov.offer_id = offers.offer_id
            LEFT JOIN affise_data_fusion.affiliates aff ON cov.affiliate_id = aff.id
        where
            cov.rn = 1
            AND cov.datestamp BETWEEN CURRENT_DATE - INTERVAL '7' DAY
            AND CURRENT_DATE
        GROUP BY
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15
    ),
    tmp AS (
        SELECT
            COALESCE(c.datestamp, cov.datestamp) AS datestamp,
            COALESCE(c.affiliate_id, cov.affiliate_id) AS affiliate_id,
            COALESCE(c.affiliate_name, cov.affiliate_name) AS affiliate_name,
            COALESCE(c.affiliate_manager, cov.affiliate_manager) AS affiliate_manager,
            COALESCE(c.advertiser_name, cov.advertiser_name) AS advertiser_name,
            COALESCE(c.account_manager, cov.account_manager) AS account_manager,
            COALESCE(c.offer_title, cov.offer_title) AS offer_title,
            COALESCE(c.offer_status, cov.offer_status) AS offer_status,
            COALESCE(c.offer_privacy, cov.offer_privacy) AS offer_privacy,
            COALESCE(c.offer_id, cov.offer_id) AS offer_id,
            COALESCE(c.country, cov.country) AS country,
            COALESCE(c.sub5, cov.sub5) AS sub5,
            COALESCE(c.sub6, cov.sub6) AS sub6,
            COALESCE(c.os, cov.os) AS os,
            COALESCE(c.os_version, cov.os_version) AS os_version,
            SUM(COALESCE(c.click_count, 0)) AS click_count,
            SUM(COALESCE(c.host_count, 0)) AS host_count,
            sum(COALESCE(cov.earnings, 0)) as earnings,
            sum(COALESCE(cov.approved, 0)) as approved,
            sum(COALESCE(cov.payouts, 0)) as payouts,
            SUM(COALESCE(cov.approved_revenue, 0)) AS approved_revenue,
            SUM(COALESCE(cov.declined_revenue, 0)) AS declined_revenue,
            SUM(COALESCE(cov.approved_conversions, 0)) AS conversions_count,
            SUM(COALESCE(cov.declined_conversions, 0)) AS declined_conversions
        FROM
            sub5_click c FULL
            JOIN sub5_conversion cov ON cov.datestamp = c.datestamp
            AND cov.affiliate_id = c.affiliate_id
            AND cov.advertiser_name = c.advertiser_name
            AND cov.account_manager = c.account_manager
            AND cov.offer_title = c.offer_title
            AND cov.offer_id = c.offer_id
            AND cov.country = c.country
            AND cov.sub6 = c.sub6
            AND cov.sub5 = c.sub5
            AND cov.os = c.os
            AND cov.os_version = c.os_version
        GROUP BY
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15
    )
SELECT
    datestamp,
    affiliate_id,
    affiliate_name,
    affiliate_manager,
    advertiser_name,
    account_manager,
    offer_title,
    offer_status,
    offer_privacy,
    offer_id,
    tmp.country,
    sub5,
    sub6,
    os,
    os_version,
    click_count,
    host_count,
    earnings,
    approved,
    payouts,
    approved_revenue,
    declined_revenue,
    conversions_count,
    declined_conversions,
    r.region
FROM
    tmp
    LEFT JOIN affise_data_fusion.country_region r ON tmp.country = r.country