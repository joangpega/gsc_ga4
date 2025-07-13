WITH ga4_data AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_url,
    COUNT(DISTINCT CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'))) AS sessions,
    --añadir usuarios que han comprado y su landing page fue igual que page_url en GSC. session_start o first_visit. filtrar google / organic
    COUNT(DISTINCT user_pseudo_id) AS users,
    COUNTIF(event_name = 'purchase') AS total_purchases,
    COUNTIF(event_name = 'page_view') AS total_page_view,
    COUNTIF(event_name = 'begin_checkout') AS total_begin_checkout,
    COUNTIF(event_name = 'add_to_cart') AS total_add_to_cart
  FROM
    `joseangexxxxxx.analytics_3xxxxxxx9.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) 
                     AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND NOT REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'#')
  GROUP BY
    date, page_url
),
gsc_data AS (
  SELECT
    data_date AS date,
    url AS page_url,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    IFNULL(SUM(clicks), 0) / NULLIF(SUM(impressions), 0) AS ctr,
    country,
    case 
    when query is null then "No disponible"
    else query
    end as query,
    search_type,
    device,
  FROM
    `joseangexxxxxxxxom.searchconsole.searchdata_url_impression`
  WHERE
    data_date BETWEEN DATE_TRUNC(CURRENT_DATE(), YEAR) 
                  AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND search_type = 'WEB'
    AND NOT CONTAINS_SUBSTR(url, '#')
    and clicks >= 1
  GROUP BY
    date, page_url, country, query, search_type, device
)
SELECT
  gsc.date,
  gsc.page_url,
  gsc.clicks,
  gsc.impressions,
  gsc.ctr,
  gsc.country,
  gsc.query,
  gsc.search_type,
  gsc.device,
  IFNULL(ga4.users, 0) AS users,
  IFNULL(ga4.sessions, 0) AS sessions,
  IFNULL(ga4.total_page_view, 0) AS total_page_view,
  IFNULL(ga4.total_purchases, 0) AS total_purchases,
  IFNULL(ga4.total_begin_checkout, 0) AS total_begin_checkout,
  IFNULL(ga4.total_add_to_cart, 0) AS total_add_to_cart
FROM
  gsc_data gsc
LEFT JOIN
  ga4_data ga4
ON
  gsc.page_url = ga4.page_url AND gsc.date = ga4.date
ORDER BY
  gsc.date DESC,
  gsc.clicks DESC;
