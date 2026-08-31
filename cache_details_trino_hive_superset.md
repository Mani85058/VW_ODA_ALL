Cache Status Summary â€” Trino Â· Hive Metastore Â· Superset (ESQA PROD)
================================================================

-------------------------------------------------------------------------------
TRINO (v479, Helm chart 1.42.1)
-------------------------------------------------------------------------------
Status: No caching enabled, no fs.cache.enabled

- config.properties       â€” No result cache, no file/data cache, no cache.enabled property set.
- delta.properties        â€” no fs.cache.enabled or any cache-related key present.
- hive.properties         â€” no cache-related key present.
- Trino's native file cache (cache.base-directory, cache.enabled) and result
  cache are NOT configured.
- No external cache layer is in use alongside Trino.



-------------------------------------------------------------------------------
HIVE METASTORE (v4.0.1)
-------------------------------------------------------------------------------
Status: Hadoop FS object cache disabled; no metastore client or JDO L2 cache configured

From the hive-site.xml
<property>
    <name>fs.file.impl.disable.cache</name>
    <value>true</value>
</property>
<property>
    <name>fs.hdfs.impl.disable.cache</name>
    <value>true</value>
</property>
<property>
    <name>fs.s3.impl.disable.cache</name>
    <value>true</value>
</property>
<property>
    <name>fs.s3a.impl.disable.cache</name>
    <value>true</value>



Property                          Value   Meaning
--------------------------------  ------  -----------------------------------------------
fs.file.impl.disable.cache        true    Hadoop local FS object cache DISABLED
fs.hdfs.impl.disable.cache        true    HDFS FS object cache DISABLED
fs.s3.impl.disable.cache          true    S3 FS object cache DISABLED
fs.s3a.impl.disable.cache         true    S3A FS object cache DISABLED


All FS-level caches are intentionally disabled (recommended â€” avoids heap memory
leaks). No metadata caching is active at the Hive layer.




-------------------------------------------------------------------------------
SUPERSET (v4.1.3) + VALKEY (Redis-compatible)
-------------------------------------------------------------------------------
Status: Caching fully enabled via Valkey

Cache Configuration:
Config                        Type        TTL (s)   Key Prefix                          DB
----------------------------  ----------  --------  ----------------------------------  ---
CACHE_CONFIG                  RedisCache  300       superset_                           db1
DATA_CACHE_CONFIG             RedisCache  300       superset_                           db1
FILTER_STATE_CACHE_CONFIG     RedisCache  86400     superset_filter_cache               db1
EXPLORE_FORM_DATA_CACHE_CONFIG RedisCache 86400     superset_explore_form_data_cache    db1
RESULTS_BACKEND               RedisCache  â€”         superset_results                    db1

CACHE_CONFIG = {
      'CACHE_TYPE': 'RedisCache',
      'CACHE_DEFAULT_TIMEOUT': 300,
      'CACHE_KEY_PREFIX': 'superset_',
      'CACHE_REDIS_URL': CACHE_REDIS_URL,
DATA_CACHE_CONFIG = CACHE_CONFIG
  broker_url = CELERY_REDIS_URL
  result_backend = CELERY_REDIS_URL
RESULTS_BACKEND = RedisCache(
      host=env('REDIS_HOST'),
      password=env('REDIS_PASSWORD'),
      port=env('REDIS_PORT'),
FILTER_STATE_CACHE_CONFIG = {
  'CACHE_TYPE': 'RedisCache',
  'CACHE_DEFAULT_TIMEOUT': 86400,
  'CACHE_KEY_PREFIX': 'superset_filter_cache',
  'CACHE_REDIS_URL': CACHE_REDIS_URL
EXPLORE_FORM_DATA_CACHE_CONFIG = {
  'CACHE_TYPE': 'RedisCache',
  'CACHE_DEFAULT_TIMEOUT': 86400,
  'CACHE_KEY_PREFIX': 'superset_explore_form_data_cache',
  'CACHE_REDIS_URL': CACHE_REDIS_URL
RATELIMIT_STORAGE_URI = CACHE_REDIS_URL
    broker_url = CELERY_REDIS_URL
    result_backend = CELERY_REDIS_URL


-------------------------------------------------------------------------------
SUMMARY TABLE
-------------------------------------------------------------------------------
Component         Cache Enabled   Mechanism                   Storage           TTL
----------------  --------------  --------------------------  ----------------  ----------
Trino             NO              â€”                           â€”                 â€”
Hive Metastore    NO              All FS caches disabled      â€”                 â€”
Superset          YES             Valkey (Redis-compatible)   In-memory 512 MB  86,400 s (24h)