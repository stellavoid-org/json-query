# json-query

A small Python CLI for:
- Normalizing mixed JSON inputs (JSON array / NDJSON / single object) into one NDJSON
- Generating a flattened extraction schema (policy A: arrays/objects kept as JSON columns)
- Building a Parquet dataset with DuckDB
- Querying and exporting CSV

## Install (editable)

```bash
git clone https://github.com/stellavoid-org/json-query
pip install -e ./json-query
```

- optional: install with venv
```bash
python -m venv venv && source ./venv/bin/activate && pip install ./json-query
```

## Quick start
```bash
tree
.
├── json-query
│   ├── ....
│   ...
├── output_sample
└── sample_json
    └── sample_loadbalancer.json
```

Build a dataset:

```bash
json-query build \
  --in sample_json \
  --glob "*.json" \
  --work output_sample \
  --sample 20000
```

```bash
tree
.
├── json-query
│   ├── ....
│   ...
├── output_sample
│   ├── all.ndjson
│   ├── flat_parquet
│   ├── flat_select.sql
│   └── work.duckdb
└── sample_json
    └── sample_loadbalancer.json
```

Run queries (view `v` points at the Parquet dataset):

```bash
json-query query --work output_sample "SELECT COUNT(*) FROM v"
```

then
```bash
count_star()
500
```


Export CSV:

```bash
json-query export-csv --work output_sample --out output_sample/out.csv "SELECT * FROM v LIMIT 1000"
```

then
| raw_json          | insert_id       | event                    | backend_timeout_ms | security_policy                       | policy_decision | enforcement | project               | proto_type                                                               | trace_hash                       | country | client_port | trace_id                             | client_ip      | edge | method | url                                                                                                                                                                                                                                                                                  | resp_size | http_status |
| ----------------- | --------------- | ------------------------ | -----------------: | ------------------------------------- | --------------- | ----------- | --------------------- | ------------------------------------------------------------------------ | -------------------------------- | ------- | ----------: | ------------------------------------ | -------------- | ---- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------: | ----------: |
| {long_raw_json….} | 1rb65s0g3fnje9p | response_sent_by_backend |              10000 | pulsar-flow-whitelist-security-policy | ALLOW           | ACCEPT      | projects/12345678910 | type.googleapis.com/google.cloud.loadbalancing.type.LoadBalancerLogEntry | 4ea056e63b7910cbf543f0c095064dfe | US      |       14618 | t13d3113h1_e8f1e7e78f70_ce5650b735ce | 192.168.0.4 | IAD  | GET    | [https://my-beautiful-domain.com/v1/files/tmp/copilot_private_repo_demo_02c.png](https://my-beautiful-domain.com/v1/files/tmp/copilot_private_repo_demo_02c.png)                                                                                                                     |       233 |         200 |
| {long_raw_json….} | 1yzd3og3msz27y  | response_from_cache      |                    |                                       |                 |             | projects/12345678910 | type.googleapis.com/google.cloud.loadbalancing.type.LoadBalancerLogEntry |                                  |         |             |                                      | 192.168.0.5 | IAD  | GET    | [https://my-beautiful-domain.com/v1/files/tmp/copilot_private_repo_demo_02c.png](https://my-beautiful-domain.com/v1/files/tmp/copilot_private_repo_demo_02c.png)                                                                                                                     |       197 |         200 |
| {long_raw_json….} | 1a4c6gcg3ye1cam | response_from_cache      |                    |                                       |                 |             | projects/12345678910 | type.googleapis.com/google.cloud.loadbalancing.type.LoadBalancerLogEntry |                                  |         |             |                                      | 192.168.0.6  | IAD  | GET    | [https://my-beautiful-domain.com/v1/files/tmp/copilot_private_repo_demo_03c.png](https://my-beautiful-domain.com/v1/files/tmp/copilot_private_repo_demo_03c.png)                                                                                                                     |       197 |         200 |
| {long_raw_json….} | ab9zg3fj1pp8t   | response_from_cache      |                    |                                       |                 |             | projects/12345678910 | type.googleapis.com/google.cloud.loadbalancing.type.LoadBalancerLogEntry |                                  |         |             |                                      | 192.168.0.7 | NRT  | GET    | [https://my-beautiful-domain.com/v1/files/tmp/copilot_private_repo_demo_01c.png](https://my-beautiful-domain.com/v1/files/tmp/copilot_private_repo_demo_01c.png)                                                                                                                     |       755 |         200 |
| {long_raw_json….} | 320ublfwii2xh   | cloud_redirect           |                    |                                       |                 |             | projects/149666284582 | type.googleapis.com/google.cloud.loadbalancing.type.LoadBalancerLogEntry |                                  |         |             |                                      | 192.168.0.8 |      | GET    | [https://my-beautiful-domain.com/favicon.ico](https://my-beautiful-domain.com/favicon.ico)                                                                                                                                                                                           |       691 |         301 |
| {long_raw_json….} | 1e8nz4pg36xtfd6 | response_sent_by_backend |              10000 | pulsar-flow-whitelist-security-policy | ALLOW           | ACCEPT      | projects/12345678910 | type.googleapis.com/google.cloud.loadbalancing.type.LoadBalancerLogEntry | de62b1c1a1860dded17275e0b45978bf | JP      |       53813 | t13d260900_6d1bcf7a4624_188c7f576dcd | 192.168.0.9 | NRT  | GET    | [https://my-beautiful-domain.com/v1/files/tmp/copilot_private_repo_demo_01c.png?_sm_pdc=1&_sm_rid=FMQ6RZDtfNvkrnkRPtt5fZJP2NZJQqHZ3DQMH4N](https://my-beautiful-domain.com/v1/files/tmp/copilot_private_repo_demo_01c.png?_sm_pdc=1&_sm_rid=FMQ6RZDtfNvkrnkRPtt5fZJP2NZJQqHZ3DQMH4N) |       862 |         200 |

## Notes

- Default Parquet output is a *directory dataset* (multiple `.parquet` files). This is fine; the CLI reads them all via `read_parquet('.../*.parquet')`.
- Scalars are extracted as strings to avoid type drift. Cast in SQL as needed.


e.g. Query json
raw json:
```json
{
  "httpRequest": {
    "remoteIp": "192.168.0.1",
    "requestMethod": "GET"
  }
}
```
Column name flatten rule for nested keys: joined by double under score "__"
- httpRequest.remoteIp → httpRequest__remoteIp
- httpRequest.requestMethod → httpRequest__requestMethod

Get specific value
```bash
json-query query --work output_sample \
"SELECT *
 FROM v
 WHERE httpRequest__remoteIp = '192.168.0.1'
 LIMIT 100" \
> output_sample/query1.csv
```

Get in multiple values
```bash
json-query query --work output_sample \
"SELECT *
 FROM v
 WHERE httpRequest__remoteIp IN ('192.168.0.1','192.168.0.2')
 LIMIT 100" \
> output_sample/query2.csv
```

Get unique
```bash
json-query query --work output_sample \
  "SELECT DISTINCT httpRequest__remoteIp
  FROM v
  WHERE httpRequest__remoteIp IS NOT NULL
  ORDER BY 1
  LIMIT 1000" \
> output_sample/query3.csv
```

Count unique
```bash
json-query query --work output_sample \
  "SELECT COUNT(DISTINCT httpRequest__remoteIp) AS uniq_ips
  FROM v
  WHERE httpRequest__remoteIp IS NOT NULL" \
> output_sample/query4.csv
```

e.g. timestamp in schema
```json
{
  "timestamp": "2026-01-01T00:00:00Z",
  "httpRequest": {
    "remoteIp": "192.168.0.1",
    "requestMethod": "GET"
  }
}
```

Timestamp + count
```bash
json-query query --work output_sample "
SELECT COUNT(*) AS n
FROM v
WHERE try_cast(timestamp AS TIMESTAMP)
      BETWEEN TIMESTAMP '2026-01-01 00:00:00' AND TIMESTAMP '2026-01-02 00:00:00'
" \
> output_sample/query5.csv
```

Count records in timestamp by unique ip
```bash
json-query query --work output_sample "
SELECT COUNT(DISTINCT httpRequest__remoteIp) AS uniq_ips
FROM v
WHERE try_cast(timestamp AS TIMESTAMP)
      BETWEEN TIMESTAMP '2026-01-01 00:00:00' AND TIMESTAMP '2026-01-02 00:00:00'
  AND httpRequest__remoteIp IS NOT NULL
" \
> output_sample/query6.csv
```

Ip ranking in timestamp
```bash
json-query query --work output_sample "
SELECT httpRequest__remoteIp, COUNT(*) AS n
FROM v
WHERE try_cast(timestamp AS TIMESTAMP)
      BETWEEN TIMESTAMP '2026-01-01 00:00:00' AND TIMESTAMP '2026-01-02 00:00:00'
  AND httpRequest__remoteIp IS NOT NULL
GROUP BY 1
ORDER BY n DESC
LIMIT 50
" \
> output_sample/query4.csv
```

When timestamp with "Z", use TIMESTAMPTZ instead of TIMESTAMP
```bash
json-query query --work output_sample "
SELECT COUNT(DISTINCT httpRequest__remoteIp) AS uniq_ips
FROM v
WHERE try_cast(timestamp AS TIMESTAMPTZ)
      BETWEEN TIMESTAMPTZ '2026-01-06T00:00:00Z' AND TIMESTAMPTZ '2026-01-16T00:00:00Z'
  AND httpRequest__remoteIp IS NOT NULL
" \
> query_timestampz.csv
```

then
```bash
uniq_ips
74
```

Timestamp + IP + Column Extraction
```bash
json-query export-csv --work output_sample --out output_sample/records_ip_in.csv "
SELECT
  timestamp,
  httpRequest__remoteIp,
  httpRequest__requestMethod,
  httpRequest__requestUrl,
  httpRequest__status
FROM v
WHERE try_cast(timestamp AS TIMESTAMPTZ)
      BETWEEN TIMESTAMPTZ '2026-01-01T00:00:00Z' AND TIMESTAMPTZ '2026-01-02T00:00:00Z'
  AND httpRequest__remoteIp IN ('192.168.0.1')
"
```