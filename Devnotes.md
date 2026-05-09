# Development notes

## Goals

- Quick/efficient inserts
- High level statistics (total counts, etc)
- Statistics per measurement+field
- Search for last value
  - By uid+measurement+field
  - By field (e.g. Power Active Total)
    - Filter by tag?
- Search for set of last values
- Inactive uid list
- Inactive field list

## Data structure

Top level struct containing:

- Statistics
  - Uid count (unique uids seen since startup)
  - Field count (raw, all fields in all messages)
  - Kafka message count
  - Influx line count (can be more than one line in each Kafka message)
  - First message timestamp
  - Last message timestamp
- Basic information
  - Startup time
- Settings
  - Timestamp precision
  - Inactivity thresholds?
  - Output measurement
- Data array/hashmap - UidInfo

UidInfo struct

- Statistsics
  - First seen
  - Last seen
  - Seen count
- Data
  - Uid
  - Tags
  - Values/Field+Measurements

Value struct

- Statistics
  - First seen
  - Last seen
  - Seen count
  - Estimated frequency
- Data
  - Last 5 values
  - Last 5 timestamps

## Openmetrics format

Openmetrics:

```
# HELP <metricname> <some help text or description>
# TYPE <metricname> [counter|gauge|etc]
# UNIT <metricname> [seconds|fields|etc] # If UNIT is used, it must also be used as the suffix.
metricname[_unit]_suffix{[label_a=value_a]} [int64|float64|bool]
# HELP service_uid_total Current count of unique uids in cache
# TYPE service_uid_total gauge
service_uid_total <val>
# HELP service_fields_seen_total Fields processed by service
# TYPE service_fields_seen_total counter
# UNIT service_fields_seen_total fields
service_fields_seen_total <val>
```

## Considerations

- Switch to using array for fields, with a hashmap mapping from field name to array indices?
  - Premature optimization?
- Store tagset for field/uid also?
  - Will tagset always be the same for a single uid?
  - Hashmap to array index won't make any difference for uid since tagset will be unique.
  - Hashmap to array index might make sense if storing per field.
