#!/usr/bin/env python3
"""Generate sqllogictest for large IN + filter pushdown + projection mismatch bug."""

from pathlib import Path

IN_LIST_SIZE = 2776
OUTPUT = Path(__file__).resolve().parents[1] / "test/sql/large_in_filter_projection.test"

HEADER = """# name: test/sql/large_in_filter_projection.test
# description: Reproduce BOOL/DOUBLE mismatch when scanning projected columns with large IN filter
# group: [sql]

require altertable

require-env ALTERTABLE_TEST_HOST

require-env ALTERTABLE_TEST_PORT

require-env ALTERTABLE_TEST_USER

require-env ALTERTABLE_TEST_PASSWORD

require-env ALTERTABLE_TEST_SSL

statement ok
ATTACH 'host=${ALTERTABLE_TEST_HOST} port=${ALTERTABLE_TEST_PORT} user=${ALTERTABLE_TEST_USER} password=${ALTERTABLE_TEST_PASSWORD} ssl=${ALTERTABLE_TEST_SSL}' AS large_in_db (TYPE ALTERTABLE)

statement ok
CALL altertable_execute('large_in_db', 'DROP TABLE IF EXISTS __sqllogic_wide_scan_probe')

statement ok
CALL altertable_execute('large_in_db', 'CREATE TABLE __sqllogic_wide_scan_probe (score_a DOUBLE, score_b DOUBLE, min_rate DOUBLE, enc_bitrate INTEGER, frame_rate DOUBLE, dim_w SMALLINT, dim_h SMALLINT, flag_sim BOOLEAN, flag_test BOOLEAN, event_day DATE, venue_id VARCHAR)')

statement ok
CALL altertable_execute('large_in_db', 'INSERT INTO __sqllogic_wide_scan_probe VALUES (0.1, 0.2, 0.3, 1000, 30.0, 1920, 1080, false, false, DATE ''2024-06-15'', ''venue_0'')')

statement ok
CALL altertable_execute('large_in_db', 'INSERT INTO __sqllogic_wide_scan_probe VALUES (0.4, 0.5, 0.6, 2000, 60.0, 1280, 720, false, false, DATE ''2024-07-01'', ''venue_1'')')

# Small IN list control: should succeed
query RIRIRII
SELECT score_a, score_b, min_rate, enc_bitrate, frame_rate, dim_w, dim_h
FROM large_in_db.main.__sqllogic_wide_scan_probe
WHERE flag_sim = false
  AND flag_test = false
  AND event_day BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
  AND venue_id IN ('venue_0', 'venue_1')
USING SAMPLE 10 ROWS
ORDER BY score_a
----
0.1	0.2	0.3	1000	30.0	1920	1080
0.4	0.5	0.6	2000	60.0	1280	720

"""

in_list = ", ".join(f"'venue_{i}'" for i in range(IN_LIST_SIZE))

LARGE_QUERY = f"""# Large IN list: triggers filter-column / projection mismatch in ScanChunk
query RIRIRII
SELECT score_a, score_b, min_rate, enc_bitrate, frame_rate, dim_w, dim_h
FROM large_in_db.main.__sqllogic_wide_scan_probe
WHERE flag_sim = false
  AND flag_test = false
  AND event_day BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
  AND venue_id IN ({in_list})
USING SAMPLE 10 ROWS
ORDER BY score_a
----
0.1	0.2	0.3	1000	30.0	1920	1080
0.4	0.5	0.6	2000	60.0	1280	720

"""

FOOTER = """statement ok
CALL altertable_execute('large_in_db', 'DROP TABLE __sqllogic_wide_scan_probe')
"""

OUTPUT.write_text(HEADER + LARGE_QUERY + FOOTER)
print(f"Wrote {OUTPUT} ({OUTPUT.stat().st_size} bytes, IN list size={IN_LIST_SIZE})")
