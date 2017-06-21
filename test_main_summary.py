import re
from datetime import datetime, date, timedelta
from pprint import pprint

# Get the data using `aws s3 ls telemetry-parquet/main_summary/v4 --recursive --human-readable > telemetry-parquet_main_summary`

path = re.compile(r'main_summary/v4/submission_date_s3=(\d+)/sample_id=(\d+)')
def line_to_parts(l):
    _, _, s, u, n = l.split()
    match = path.match(n)
    if match:
        return {'match': True, 'size': s, 'unit': u, 'date': match.group(1), 'sample_id': match.group(2)}
    return {'match': False, 'line': l}

files = [
    ('telemetry-parquet_main_summary', '20160601'),
    ('telemetry-backfill_main_summary', '20170205')
]

for filename, date in files:
    print("\nAnalyzing {}\n".format(filename))
    with open(filename, 'r') as f:
        lines = [line_to_parts(l) for l in f.readlines() if 'snappy.parquet' in l]

    start = datetime.strptime(date, '%Y%m%d')
    end = datetime.strptime('20170618', '%Y%m%d')
    expected = set([
        ((start + timedelta(days=d)).strftime('%Y%m%d'), str(sid)) 
        for d in xrange(int((end - start).total_seconds() / (60 * 60 * 24)))
        for sid in xrange(100)
    ])

    ok = set([(l['date'], l['sample_id']) for l in lines if l['match'] and 300 < float(l['size']) < 1000 and l['unit'] == 'MiB'])
    print("\nExpecting {} elements, Found {} elements".format(len(expected), len(ok)))

    unmatches = [l for l in lines if not l['match']]
    found = set([(l['date'], l['sample_id']) for l in lines if l['match']])

    days_missing_altogether = {d for d,sid in expected} - {d for d,sid in found}
    missing_altogether = {e for e in (expected - found) if e[0] not in days_missing_altogether}
    missing_because_size = {e for e in (ok - found) if e[0] not in days_missing_altogether}

    print("\nImproper Data: {}".format(len(unmatches)))
    print(unmatches.pop()['line'])

    print("\nDays Missing Altogether: {}".format(len(days_missing_altogether)))
    pprint(days_missing_altogether)

    print("\nPartitions Missing altogether: {}".format(len(missing_altogether)))
    pprint(missing_altogether)

    print("\nFiles Where Size is Off: {}".format(len(missing_because_size)))
    pprint(missing_because_size)
