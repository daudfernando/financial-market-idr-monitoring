"""JISDOR collection shared by exploration and the scheduled batch."""
from datetime import date as calendar_date, datetime, timezone
import json
import math
import xml.etree.ElementTree as ET
from curl_cffi import requests

def save(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def collect_bi(folder, prepare_processed=True, start_date=None, end_date=None):
    if bool(start_date) != bool(end_date):
        raise ValueError('Both interval dates required')
    params = None
    if start_date:
        start, end = calendar_date.fromisoformat(start_date), calendar_date.fromisoformat(end_date)
        if not 0 <= (end-start).days <= 365:
            raise ValueError('JISDOR interval must be 0..365 days')
        params = {'mts':'USD','startDate':start_date,'endDate':end_date}
    operation = 'getSubKursJisdor3' if params else 'getSubKursJisdor1'
    url = 'https://www.bi.go.id/biwebservice/wskursbi.asmx/' + operation
    result = {'source': url, 'status': 'failed', 'attempts': []}
    if params:
        result['requested_interval'] = params
    for method in ['GET', 'POST']:
        try:
            kwargs = ({'params':params} if method == 'GET' else {'data':params}) if params else {}
            response = requests.request(method, url, impersonate='chrome', timeout=25, **kwargs)
            (folder / f'bi_{method.lower()}.xml').write_bytes(response.content)
            result['attempts'].append({'method': method, 'http_status': response.status_code})
            response.raise_for_status()
            root = ET.fromstring(response.content)
            rows = [{c.tag.split('}')[-1]: c.text for c in node}
                    for node in root.iter() if node.tag.split('}')[-1] == 'Table']
            if not rows:
                raise ValueError('No Table records in response')
            save(folder / 'bi_records.json', rows)
            cleaned = []
            seen = set()
            for row in rows:
                date = datetime.fromisoformat(row['tgl_subkursasing']).date().isoformat()
                if params and not start_date <= date <= end_date:
                    raise ValueError('BI returned date outside requested interval')
                currency = row['mts_subkursasing'].strip().upper()
                rate = float(row['jual_subkursasing'])
                if currency != 'USD' or not math.isfinite(rate) or rate <= 0:
                    raise ValueError('Invalid JISDOR currency or rate')
                if (date, currency) in seen:
                    raise ValueError('Duplicate JISDOR date and currency')
                seen.add((date, currency))
                cleaned.append({'jisdor_date': date, 'currency': currency, 'usd_idr': rate,
                                'source': url, 'ingested_at': datetime.now(timezone.utc).isoformat(),
                                'fx_available_at': None})
            # Only publish processed partitions after all records pass validation.
            for row in cleaned if prepare_processed else []:
                partition = folder / 'processed' / 'jisdor' / f'date={row["jisdor_date"]}'
                partition.mkdir(parents=True)
                save(partition / 'record.json', row)
            result.update(status='passed', records=len(rows), sample=rows[0],
                          first_date=min(r['jisdor_date'] for r in cleaned),
                          last_date=max(r['jisdor_date'] for r in cleaned))
            return result
        except Exception as exc:
            result['attempts'].append({'method': method, 'error': f'{type(exc).__name__}: {exc}'})
    return result

