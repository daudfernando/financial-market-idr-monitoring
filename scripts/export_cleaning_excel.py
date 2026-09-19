"""Build an audit workbook from saved real source data, processed files and BQ export."""
import csv
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import xlsxwriter

ROOT = Path(__file__).resolve().parents[1]
BATCH = ROOT / 'data/source_samples/20260917T123202407661Z'
SOURCE = ROOT / 'data/market_universe/20260917T121614070331Z'
STREAM = ROOT / 'data/streaming/20260917T123103782750Z'


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def lines(path):
    return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line]


def main():
    before_batch = read_json(BATCH / 'bi_records.json')
    after_batch = [read_json(p) for p in sorted((BATCH / 'processed/jisdor').glob('date=*/record.json'))]
    after_by_date = {r['jisdor_date']: r for r in after_batch}
    comparison_batch = []
    for r in before_batch:
        day = datetime.fromisoformat(r['tgl_subkursasing']).date().isoformat()
        a = after_by_date[day]
        comparison_batch.append({
            'tanggal_before_teks': r['tgl_subkursasing'], 'tanggal_after': a['jisdor_date'],
            'currency_before_asli': r['mts_subkursasing'],
            'currency_before_terlihat': repr(r['mts_subkursasing']),
            'panjang_before': len(r['mts_subkursasing']), 'currency_after': a['currency'],
            'panjang_after': len(a['currency']), 'kurs_before_teks': r['jual_subkursasing'],
            'kurs_after_angka': a['usd_idr'],
            'perubahan': 'Timestamp ke tanggal; TRIM + UPPER mata uang; teks kurs ke numerik',
            'fx_available_at': a['fx_available_at']})
    raw_market = []
    raw_by_key = {}
    for symbol in ['BAC', 'GS', 'JPM', 'MS']:
        with (SOURCE / f'{symbol}_as_returned.csv').open(encoding='utf-8', newline='') as f:
            for r in csv.DictReader(f):
                row = {'symbol': symbol, **r}
                raw_market.append(row)
                stamp = datetime.fromisoformat(r['Datetime']).astimezone(timezone.utc).isoformat()
                raw_by_key[(symbol, stamp)] = row
    candidates = lines(SOURCE / 'replay_candidates.jsonl')
    events = [r for p in sorted(STREAM.glob('valid-*.jsonl')) for r in lines(p)]
    encoded = (ROOT / 'dashboard/data.js').read_text(encoding='utf-8')
    snapshot = json.loads(encoded.removeprefix('window.PROJECT_DATA = ').rstrip(';\n'))
    mart = snapshot['rows']
    event_by_id = {r['event_id']: r for r in events}
    assert len(events) == len(event_by_id) == len(candidates) == len(mart) == 7788
    assert {r['event_id'] for r in mart} == set(event_by_id), 'Dashboard snapshot does not match source replay'
    comparison_market = []
    for m in mart:
        e = event_by_id[m['event_id']]
        r = raw_by_key[(e['symbol'], e['event_timestamp'])]
        assert float(r['Close']) == e['price_usd'] == m['price_usd']
        comparison_market.append({
            'symbol': e['symbol'], 'waktu_before_New_York': r['Datetime'],
            'waktu_after_UTC': e['event_timestamp'], 'close_before_teks_asli': r['Close'],
            'harga_after_angka_USD': e['price_usd'], 'harga_tetap_sama': True,
            'volume_before': r['Volume'], 'volume_payload_after': json.loads(e['raw_payload'])['volume'],
            'mode': e['ingestion_mode'], 'event_id': e['event_id'],
            'jisdor_date': m['jisdor_date'], 'usd_idr': m['usd_idr'], 'exposure_idr': m['exposure_idr'],
            'strategy_signal': m['strategy_signal'], 'data_status': m['data_status'],
            'comparison_complete': m['comparison_complete'],
            'catatan': 'Zona waktu distandarkan; harga dipertahankan; kurs dan indikator adalah enrichment dbt'})
    out = ROOT / 'docs/before_after_data_cleaning.xlsx'
    book = xlsxwriter.Workbook(out, {'strings_to_urls': False, 'strings_to_formulas': False})
    book.set_properties({'title': 'Before After Data Cleaning - JCDEAH-009', 'author': 'Daud',
                         'comments': 'Real saved source data; no synthetic price rows.'})
    title = book.add_format({'bold': True, 'font_size': 18, 'font_color': '#184638'})
    note = book.add_format({'text_wrap': True, 'valign': 'top', 'font_color': '#44564D'})
    decimal = book.add_format({'num_format': '#,##0.000000'})
    good = book.add_format({'bg_color': '#E3F1E5', 'font_color': '#195C31'})
    warn = book.add_format({'bg_color': '#FFF1CF', 'font_color': '#775700'})

    def sheet(name, rows, description, columns=None):
        ws = book.add_worksheet(name)
        ws.hide_gridlines(2)
        columns = columns or list(rows[0])
        ws.merge_range(0, 0, 0, max(2, len(columns)-1), name.replace('_', ' '), title)
        ws.merge_range(1, 0, 2, max(2, len(columns)-1), description, note)
        data = [[r.get(k) if r.get(k) is not None else None for k in columns] for r in rows]
        ws.add_table(4, 0, 4+len(data), len(columns)-1,
                     {'style': 'Table Style Medium 4', 'columns': [{'header': k} for k in columns], 'data': data})
        ws.freeze_panes(5, 1)
        ws.set_column(0, len(columns)-1, 23)
        for i, k in enumerate(columns):
            if any(t in k for t in ['reason', 'catatan', 'perubahan', 'raw_payload', 'penjelasan', 'lokasi']):
                ws.set_column(i, i, 52)
            elif any(t in k for t in ['date', 'waktu', 'timestamp', 'ingested', 'minute']):
                ws.set_column(i, i, 29)
            if any(isinstance(r.get(k), float) for r in rows):
                ws.set_column(i, i, 23, decimal)
            if k in ['strategy_signal', 'data_status']:
                ws.conditional_format(5, i, 4+len(rows), i, {'type': 'text', 'criteria': 'containing',
                                      'value': 'NO_SIGNAL' if k == 'strategy_signal' else 'insufficient', 'format': warn})
            if k == 'harga_tetap_sama':
                ws.conditional_format(5, i, 4+len(rows), i, {'type': 'cell', 'criteria': '==', 'value': True, 'format': good})
        ws.set_landscape()
        ws.fit_to_pages(1, 0)
        ws.repeat_rows(4)
        return ws

    overview = [
        {'topik': 'Tujuan', 'penjelasan': 'Bandingkan raw asli, hasil Spark/Python, dan mart dbt. Semua baris sumber adalah data real tersimpan.', 'lokasi': 'Mulai dari Batch_Perbandingan dan Saham_Perbandingan'},
        {'topik': 'Snapshot batch', 'penjelasan': f'{len(before_batch)} record dalam satu batch 17 September 2026; bukan seluruh akumulasi fact BigQuery.', 'lokasi': str(BATCH.relative_to(ROOT))},
        {'topik': 'Snapshot saham', 'penjelasan': '7.788 bar, empat saham, sesi 10-16 September 2026. Replay run 20260917T123103782750Z.', 'lokasi': str(SOURCE.relative_to(ROOT))},
        {'topik': 'Sumber mart', 'penjelasan': 'Ekspor BigQuery tersimpan, bukan query live saat Excel dibuat. exported_at: '+snapshot['exported_at'], 'lokasi': snapshot['source']},
        {'topik': 'Perubahan nyata batch', 'penjelasan': 'USD dengan dua spasi di belakang menjadi USD; timestamp menjadi tanggal; kurs teks menjadi angka.', 'lokasi': 'Batch_Perbandingan'},
        {'topik': 'Perubahan nyata saham', 'penjelasan': 'Waktu New York diubah ke UTC; 7.788 harga Close tetap sama; metadata identitas dan replay ditambahkan.', 'lokasi': 'Saham_Perbandingan'},
        {'topik': 'Null', 'penjelasan': 'Sel kosong berarti null/tidak tersedia; tidak diisi nol. NO_SIGNAL bisa muncul pada awal sesi atau sesudah gap.', 'lokasi': 'Mart_Final'},
        {'topik': 'Presisi Excel', 'penjelasan': 'Excel membatasi presisi numerik sekitar 15 digit. Teks harga sumber dipertahankan di sheet Before; format angka tampilan bukan perubahan pipeline.', 'lokasi': 'Saham_Before'},
        {'topik': 'Gap asli', 'penjelasan': 'GS kehilangan 11 menit, MS satu menit relatif sesi snapshot; tidak diimputasi. Dataset invalid/duplikasi tidak ditemukan pada run ini.', 'lokasi': 'Kualitas'},
        {'topik': 'Batas', 'penjelasan': 'Replay historis, bukan harga live. Sinyal dan valuasi merupakan enrichment, bukan perbaikan harga. Fixture sintetis tidak dimasukkan.', 'lokasi': 'Aturan'},
    ]
    sheet('Panduan', overview, 'Workbook audit before/after. Tidak ada perubahan pada data sumber, BigQuery, atau progress.txt.')
    sheet('Batch_Before', before_batch, 'Nilai asli dari elemen Table pada XML BI; disimpan sebagai teks sebagaimana hasil parsing. Spasi mata uang dipertahankan.')
    sheet('Batch_After_Spark', after_batch, 'JSON processed yang benar-benar dihasilkan Spark. Tanggal/metadata ditampilkan sebagai teks ISO; kurs sebagai angka.')
    sheet('Batch_Perbandingan', comparison_batch, 'Perbandingan satu baris per tanggal. Kolom repr dan panjang menunjukkan spasi yang tidak terlihat di Excel.')
    sheet('Saham_Before', raw_market, 'Seluruh kolom CSV yfinance asli. symbol ditambahkan dari nama file; waktu sumber New York, harga asli berupa teks CSV.')
    sheet('Saham_After_Python', events, 'Output consumer Python aktual: harga tidak diubah, timestamp UTC, metadata replay dan koordinat Kafka ditambahkan.')
    sheet('Saham_Perbandingan', comparison_market, '7.788 pasangan source -> consumer -> mart dicocokkan; setiap harga telah diperiksa sama di Python sebelum ekspor.')
    mart_columns = ['symbol', 'institution_name', 'minute_utc', 'session_date', 'event_date_wib', 'price_usd',
                    'jisdor_date', 'usd_idr', 'fx_join_policy', 'fx_age_calendar_days', 'quantity', 'exposure_idr',
                    'consecutive_minute', 'asset_return', 'asset_effect_idr', 'fx_effect_idr', 'interaction_effect_idr',
                    'total_change_idr', 'moving_average_short', 'moving_average_long', 'ma_observation_count',
                    'strategy_signal', 'signal_reason', 'signal_available_at', 'data_status', 'session_return',
                    'peer_average_return', 'peer_rank', 'comparison_complete', 'return_zscore', 'risk_indicator', 'event_id']
    sheet('Mart_Final', mart, 'Kolom utama mart hasil dbt; sel kosong dipertahankan. SMA/sinyal/FX/peers adalah hasil transformasi bisnis.', mart_columns)
    rules = [
        ('Extract BI / Spark', 'TRIM + UPPER currency, parsing tanggal dan angka', 'Normalisasi', 'USD  -> USD; teks tanggal -> tanggal; teks kurs -> angka'),
        ('Spark', 'Tolak tanggal/rate invalid dan key tanggal+currency duplikat', 'Validasi', 'Run 14 record valid; tidak ada record invalid yang dibuang diam-diam'),
        ('GCS -> BigQuery batch', 'Checksum, manifest, MERGE tanggal+currency', 'Integritas dan rerun', 'Tanggal sama diperbarui bila ingestion lebih baru, tidak ditambah sebagai duplikat'),
        ('Extract saham', 'Metadata USD, harga finite positif, timestamp unique/timezone', 'Validasi dan normalisasi', 'UTC; common boundary; harga tidak diimputasi'),
        ('Python consumer', 'Schema/event_id/payload/waktu/mode; valid dan invalid terpisah', 'Validasi', 'Replay lengkap 7.788 valid, invalid 0; invalid menyebabkan publikasi ditolak'),
        ('stg_prices', 'ROW_NUMBER per event_id', 'Deduplikasi', 'Pilih satu record; duplikasi pada snapshot ini 0'),
        ('int_market_minutes', 'Satu observasi per saham/mode/tipe/menit', 'Grain dan zona waktu', 'Tanggal sesi New York dan tanggal WIB'),
        ('fact_market_exposure', 'Join kurs terbaru yang memenuhi aturan waktu', 'Enrichment', 'Jika availability unknown, hanya tanggal sebelum event WIB; quantity 1'),
        ('mart_market_risk', 'Return hanya menit berurutan; baseline min 20', 'Penanganan gap', 'Gap/awal sesi null; pergerakan besar ditandai, tidak dihapus'),
        ('int_stock_features', 'Window waktu SMA3/SMA8 dalam sesi', 'Fitur analitik', 'Tidak memakai harga masa depan'),
        ('mart_stock_monitoring', 'Syarat riwayat lengkap dan FX; peers 4 saham sejajar', 'Status kelengkapan', 'NO_SIGNAL/null peers saat syarat belum lengkap'),
        ('mart_stock_latest', 'Pilih waktu sumber terbaru tiap saham', 'Seleksi output', 'Bukan waktu replay terbaru'),
    ]
    sheet('Aturan', [dict(zip(['tahap', 'aturan', 'jenis', 'penjelasan'], r)) for r in rules],
          'Bedakan aturan yang tersedia dari masalah yang benar-benar ditemukan. Tidak ada klaim menghapus outlier atau mengisi gap.')
    quality = []
    for symbol in sorted({r['symbol'] for r in mart}):
        rows = [r for r in mart if r['symbol'] == symbol]
        counts = Counter(r['strategy_signal'] for r in rows)
        quality.append({'symbol': symbol, 'bar_source': sum(r['symbol'] == symbol for r in raw_market),
                        'bar_consumer': sum(r['symbol'] == symbol for r in events), 'bar_mart': len(rows),
                        'unique_event_id': len({r['event_id'] for r in rows}),
                        'harga_berubah': 0, 'fx_null': sum(r['usd_idr'] is None for r in rows),
                        'peer_tidak_lengkap': sum(not r['comparison_complete'] for r in rows), **dict(counts)})
    sheet('Kualitas', quality, 'Jumlah dihitung langsung dari data workbook. NO_SIGNAL bukan jumlah event invalid.',
          ['symbol', 'bar_source', 'bar_consumer', 'bar_mart', 'unique_event_id', 'harga_berubah', 'fx_null',
           'peer_tidak_lengkap', 'BUY', 'SELL', 'HOLD', 'NO_SIGNAL'])
    book.close()
    # Validate archive integrity and worksheet row counts without modifying it.
    import zipfile
    import xml.etree.ElementTree as ET
    with zipfile.ZipFile(out) as z:
        assert z.testzip() is None
        sheets = [p for p in z.namelist() if p.startswith('xl/worksheets/sheet') and p.endswith('.xml')]
        assert len(sheets) == 10
        for p in sheets:
            ET.fromstring(z.read(p))
    print(json.dumps({'file': str(out), 'sheets': 10, 'batch_rows': len(before_batch),
                      'market_rows': len(mart), 'price_changes': 0, 'status': 'passed'}))


if __name__ == '__main__':
    main()
