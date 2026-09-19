"""Read real batch/replay results from BigQuery and export an auditable workbook."""
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import zipfile
import xlsxwriter
from google.cloud import bigquery
from gcp_auth import bigquery_client

ROOT = Path(__file__).resolve().parents[1]


def main():
    client = bigquery_client('jcdeah-009', 'asia-southeast2', True)
    tables = {
        'Batch_JISDOR': ('jcdeah-009.daud_finalproject.fact_jisdor_daily', 'jisdor_date'),
        'Streaming_Replay': ('jcdeah-009.daud_finalproject.stg_market_replay_demo', 'symbol,event_timestamp'),
        'Mart_Hasil': ('jcdeah-009.daud_finalproject_demo.mart_stock_monitoring', 'symbol,minute_utc'),
        'Latest_Saham': ('jcdeah-009.daud_finalproject_demo.mart_stock_latest', 'symbol'),
    }
    fetched, jobs = {}, {}
    for name, (relation, order) in tables.items():
        job = client.query(f'SELECT * FROM `{relation}` ORDER BY {order} LIMIT 10001',
                           job_config=bigquery.QueryJobConfig(maximum_bytes_billed=104857600))
        rows = [dict(r) for r in job.result(timeout=120)]
        if not rows or len(rows) > 10000:
            raise ValueError(f'{name}: empty/oversize export refused')
        fetched[name], jobs[name] = rows, job.job_id
        print(name, len(rows), flush=True)
    mart, raw = fetched['Mart_Hasil'], fetched['Streaming_Replay']
    assert len({r['event_id'] for r in mart}) == len(mart)
    raw_by_id = {r['event_id']: r for r in raw}
    assert {r['event_id'] for r in mart} == set(raw_by_id)
    for r in mart:
        assert r['ingestion_mode'] == 'replay' and r['data_kind'] == 'real_historical_replay'
        assert r['price_usd'] == raw_by_id[r['event_id']]['price_usd']
        ready = (r['ma_observation_count'] == 8 and r['previous_ma_count'] == 8
                 and r['consecutive_minute'] is True and r['usd_idr'] is not None)
        expected = 'NO_SIGNAL' if not ready else (
            'BUY' if r['previous_short_ma'] <= r['previous_long_ma'] and r['moving_average_short'] > r['moving_average_long'] else
            'SELL' if r['previous_short_ma'] >= r['previous_long_ma'] and r['moving_average_short'] < r['moving_average_long'] else 'HOLD')
        assert expected == r['strategy_signal'], 'Formula mismatch'
    stamp = datetime.now(timezone.utc).isoformat()
    out = ROOT / 'docs/hasil_simulasi_batch_streaming.xlsx'
    book = xlsxwriter.Workbook(out, {'strings_to_formulas': False, 'strings_to_urls': False})
    title = book.add_format({'bold': True, 'font_size': 18, 'font_color': '#174C3C'})
    note = book.add_format({'text_wrap': True, 'valign': 'top'})
    pct = book.add_format({'num_format': '0.00%'})
    num = book.add_format({'num_format': '#,##0.0000'})
    colors = {s: book.add_format({'bg_color': c}) for s,c in
              [('BUY','#DEF1DC'),('SELL','#F8DEDC'),('HOLD','#E9ECEF'),('NO_SIGNAL','#FFF0C4')]}
    def scalar(v):
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, Decimal):
            return float(v)
        return v
    def sheet(name, rows, description, columns=None):
        ws = book.add_worksheet(name)
        keys = columns or list(rows[0])
        ws.hide_gridlines(2)
        ws.merge_range(0,0,0,max(3,len(keys)-1),name.replace('_',' '),title)
        ws.merge_range(1,0,2,max(3,len(keys)-1),description,note)
        ws.add_table(4,0,4+len(rows),len(keys)-1,{'style':'Table Style Medium 4',
            'columns':[{'header':k} for k in keys], 'data':[[scalar(r.get(k)) for k in keys] for r in rows]})
        ws.freeze_panes(5,2)
        ws.set_column(0,len(keys)-1,23)
        for i,k in enumerate(keys):
            if 'return' in k and k not in ['return_zscore']:
                ws.set_column(i,i,23,pct)
            elif any(isinstance(r.get(k),(float,Decimal)) for r in rows):
                ws.set_column(i,i,23,num)
            if k in ['keterangan','signal_reason','source','raw_payload']:
                ws.set_column(i,i,55)
            if k in ['strategy_signal','signal_excel']:
                for value,fmt in colors.items():
                    ws.conditional_format(5,i,4+len(rows),i,{'type':'cell','criteria':'==','value':f'"{value}"','format':fmt})
        return ws
    info = [
        {'item':'Sumber', 'keterangan':'Ekspor read-only hasil nyata BigQuery, bukan data harga buatan. Kurs batch dan replay saham dipisahkan.'},
        {'item':'Waktu ekspor UTC','keterangan':stamp},
        {'item':'Cakupan batch','keterangan':f"{len(fetched['Batch_JISDOR'])} tanggal; {fetched['Batch_JISDOR'][0]['jisdor_date']} sampai {fetched['Batch_JISDOR'][-1]['jisdor_date']}"},
        {'item':'Cakupan saham','keterangan':f"{len(mart)} bar; {min(r['minute_utc'] for r in mart)} sampai {max(r['minute_utc'] for r in mart)}"},
        {'item':'Mulai membaca','keterangan':'Ringkasan -> Batch_JISDOR -> Streaming_Replay -> Mart_Hasil -> Hitung_Sinyal -> Latest_Saham.'},
        {'item':'Batch bukan simulasi kurs','keterangan':'Kurs JISDOR asli hasil batch. Simulasi hanya pengiriman ulang saham dan aturan sinyal analitik.'},
        {'item':'Perhitungan Excel','keterangan':'Hitung_Sinyal memakai fitur SMA dari dbt untuk menjelaskan kondisi BUY/SELL/HOLD/NO_SIGNAL dengan formula Excel. Semua hasil dibandingkan kembali dengan dbt sebelum ekspor.'},
        {'item':'SMA','keterangan':'SMA3 dan SMA8 memakai harga close USD dalam sesi. Sinyal perlu window sekarang dan sebelumnya lengkap, menit berurutan, FX tersedia.'},
        {'item':'Batas interpretasi','keterangan':'Tidak ada P&L/backtest/posisi transaksi. Quantity satu saham. Latest boleh semuanya HOLD. Replay bukan harga live.'},
        {'item':'Null dan presisi','keterangan':'Sel kosong = null, bukan nol. Excel menyimpan sekitar 15 digit presisi angka; waktu memakai teks ISO agar zona waktu tidak hilang. Pembulatan tampilan tidak mengubah sumber.'},
        {'item':'Konsistensi','keterangan':'Ekspor tabel dilakukan berurutan, bukan transaksi snapshot tunggal. Event ID dan harga staging dicocokkan dengan mart.'},
    ]
    for name,(relation,_) in tables.items():
        info.append({'item':name,'keterangan':relation+' | query job '+jobs[name]})
    sheet('Panduan',info,'Hasil batch dan simulasi replay dari data asli. Tidak menyertakan fixture sintetis.')
    summary = []
    for symbol in sorted({r['symbol'] for r in mart}):
        rows = [r for r in mart if r['symbol']==symbol]
        count = Counter(r['strategy_signal'] for r in rows)
        summary.append({'symbol':symbol,'jumlah_bar':len(rows),**{k:count[k] for k in colors},
                        'fx_kosong':sum(r['usd_idr'] is None for r in rows),
                        'perbandingan_tidak_lengkap':sum(not r['comparison_complete'] for r in rows)})
    ws = sheet('Ringkasan',summary,'Jumlah sinyal adalah jumlah observasi, bukan jumlah transaksi.')
    chart = book.add_chart({'type':'column','subtype':'stacked'})
    for col,signal in enumerate(colors,2):
        chart.add_series({'name':signal,'categories':['Ringkasan',5,0,8,0], 'values':['Ringkasan',5,col,8,col]})
    chart.set_title({'name':'Distribusi sinyal dari data historis asli'})
    chart.set_y_axis({'name':'Jumlah observasi'})
    ws.insert_chart('A12',chart,{'x_scale':1.4})
    for name,rows in fetched.items():
        sheet(name,rows,'Sumber: '+tables[name][0]+'. Waktu sumber berbeda dari waktu ingestion/replay.')
    columns = ['symbol','minute_utc','price_usd','previous_short_ma','previous_long_ma',
               'moving_average_short','moving_average_long','ma_observation_count','previous_ma_count',
               'consecutive_minute','usd_idr','strategy_signal','signal_excel']
    ws = sheet('Hitung_Sinyal',mart,'Kolom signal_excel berisi rumus aktif; SMA diambil dari hasil dbt. Hasil formula dicache agar terbaca sebelum Excel menghitung ulang.',columns)
    for i,r in enumerate(mart,5):
        n=i+1
        formula=(f'=IF(OR(H{n}<>8,I{n}<>8,J{n}<>TRUE,K{n}=""),"NO_SIGNAL",'
                 f'IF(AND(D{n}<=E{n},F{n}>G{n}),"BUY",IF(AND(D{n}>=E{n},F{n}<G{n}),"SELL","HOLD")))')
        ws.write_formula(i,12,formula,None,r['strategy_signal'])
    book.close()
    with zipfile.ZipFile(out) as z:
        assert z.testzip() is None
        xml=z.read('xl/worksheets/sheet7.xml').decode()
        assert xml.count('<f>') == len(mart)
    print(json.dumps({'status':'passed','file':str(out),'batch_rows':len(fetched['Batch_JISDOR']),
                      'streaming_rows':len(raw),'mart_rows':len(mart),'formula_checks':len(mart),'sheets':7}))


if __name__ == '__main__':
    main()
