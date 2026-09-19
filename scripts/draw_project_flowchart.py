"""Render a simple project status flowchart from the verified 12 Sep 2026 log."""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
SCALE = 2
im = Image.new('RGB', (1600 * SCALE, 1330 * SCALE), '#f8fafc')
d = ImageDraw.Draw(im)
COLORS = {'done': ('#e7f5ec', '#27834b'), 'partial': ('#fff4da', '#b77914'),
          'planned': ('#eef1f5', '#8290a1')}


def font(size, bold=False):
    return ImageFont.truetype('C:/Windows/Fonts/' + ('arialbd.ttf' if bold else 'arial.ttf'), size*SCALE)


def text(x, y, value, size=23, fill='#203047', bold=False, center=True):
    d.text((x*SCALE, y*SCALE), value, font=font(size, bold), fill=fill,
           anchor='mt' if center else 'lt')


def box(x, y, w, h, title, lines, status):
    bg, border = COLORS[status]
    d.rounded_rectangle((x*SCALE,y*SCALE,(x+w)*SCALE,(y+h)*SCALE), radius=16*SCALE,
                        fill=bg, outline=border, width=2*SCALE)
    text(x+w/2,y+17,title,26,bold=True)
    for i, line in enumerate(lines):
        text(x+w/2,y+55+i*27,line,21)


def arrow(points, planned=False):
    color = '#9aa5b1' if planned else '#27834b'
    p = [(int(x*SCALE),int(y*SCALE)) for x,y in points]
    d.line(p,fill=color,width=3*SCALE,joint='curve')
    x,y=points[-1]
    d.polygon([(x*SCALE,y*SCALE),((x-7)*SCALE,(y-11)*SCALE),((x+7)*SCALE,(y-11)*SCALE)],fill=color)


text(800,32,'FLOWCHART FINAL PROJECT',38,bold=True)
text(800,82,'Financial Market Risk & IDR Exposure Monitoring',25)
for x,status,label in [(375,'done','Sudah berhasil diuji'),(800,'partial','Sebagian / sampel tersedia'),(1230,'planned','Belum dikerjakan')]:
    bg,border=COLORS[status]
    d.ellipse(((x-150)*SCALE,133*SCALE,(x-132)*SCALE,151*SCALE),fill=border)
    text(x-120,130,label,21,center=False)
text(430,188,'JALUR BATCH',26,bold=True)
text(1170,188,'JALUR STREAMING',26,bold=True)

box(100,232,660,110,'Bank Indonesia JISDOR',
    ['Kurs USD/IDR harian', 'Respons sumber berhasil diambil'], 'done')
box(840,232,660,110,'Harga saham melalui yfinance',
    ['Target: JPM, BAC, GS, MS', '1.949 bar historis JPM tersedia; live belum valid'], 'partial')
arrow([(430,342),(430,375)])
arrow([(1170,342),(1170,375)],True)
box(100,375,660,110,'Airflow + Python',
    ['Ambil dan validasi JISDOR', 'Run manual sukses; jadwal 18.00 WIB aktif'], 'done')
box(840,375,660,110,'Producer -> Pub/Sub -> Consumer',
    ['Kirim dan terima event harga', 'Live atau controlled replay yang diberi label'], 'planned')
arrow([(430,485),(430,518)])
arrow([(1170,485),(1170,518)],True)
box(100,518,660,110,'GCS: data lake JISDOR',
    ['Raw XML + record harian + manifest', 'Upload dan rerun tanpa overwrite teruji'], 'done')
box(840,518,660,110,'GCS: data lake saham',
    ['Simpan event mentah dan data hasil pengolahan', 'Jalur streaming ke GCS belum dibuat'], 'planned')
arrow([(430,628),(430,677)])
arrow([(1170,628),(1170,677)],True)
d.rectangle((210*SCALE,635*SCALE,650*SCALE,660*SCALE), fill='#f8fafc')
text(430,638,'Saat ini Python; PySpark belum ditambahkan',18)
box(100,677,660,110,'BigQuery: fact_jisdor_daily',
    ['16 tanggal unik sampai 11 September 2026', 'MERGE + quality check lulus; tanpa duplikasi'], 'done')
box(840,677,660,110,'BigQuery: data harga saham',
    ['Normalisasi harga, waktu, simbol, dan mata uang', 'Pemuatan data saham belum dibuat'], 'planned')
text(430,802,'POSISI SAAT INI: batch sampai warehouse selesai',21,fill='#217342',bold=True)

arrow([(430,840),(430,863),(800,863),(800,896)],True)
arrow([(1170,787),(1170,863),(800,863),(800,896)],True)
box(390,896,820,112,'dbt + analytical mart',
    ['Join harga saham dengan JISDOR yang sudah tersedia',
     'Nilai IDR, kontribusi aset/FX, volatilitas, indikator risiko'], 'planned')
arrow([(800,1008),(800,1043)],True)
box(390,1043,820,98,'Dashboard Looker Studio',
    ['Minimal 2 chart untuk monitoring nilai rupiah dan risiko'], 'planned')
box(100,1172,1400,87,'Monitoring yang sudah diuji',
    ['Task gagal -> callback -> email Mailpit lokal berhasil | 10 unit test lulus'], 'done')
text(800,1280,'Status berdasarkan pengujian 12 September 2026. Jadwal berjalan selama stack aktif; email eksternal belum disiapkan.',19)
target=ROOT/'docs'/'project_flowchart.png'
im.save(target)
print(target)
