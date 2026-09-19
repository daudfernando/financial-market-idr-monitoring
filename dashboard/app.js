'use strict';
const D = window.PROJECT_DATA;
const $ = id => document.getElementById(id);
const num = (v, digits=0) => v == null ? 'Tidak tersedia' : Number(v).toLocaleString('id-ID', {maximumFractionDigits:digits});
const idr = v => v == null ? 'Tidak tersedia' : 'Rp ' + num(v);
if (!D || !D.rows.length) { document.body.textContent = 'Snapshot belum tersedia. Jalankan scripts/export_dashboard.py.'; }
else {
  const symbols = [...new Set(D.rows.map(r => r.symbol))];
  for (const s of symbols) $('symbol').add(new Option(s,s));
  function sessions() {
    $('session').replaceChildren(new Option('Semua sesi','all'));
    [...new Set(D.rows.filter(r=>r.symbol===$('symbol').value).map(r=>r.session_date))].sort().forEach(s=>$('session').add(new Option(s,s)));
  }
  function chart(id, rows, field, format, step=false) {
    const c=$(id), ratio=window.devicePixelRatio||1, width=c.clientWidth, height=250;
    c.width=width*ratio;c.height=height*ratio;
    const ctx=c.getContext('2d');ctx.scale(ratio,ratio);
    const values=rows.map(r=>r[field]).filter(v=>v!=null);
    if(!values.length){ctx.fillText('Kurs belum tersedia',30,50);return;}
    let lo=Math.min(...values),hi=Math.max(...values);const pad=(hi-lo)*.12||hi*.005||1;lo-=pad;hi+=pad;
    const left=80,right=width-12,top=15,bottom=215;
    const x=i=>left+(right-left)*i/Math.max(1,rows.length-1),y=v=>bottom-(v-lo)/(hi-lo)*(bottom-top);
    ctx.font='11px Segoe UI';ctx.lineWidth=1;
    for(let i=0;i<4;i++){const v=lo+(hi-lo)*i/3;ctx.strokeStyle='#e6ebe1';ctx.beginPath();ctx.moveTo(left,y(v));ctx.lineTo(right,y(v));ctx.stroke();ctx.fillStyle='#6b7c70';ctx.fillText(num(v),3,y(v)+3);}
    ctx.strokeStyle=step?'#869939':'#217458';ctx.lineWidth=2;ctx.beginPath();let prev=null;
    rows.forEach((r,i)=>{const v=r[field];if(v==null){prev=null;return;}if(prev==null)ctx.moveTo(x(i),y(v));else{if(step)ctx.lineTo(x(i),y(prev));ctx.lineTo(x(i),y(v));}prev=v;});ctx.stroke();
    ctx.fillStyle='#6b7c70';ctx.fillText('Awal pilihan',left,240);ctx.textAlign='right';ctx.fillText('Akhir pilihan',right,240);ctx.textAlign='left';
    c.onmousemove=e=>{const i=Math.max(0,Math.min(rows.length-1,Math.round((e.offsetX-left)/(right-left)*(rows.length-1))));const r=rows[i];$(id+'Tip').textContent=r.event_timestamp+' · '+format(r[field])+' · FX '+r.jisdor_date;};
  }
  function render(){
    const rows=D.rows.filter(r=>r.symbol===$('symbol').value&&($('session').value==='all'||r.session_date===$('session').value));
    if(!rows.length)return;
    const first=rows[0],last=rows[rows.length-1],review=rows.filter(r=>r.risk_indicator==='review_movement').length, insufficient=rows.filter(r=>r.risk_indicator==='insufficient_data').length;
    $('metrics').replaceChildren();
    for(const [label,value,note] of [['Nilai per saham terakhir',idr(last.exposure_idr),'Valuasi indikatif, bukan portofolio'],['Harga USD terakhir','$ '+num(last.price_usd,2),last.symbol],['Observasi',num(rows.length),'Bar satu menit · replay'],['FX tidak tersedia',num(rows.filter(r=>r.usd_idr==null).length),'Tidak diisi dengan kurs masa depan']]){
      const box=document.createElement('div');box.className='metric';const title=document.createElement('span'),strong=document.createElement('strong'),small=document.createElement('small');title.textContent=label;strong.textContent=value;small.textContent=note;box.append(title,strong,small);$('metrics').append(box);
    }
    $('period').textContent=first.session_date+' → '+last.session_date+' · sumbu grafik mengikuti urutan observasi';
    $('signalCounts').replaceChildren();
    for(const signal of ['BUY','SELL','HOLD','NO_SIGNAL']){const span=document.createElement('span');span.className='badge';span.textContent=signal+': '+rows.filter(r=>r.strategy_signal===signal).length;$('signalCounts').append(span);}
    $('comparison').replaceChildren();
    for(const symbol of symbols){
      const series=D.rows.filter(r=>r.symbol===symbol&&($('session').value==='all'||r.session_date===$('session').value));
      if(!series.length)continue;const r=series[series.length-1],tr=document.createElement('tr');
      [r.symbol,r.minute_utc,r.strategy_signal,num(r.session_return*100,3)+'%',r.peer_average_return==null?'Tidak tersedia':num(r.peer_average_return*100,3)+'%',r.peer_rank??'—',r.signal_reason].forEach(v=>{const td=document.createElement('td');td.textContent=v;tr.append(td);});$('comparison').append(tr);
    }
    chart('price',rows,'exposure_idr',idr);chart('fx',rows,'usd_idr',v=>'Rp '+num(v),true);
    const ok=first.usd_idr!=null&&last.usd_idr!=null;
    const effects=ok?[(last.price_usd-first.price_usd)*first.usd_idr,first.price_usd*(last.usd_idr-first.usd_idr),(last.price_usd-first.price_usd)*(last.usd_idr-first.usd_idr)]:[null,null,null];
    $('effects').replaceChildren();['Efek harga saham','Efek perubahan kurs','Efek interaksi'].forEach((label,i)=>{const row=document.createElement('div');row.className='effect';const text=document.createElement('span'),value=document.createElement('b');text.textContent=label;value.textContent=idr(effects[i]);row.append(text,value);$('effects').append(row);});
    $('equation').textContent='Total perubahan: '+idr(ok?last.exposure_idr-first.exposure_idr:null)+' · quantity tetap 1 saham.';
    $('risk').textContent=review+' observasi untuk ditinjau · '+insufficient+' baseline belum cukup';
    $('rows').replaceChildren();rows.slice(-8).reverse().forEach(r=>{const tr=document.createElement('tr');[r.event_timestamp,num(r.price_usd,2),r.jisdor_date,num(r.fx_age_calendar_days),r.fx_join_policy,r.risk_indicator].forEach(v=>{const td=document.createElement('td');td.textContent=v??'Tidak tersedia';tr.append(td);});$('rows').append(tr);});
    $('provenance').textContent='Snapshot: '+D.exported_at+' | Sumber: '+D.source+'. Waktu publikasi FX belum terverifikasi; prior_day_assumption menggunakan tanggal FX sebelum tanggal event WIB. Data historis dapat mengandung revisi sumber; belum merupakan arsip point-in-time. Threshold bukan rekomendasi investasi.';
  }
  sessions();render();$('symbol').onchange=()=>{sessions();render();};$('session').onchange=render;window.addEventListener('resize',render);
}
