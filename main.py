# -*- coding: utf-8 -*-
"""KOSPI300 전 종목 장초 스캔기. 누락 종목도 전수 CSV에 미확인으로 보존."""
import os, re, json, time, math, html, argparse, datetime as dt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
import numpy as np

KST=dt.timezone(dt.timedelta(hours=9)); BASE=Path(__file__).resolve().parent
DOCS=BASE/'docs'; DOCS.mkdir(exist_ok=True)
TOP_N=int(os.getenv('TOP_N','300')); SHOW_N=int(os.getenv('SHOW_N','40'))
DAILY_WORKERS=int(os.getenv('DAILY_WORKERS','12')); MINUTE_WORKERS=int(os.getenv('MINUTE_WORKERS','12'))
TIMEOUT=int(os.getenv('REQUEST_TIMEOUT','12')); TRIES=int(os.getenv('REQUEST_TRIES','2'))
SLOTS=['0910','0920','0930','0950']; TODAY=dt.datetime.now(KST).strftime('%Y%m%d')
UA={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36','Referer':'https://finance.naver.com/'}
WEIGHTS={'liq':.10,'accel':.25,'vol':.15,'momentum':.10,'pullback':.15,'hold':.15,'trend':.10}
LOGS=[]

def log(*a):
 s=' '.join(map(str,a)); print(s,flush=True); LOGS.append(s)
def code6(x):
 if x is None or pd.isna(x): return ''
 s=re.sub(r'\D','',str(x).strip().removesuffix('.0')); return s.zfill(6) if s else ''
def num(x,default=None):
 try:
  if x is None: return default
  s=re.sub(r'[^\d.\-]','',str(x).replace(',',''))
  return float(s) if s not in ('','-','.','-.') else default
 except: return default
def first(d,keys,default=None):
 if isinstance(d,dict):
  for k in keys:
   if d.get(k) not in (None,''): return d[k]
 return default
def get(url,params=None,headers=None):
 h=dict(UA); h.update(headers or {})
 for i in range(TRIES):
  try:
   r=requests.get(url,params=params,headers=h,timeout=TIMEOUT)
   if r.status_code==429 or r.status_code>=500: raise requests.HTTPError(f'HTTP {r.status_code}')
   r.raise_for_status(); return r
  except Exception as e:
   if i==TRIES-1: log('[WARN] GET',url,type(e).__name__,str(e)[:100])
   time.sleep(.5*(i+1))
 return None
def rank(s):
 s=pd.to_numeric(s,errors='coerce')
 return s.rank(pct=True,method='average').fillna(.5) if s.notna().sum()>1 else pd.Series(.5,index=s.index)

# 1) KOSPI 시총순위 상위 300개. 종목코드 끝자리 필터는 사용하지 않음.
def universe():
 found={}; pages=max(5,math.ceil(TOP_N/100)+2)
 for page in range(1,pages+1):
  r=get('https://m.stock.naver.com/api/stocks/marketValue/KOSPI',{'page':page,'pageSize':100})
  if not r: break
  try:
   p=r.json(); arr=p.get('stocks',p.get('items',[])) if isinstance(p,dict) else p
   if not arr: break
   added=0
   for x in arr:
    c=code6(first(x,['itemCode','stockCode','code','symbolCode']))
    n=first(x,['stockName','itemName','name'],c)
    if len(c)==6 and c.isdigit() and c not in found:
     found[c]={'code':c,'name':str(n),'cap_rank':len(found)+1,'universe_source':'Naver mobile'}; added+=1
   log('[INFO] 유니버스 페이지',page,'신규',added,'누적',len(found))
   if len(found)>=TOP_N or added==0: break
  except Exception as e: log('[WARN] 모바일 시총 파싱',str(e)[:100]); break
 if len(found)<TOP_N:
  for page in range(1,31):
   r=get('https://finance.naver.com/sise/sise_market_sum.naver',{'sosok':0,'page':page})
   if not r: continue
   r.encoding='euc-kr'; matches=re.findall(r'code=([0-9]{6})" class="tltle">([^<]+)<',r.text)
   for c,n in matches:
    c=code6(c)
    if c not in found: found[c]={'code':c,'name':html.unescape(n.strip()),'cap_rank':len(found)+1,'universe_source':'Naver PC fallback'}
   if len(found)>=TOP_N: break
 out=list(found.values())[:TOP_N]
 for i,x in enumerate(out,1): x['cap_rank']=i
 log(f'[INFO] 최종 유니버스 {len(out)}/{TOP_N}')
 return out

# 2) 실시간 시세 300개 전체

def quotes(codes):
 chunks=[codes[i:i+25] for i in range(0,len(codes),25)]
 def worker(chunk):
  r=get('https://polling.finance.naver.com/api/realtime/domestic/stock/'+','.join(chunk))
  if not r:return {}
  try:
   arr=r.json().get('datas',[]); out={}
   for i,d in enumerate(arr):
    c=code6(first(d,['itemCode','symbolCode','stockCode','code']))
    if not c and len(arr)==len(chunk): c=chunk[i]
    if c: out[c]=d
   return out
  except Exception as e: log('[WARN] 시세 JSON',str(e)[:100]); return {}
 result={}
 with ThreadPoolExecutor(max_workers=6) as ex:
  for part in ex.map(worker,chunks): result.update(part)
 log(f'[INFO] 실시간 시세 {len(result)}/{len(codes)}'); return result

def amount_fix(raw,vol,price):
 if not raw or not vol or not price:return (price*vol if price and vol else np.nan),None,'price_x_volume_estimate'
 target=price*vol; factors=[1,10,100,1000,10000,100000,1000000,100000000]
 candidates=[(raw*f,f) for f in factors]
 amt,f=min(candidates,key=lambda z:abs(math.log(max(z[0],1)/target)))
 ratio=amt/target if target else 0
 if not .35<=ratio<=2.8:return target,None,'price_x_volume_estimate'
 return amt,f,'naver_amount_scaled'

def parse_quote(c,name,d,cap_rank):
 p=num(first(d,['closePrice','nowPrice','currentPrice','tradePrice'])); op=num(first(d,['openPrice','open']))
 hi=num(first(d,['highPrice','high'])); lo=num(first(d,['lowPrice','low'])); vol=num(first(d,['accumulatedTradingVolume','tradeVolume','volume']),0)
 raw=num(first(d,['accumulatedTradingValue','accumulatedTradingAmount','tradeValue']),0)
 diff=num(first(d,['compareToPreviousClosePrice','compareToPreviousPrice','changePrice']))
 if None in (p,op,hi,lo):return None
 prev=p-diff if diff is not None else num(first(d,['previousClose','prevClosePrice']))
 if not prev or prev<=0:return None
 amt,factor,source=amount_fix(raw,vol,p); vwap=amt/vol if pd.notna(amt) and vol else np.nan
 return {'code':c,'name':name,'cap_rank':cap_rank,'quote_status':'OK','price':p,'prev':prev,'open':op,'high':hi,'low':lo,'volume':vol,'amount_raw':raw,'amount_won':amt,'amount_eok':amt/1e8 if pd.notna(amt) else np.nan,'amount_factor':factor,'amount_source':source,'change_pct':(p/prev-1)*100,'gap_pct':(op/prev-1)*100,'high_pct':(hi/prev-1)*100,'from_high_pct':(p/hi-1)*100 if hi else np.nan,'vwap':vwap,'vwap_pct':(p/vwap-1)*100 if pd.notna(vwap) and vwap>0 else np.nan}

# 3) 일봉 전수 300개
def daily(c):
 r=get(f'https://m.stock.naver.com/api/stock/{c}/price',{'pageSize':35,'page':1})
 if not r:return c,None
 try:
  arr=r.json(); rows=[]
  if not isinstance(arr,list):return c,None
  for x in arr:
   date=str(x.get('localTradedAt','')).replace('-','')[:8]; close=num(x.get('closePrice')); vol=num(x.get('accumulatedTradingVolume'))
   if date and date<TODAY and close and close>0:rows.append((date,close,vol))
  rows.sort(); closes=[x[1] for x in rows if x[1] is not None]; vols=[x[2] for x in rows if x[2] is not None]
  if len(closes)<6:return c,None
  n=min(20,len(closes))
  return c,{'avg_vol20':sum(vols[-n:])/n if len(vols)>=n else np.nan,'ret5_pct':(closes[-1]/closes[-6]-1)*100,'ma20':sum(closes[-n:])/n,'daily_status':'OK'}
 except Exception as e:log('[WARN] 일봉',c,str(e)[:80]);return c,None
def all_daily(codes):
 out={}
 with ThreadPoolExecutor(max_workers=DAILY_WORKERS) as ex:
  fs={ex.submit(daily,c):c for c in codes}
  for f in as_completed(fs):
   c=fs[f]
   try:k,v=f.result();out[k]=v
   except Exception as e:log('[WARN] 일봉 작업',c,str(e)[:80]);out[c]=None
 log('[INFO] 일봉 전수',sum(v is not None for v in out.values()),'/',len(codes));return out

# 4) 1분봉 전수 300개 시도
def minutes(c):
 r=get('https://fchart.stock.naver.com/sise.nhn',{'symbol':c,'timeframe':'minute','count':240,'requestType':0})
 if not r:return c,None
 try:
  raw=re.findall(r'data="(\d{12,14})\|([\d.]+)\|([\d.]+)\|([\d.]+)\|([\d.]+)\|(\d+)"',r.text); bars=[]
  for z in raw:
   if z[0][:8]==TODAY:bars.append({'ts':z[0],'open':float(z[1]),'high':float(z[2]),'low':float(z[3]),'close':float(z[4]),'volume':float(z[5])})
  bars.sort(key=lambda x:x['ts'])
  if len(bars)<3:return c,None
  groups={}
  for b in bars:
   minute=int(b['ts'][10:12]); key=b['ts'][:10]+f'{minute//5*5:02d}';groups.setdefault(key,[]).append(b)
  five=[]
  for k,a in sorted(groups.items()):five.append({'time':k[-4:-2]+':'+k[-2:],'open':a[0]['open'],'high':max(x['high'] for x in a),'low':min(x['low'] for x in a),'close':a[-1]['close'],'volume':sum(x['volume'] for x in a)})
  total=sum(x['volume'] for x in bars); mvwap=sum(((x['high']+x['low']+x['close'])/3)*x['volume'] for x in bars)/total if total else np.nan
  ret=(five[-1]['close']/five[-2]['close']-1)*100 if len(five)>1 and five[-2]['close'] else np.nan
  return c,{'bars5':five,'last5':five[-1],'prev5':five[-2] if len(five)>1 else None,'bars_count':len(bars),'minute_vwap':mvwap,'minute_return_5m':ret}
 except Exception as e:log('[WARN] 분봉 파싱',c,str(e)[:80]);return c,None
def all_minutes(codes):
 out={}
 with ThreadPoolExecutor(max_workers=MINUTE_WORKERS) as ex:
  fs={ex.submit(minutes,c):c for c in codes}
  for f in as_completed(fs):
   c=fs[f]
   try:k,v=f.result();out[k]=v
   except Exception as e:log('[WARN] 분봉 작업',c,str(e)[:80]);out[c]=None
 log('[INFO] 분봉 전수',sum(v is not None for v in out.values()),'/',len(codes));return out

# 5) 스냅샷 비교
def prev_slot(slot):
 if slot in {'0920','0930','0950'}:return {'0920':'0910','0930':'0920','0950':'0930'}[slot]
 t=dt.datetime.now(KST).time()
 return '0930' if t>=dt.time(9,50) else '0920' if t>=dt.time(9,30) else '0910' if t>=dt.time(9,20) else None
def load_previous(slot):
 ps=prev_slot(slot)
 if not ps:return None,None
 p=DOCS/f'snap_{ps}.json'
 if not p.exists():return None,ps
 try:
  obj=json.loads(p.read_text(encoding='utf-8'))
  if obj.get('date')!=TODAY:return None,ps
  old=pd.DataFrame(obj.get('rows',[]))
  if old.empty:return None,ps
  old['code']=old.code.map(code6);return old.drop_duplicates('code').set_index('code'),ps
 except Exception as e:log('[WARN] 이전 스냅샷',str(e)[:80]);return None,ps
def save_snapshot(slot,df):
 ordered=df.sort_values('amount_eok',ascending=False);ranks={c:i+1 for i,c in enumerate(ordered.index)};rows=[]
 for c,r in df.iterrows():
  rows.append({'code':c,'amount_eok':float(r.amount_eok) if pd.notna(r.amount_eok) else None,'change_pct':float(r.change_pct) if pd.notna(r.change_pct) else None,'amount_rank':ranks.get(c),'price':float(r.price) if pd.notna(r.price) else None})
 (DOCS/f'snap_{slot}.json').write_text(json.dumps({'date':TODAY,'time':dt.datetime.now(KST).strftime('%H:%M:%S'),'slot':slot,'rows':rows},ensure_ascii=False,indent=2),encoding='utf-8')

# 6) 매크로/뉴스
def macro_data():
 out={}
 try:
  import yfinance as yf
  tickers={'S&P500':'^GSPC','NASDAQ':'^IXIC','SOX':'^SOX','S&P선물':'ES=F','NASDAQ선물':'NQ=F','원달러':'KRW=X','WTI':'CL=F','미10년':'^TNX','VIX':'^VIX'}
  for label,ticker in tickers.items():
   try:
    h=yf.Ticker(ticker).history(period='5d',interval='1d');out[label]=round((float(h.Close.iloc[-1])/float(h.Close.iloc[-2])-1)*100,2) if len(h)>=2 else None
   except:out[label]=None
 except Exception as e:log('[WARN] yfinance',str(e)[:80])
 return out
def news(name):
 cid=os.getenv('NAVER_ID');secret=os.getenv('NAVER_SECRET')
 if not cid or not secret:return []
 r=get('https://openapi.naver.com/v1/search/news.json',{'query':name,'display':3,'sort':'date'},{'X-Naver-Client-Id':cid,'X-Naver-Client-Secret':secret})
 if not r:return []
 try:return [re.sub(r'<[^>]+>','',html.unescape(x.get('title',''))) for x in r.json().get('items',[])[:3]]
 except:return []

# 7) 전 종목 스코어링
def score_all(qdf,dmap,mmap,old,ps):
 d=qdf.copy();d['avg_vol20']=[(dmap.get(c) or {}).get('avg_vol20',np.nan) for c in d.index];d['ret5_pct']=[(dmap.get(c) or {}).get('ret5_pct',np.nan) for c in d.index];d['ma20']=[(dmap.get(c) or {}).get('ma20',np.nan) for c in d.index]
 d['daily_status']=['OK' if dmap.get(c) else '미확인' for c in d.index]
 d['vol_ratio']=np.where(d.avg_vol20>0,d.volume/d.avg_vol20,np.nan);d['vwap_pct']=np.where(d.vwap>0,(d.price/d.vwap-1)*100,np.nan)
 d['hold']=((d.price>=d.open).astype(float)+(d.vwap_pct>=0).where(d.vwap_pct.notna(),False).astype(float))/2
 d['liq']=rank(d.amount_eok);d['vol_score']=rank(d.vol_ratio);d['momentum']=(d.change_pct.clip(0,15)/15).fillna(0)
 def pb(x):
  if pd.isna(x):return .45
  if -5<=x<=-1:return 1.
  if -8<=x< -5:return .65
  if -1<x<=0:return .55
  if x< -8:return .2
  return .35
 d['pullback']=d.from_high_pct.apply(pb);d['trend']=np.where(d.ma20.notna(),(d.price>=d.ma20).astype(float),.5)
 d['delta_amt_eok']=np.nan;d['delta_change_pp']=np.nan;d['rank_improve']=np.nan
 if old is not None:
  common=d.index.intersection(old.index)
  for col,target in [('amount_eok','delta_amt_eok'),('change_pct','delta_change_pp')]:
   if col in old:d.loc[common,target]=d.loc[common,col]-pd.to_numeric(old.loc[common,col],errors='coerce')
  if 'amount_rank' in old:d.loc[common,'rank_improve']=pd.to_numeric(old.loc[common,'amount_rank'],errors='coerce')-d.loc[common,'amount_rank']
 d['accel']=rank(d.delta_amt_eok.clip(lower=0))*.7+rank(d.rank_improve.clip(lower=0))*.3
 d['minute_data']=[mmap.get(c) for c in d.index];d['minute_status']=['OK' if mmap.get(c) else '미확인' for c in d.index]
 d['minute_vwap']=[(mmap.get(c) or {}).get('minute_vwap',np.nan) for c in d.index];d['minute_return_5m']=[(mmap.get(c) or {}).get('minute_return_5m',np.nan) for c in d.index]
 d['score']=(100*(WEIGHTS['liq']*d.liq+WEIGHTS['accel']*d.accel+WEIGHTS['vol']*d.vol_score+WEIGHTS['momentum']*d.momentum+WEIGHTS['pullback']*d.pullback+WEIGHTS['hold']*d.hold+WEIGHTS['trend']*d.trend)).round(1)
 d['minute_signal']=0.
 d['pattern']='관찰';d['entry_zone']=np.nan;d['stop_price']=np.nan;d['target1']=np.nan;d['target2']=np.nan
 for c,r in d.iterrows():
  m=r.minute_data
  if not m:
   if r.change_pct>=8 and r.from_high_pct> -1:d.loc[c,'pattern']='고점추격주의(분봉미확인)'
   continue
  b=m['last5'];p=m.get('prev5');rising=bool(p and b['close']>=p['close'] and b['low']>=p['low']);vwapok=bool(pd.notna(r.vwap) and r.price>=r.vwap);green=b['close']>=b['open']
  if rising and pd.notna(r.minute_vwap) and r.price>=r.minute_vwap:d.loc[c,'minute_signal']=1.
  elif pd.notna(r.minute_return_5m) and r.minute_return_5m>0:d.loc[c,'minute_signal']=.5
  if r.change_pct>=3 and pd.notna(r.delta_amt_eok) and r.delta_amt_eok>0 and vwapok and rising:pat='EARLY/BREAKOUT'
  elif r.change_pct>=2 and -5<=r.from_high_pct<=-1 and vwapok and r.price>=r.open and green:pat='PULLBACK'
  elif r.price<r.open and (pd.isna(r.vwap) or r.price<r.vwap):pat='약세/매도압력'
  elif r.change_pct>=8 and r.from_high_pct> -1:pat='고점추격주의'
  elif rising and vwapok:pat='분봉재상승관찰'
  else:pat='관찰'
  d.loc[c,'pattern']=pat
  support=max([b['low'],r.vwap] if pd.notna(r.vwap) and r.vwap>0 else [b['low']])
  if r.price>=support and r.price>0:
   d.loc[c,'entry_zone']=round(support);d.loc[c,'stop_price']=round(min(support*.97,r.price*.97));d.loc[c,'target1']=round(support*1.04);d.loc[c,'target2']=round(support*1.07)
 d['score']=(d.score+d.minute_signal*3).clip(0,100).round(1)
 d['data_quality_pct']=100;d.loc[d.daily_status!='OK','data_quality_pct']-=20;d.loc[d.minute_status!='OK','data_quality_pct']-=25;d.loc[d.vwap.isna(),'data_quality_pct']-=15;d.loc[d.delta_amt_eok.isna(),'data_quality_pct']-=10;d['amount_rank']=d.amount_eok.rank(ascending=False,method='min');d['previous_slot']=ps or '없음'
 return d

# 8) 출력: 전수 universe CSV와 유효 시세 분석 CSV 모두 생성
def write_outputs(slot,uni,all_rows,qdf,scored,dmap,mmap,macro,newsmap,ps):
 stamp=dt.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST');n=len(uni);valid=len(qdf);ds=sum(v is not None for v in dmap.values());ms=sum(v is not None for v in mmap.values())
 adv=int((qdf.change_pct>0).sum());dec=int((qdf.change_pct<0).sum());flat=int((qdf.change_pct==0).sum())
 order={'PULLBACK':0,'EARLY/BREAKOUT':1,'분봉재상승관찰':2,'관찰':3,'고점추격주의':4,'고점추격주의(분봉미확인)':5,'약세/매도압력':6}
 s=scored.copy();s['pat_rank']=s.pattern.map(order).fillna(9);s=s.sort_values(['pat_rank','score','amount_eok'],ascending=[True,False,False]);top=s.head(SHOW_N)
 lines=[f'# KOSPI 시총 상위 {TOP_N} 전수 스캔 ({slot})',f'- 생성: {stamp}',f'- 유니버스: {n}/{TOP_N}',f'- 실시간 시세: {valid}/{n} (누락 {n-valid})',f'- 일봉 정상: {ds}/{n}',f'- 1분봉 정상: {ms}/{n}',f'- 직전 스냅샷: {ps or "없음"}',f'- 시장 breadth: 상승 {adv} / 하락 {dec} / 보합 {flat}',f'- 매크로 일봉 변화율(실시간 장중 수익률 아님): {json.dumps(macro,ensure_ascii=False)}','', '## 우선 후보','|순위|시총순위|종목|현재가|등락%|거래대금(억)|대금순위|대금증가(억)|등락변화(%p)|VWAP대비%|고점대비%|패턴|점수|품질|진입|손절|목표1|목표2|뉴스|','|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|']
 def f(x,d=1):return '미확인' if pd.isna(x) else f'{x:+.{d}f}'
 def iv(x):return '미확인' if pd.isna(x) else str(int(round(x)))
 for i,(c,r) in enumerate(top.iterrows(),1):
  lines.append(f"|{i}|{int(r.cap_rank)}|{r['name']}({c})|{r.price:,.0f}|{r.change_pct:+.2f}|{r.amount_eok:,.1f}|{r.amount_rank:.0f}|{f(r.delta_amt_eok)}|{f(r.delta_change_pp,2)}|{f(r.vwap_pct,2)}|{f(r.from_high_pct,2)}|{r.pattern}|{r.score:.1f}|{r.data_quality_pct:.0f}|{iv(r.entry_zone)}|{iv(r.stop_price)}|{iv(r.target1)}|{iv(r.target2)}|{html.escape(' / '.join(newsmap.get(c,[])))}|")
 lines+=['','## 눌림목 후보'];pull=s[s.pattern=='PULLBACK'].head(10)
 if pull.empty:lines.append('조건을 충족한 눌림목 후보 없음. 억지 선정하지 않음.')
 else:
  for c,r in pull.iterrows():lines.append(f"- **{r['name']}({c})**: 현재 {r.price:,.0f}원, 고점대비 {r.from_high_pct:.2f}%, VWAP대비 {f(r.vwap_pct,2)}%, 진입참고 {iv(r.entry_zone)}, 손절참고 {iv(r.stop_price)}")
 lines+=['','## 수집 범위 및 한계',f'- 시총 상위 {n}개 전체를 실시간 시세/일봉/1분봉 조회 대상으로 사용. 수집 실패 종목은 전수 CSV에 미확인으로 유지.','- 외국인/기관/프로그램 수급, KOSPI200 선물/베이시스, NXT 데이터는 수집하지 않아 미확인.','- VWAP은 누적 거래대금/누적 거래량 기반 근사치이며 체결별 실제 VWAP이 아님.','- 점수는 상대평가이며 상승확률이 아님. 진입/손절/목표는 참고값.']
 md='\n'.join(lines);(DOCS/'index.md').write_text(md,encoding='utf-8');(DOCS/'index.html').write_text('<!doctype html><html lang="ko"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>KOSPI300</title><pre>'+html.escape(md)+'</pre></html>',encoding='utf-8')
 all_rows.reset_index().to_csv(DOCS/f'universe_{slot}.csv',index=False,encoding='utf-8-sig');s.reset_index().to_csv(DOCS/f'scan_{slot}.csv',index=False,encoding='utf-8-sig')
 (DOCS/f'diagnostics_{slot}.txt').write_text(f'Universe={n}\nQuotes={valid}\nQuote missing={n-valid}\nDaily={ds}/{n}\nMinute={ms}/{n}\nPrevious={ps}\n',encoding='utf-8')
 log(f'[DONE] {slot}: universe={n}, quotes={valid}, daily={ds}, minute={ms}, candidates={len(top)}')

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--slot',default=os.getenv('SLOT','now'),choices=SLOTS+['now']);slot=ap.parse_args().slot
 log('=== KOSPI300 전수 스캔',slot,dt.datetime.now(KST).isoformat())
 uni=universe()
 if not uni:log('[ERROR] 유니버스 수집 실패');return 1
 codes=[x['code'] for x in uni];qmap=quotes(codes);name={x['code']:x['name'] for x in uni};cap={x['code']:x['cap_rank'] for x in uni}
 qrows=[]
 for c,d in qmap.items():
  if c in name:
   x=parse_quote(c,name[c],d,cap[c])
   if x:qrows.append(x)
 qdf=pd.DataFrame(qrows)
 if qdf.empty or len(qdf)<30:log('[ERROR] 유효 시세 부족',len(qdf));return 1
 qdf=qdf.drop_duplicates('code').set_index('code');qdf['amount_rank']=qdf.amount_eok.rank(ascending=False,method='min')
 log(f'[INFO] 실시간 시세 유효 {len(qdf)}/{len(uni)}')
 # 반드시 전체 유니버스 코드로 일봉과 분봉을 모두 호출한다.
 dmap=all_daily(codes);mmap=all_minutes(codes);old,ps=load_previous(slot)
 scored=score_all(qdf,dmap,mmap,old,ps)
 newsmap={c:news(r['name']) for c,r in scored.sort_values('score',ascending=False).head(20).iterrows()}
 macro=macro_data();save_snapshot(slot,qdf)
 # 전수 유니버스 왼쪽 기준: 실시간 시세가 없는 종목도 행을 보존
 base=pd.DataFrame(uni).set_index('code');all_rows=base.join(qdf.drop(columns=['name','cap_rank','quote_status'],errors='ignore'),how='left')
 all_rows['quote_status']=np.where(all_rows['price'].notna(),'OK','미수집')
 all_rows['daily_status']=[('OK' if dmap.get(c) else '미확인') for c in all_rows.index]
 all_rows['minute_status']=[('OK' if mmap.get(c) else '미확인') for c in all_rows.index]
 # 점수/패턴은 유효 시세 종목 전부 포함. 미수집 종목은 universe CSV에 별도 보존.
 write_outputs(slot,uni,all_rows,qdf,scored,dmap,mmap,macro,newsmap,ps)
 return 0

if __name__=='__main__':raise SystemExit(main())
