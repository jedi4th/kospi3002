# -*- coding: utf-8 -*-
"""
KOSPI300 장초 스냅샷 분석기
- KOSPI 시가총액 상위 보통주 최대 300개
- Naver Finance 비공식 공개 엔드포인트 + yfinance
- 09:10 / 09:20 / 09:30 / 09:50 스냅샷 비교
- 09:50 눌림목 후보를 별도 산출
주의: 네이버 비공식 API는 사전 고지 없이 변경/차단될 수 있음.
      네이버에서 제공하지 않는 외국인·기관·프로그램·선물 수급은 미확인으로 표시.
"""
import os, re, json, time, html, math, argparse, datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
import pandas as pd

KST = dt.timezone(dt.timedelta(hours=9))
BASE = Path(__file__).resolve().parent
DOCS = BASE / "docs"
DOCS.mkdir(exist_ok=True)
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
      "Referer": "https://finance.naver.com/"}
TOP_N = int(os.getenv("TOP_N", "300"))
SHOW_N = int(os.getenv("SHOW_N", "40"))
SLOTS = ["0910", "0920", "0930", "0950"]
WEIGHTS = {"liq": .10, "accel": .25, "vol": .15, "momentum": .10,
           "pullback": .15, "hold": .15, "trend": .10}
TODAY = dt.datetime.now(KST).strftime("%Y%m%d")
STAMP = lambda: dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
SESSION = requests.Session()
SESSION.headers.update(UA)
LOGS = []

def log(*args):
    s = " ".join(map(str, args))
    print(s, flush=True)
    LOGS.append(s)

def get(url, params=None, headers=None, timeout=12, tries=2):
    for attempt in range(tries):
        try:
            r = SESSION.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == tries - 1:
                log("[WARN] GET", url, type(e).__name__, str(e)[:160])
            time.sleep(.5 * (attempt + 1))
    return None

def number(x, default=None):
    if x is None: return default
    try:
        s = re.sub(r"[^\d.\-]", "", str(x).replace(",", ""))
        return float(s) if s not in ("", "-", ".", "-.") else default
    except Exception:
        return default

def first(d, keys, default=None):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default

def naver_universe():
    """네이버 모바일 시총 API 우선, PC 시총 페이지를 보조로 사용."""
    found = {}
    for page in range(1, 8):
        r = get("https://m.stock.naver.com/api/stocks/marketValue/KOSPI",
                {"page": page, "pageSize": 100})
        if not r: break
        try:
            payload = r.json()
            arr = payload.get("stocks", []) if isinstance(payload, dict) else payload
            if not arr: break
            for x in arr:
                code = str(first(x, ["itemCode", "stockCode", "code"], "")).zfill(6)
                name = first(x, ["stockName", "itemName", "name"], code)
                if len(code) == 6 and code.isdigit() and code.endswith("0"):
                    found.setdefault(code, str(name))
        except Exception as e:
            log("[WARN] 모바일 시총 파싱", str(e)[:120]); break
        if len(found) >= TOP_N: break
    if len(found) < min(TOP_N, 200):
        for page in range(1, 11):
            r = get("https://finance.naver.com/sise/sise_market_sum.naver",
                    {"sosok": 0, "page": page})
            if not r: continue
            r.encoding = "euc-kr"
            for code, name in re.findall(r'code=([0-9]{6})" class="tltle">([^<]+)<', r.text):
                if code.endswith("0"):
                    found.setdefault(code, html.unescape(name.strip()))
            if len(found) >= TOP_N: break
    out = list(found.items())[:TOP_N]
    log(f"[INFO] 유니버스 {len(out)}/{TOP_N}")
    return out

def fetch_quotes(codes):
    """네이버 실시간 국내주식 시세. 응답 필드명 변동에 대비해 alias 처리."""
    chunks = [codes[i:i+25] for i in range(0, len(codes), 25)]
    result = {}
    def worker(chunk):
        r = get("https://polling.finance.naver.com/api/realtime/domestic/stock/" + ",".join(chunk))
        if not r: return {}
        try:
            data = r.json().get("datas", [])
            out = {}
            for i, d in enumerate(data):
                code = str(first(d, ["itemCode", "symbolCode", "stockCode"], chunk[i] if len(data)==len(chunk) else ""))
                if code: out[code] = d
            return out
        except Exception as e:
            log("[WARN] 시세 JSON", str(e)[:100]); return {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        for part in ex.map(worker, chunks): result.update(part)
    log(f"[INFO] 실시간 시세 응답 {len(result)}/{len(codes)}")
    return result

def parse_quote(code, name, d, cap_rank):
    cur = number(first(d, ["closePrice", "nowPrice", "currentPrice", "tradePrice"]))
    op = number(first(d, ["openPrice", "open"]))
    hi = number(first(d, ["highPrice", "high"]))
    lo = number(first(d, ["lowPrice", "low"]))
    vol = number(first(d, ["accumulatedTradingVolume", "tradeVolume", "volume"]), 0)
    amt_raw = number(first(d, ["accumulatedTradingValue", "accumulatedTradingAmount", "tradeValue"]), 0)
    chg_abs = number(first(d, ["compareToPreviousClosePrice", "compareToPreviousPrice", "changePrice"]))
    chg_pct = number(first(d, ["fluctuationsRatio", "changeRate", "rate"]))
    prev = cur - chg_abs if cur is not None and chg_abs is not None else None
    if cur is None or op is None or hi is None or lo is None or prev is None or prev <= 0:
        return None
    # 누적 거래대금 단위는 응답마다 확인이 필요. 주가*거래량과 가장 가까운 배율로 보정.
    amt_won = None
    if amt_raw and vol:
        candidates = [amt_raw, amt_raw*1000, amt_raw*1_000_000]
        implied = [a/vol for a in candidates]
        amt_won = min(candidates, key=lambda a: abs(a/vol-cur))
    if amt_won is None and vol:
        amt_won = cur * vol
    # Naver fluctuationsRatio는 보통 % 숫자. 절대변화로 계산한 값을 우선 사용.
    chg = cur / prev - 1
    return {
        "code": code, "name": name, "cap_rank": cap_rank, "price": cur,
        "prev": prev, "open": op, "high": hi, "low": lo, "volume": vol,
        "amount_won": amt_won, "amount_eok": amt_won/1e8 if amt_won else 0,
        "change_pct": chg*100, "gap_pct": (op/prev-1)*100,
        "high_pct": (hi/prev-1)*100, "from_high_pct": (cur/hi-1)*100 if hi else None,
        "vwap": amt_won/vol if amt_won and vol else None
    }

def fetch_daily(code):
    r = get(f"https://m.stock.naver.com/api/stock/{code}/price",
            {"pageSize": 35, "page": 1})
    if not r: return code, None
    try:
        arr = r.json()
        rows = []
        for x in arr:
            date = str(x.get("localTradedAt", "")).replace("-", "")[:8]
            if date and date < TODAY:
                rows.append((date, number(x.get("closePrice")), number(x.get("accumulatedTradingVolume"))))
        rows.sort()
        if len(rows) < 6: return code, None
        closes = [x[1] for x in rows if x[1] is not None]
        vols = [x[2] for x in rows if x[2] is not None]
        if len(closes) < 6: return code, None
        n = min(20, len(closes))
        return code, {"avg_vol20": sum(vols[-n:])/n if len(vols)>=n else None,
                      "ret5_pct": (closes[-1]/closes[-6]-1)*100,
                      "ma20": sum(closes[-n:])/n}
    except Exception:
        return code, None

def fetch_minutes(code):
    """네이버 fchart XML의 1분봉을 받아 5분봉으로 재구성."""
    r = get("https://fchart.stock.naver.com/sise.nhn",
            {"symbol": code, "timeframe": "minute", "count": 240, "requestType": 0})
    if not r: return code, None
    try:
        raw = re.findall(r'data="(\d{12,14})\|([\d.]+)\|([\d.]+)\|([\d.]+)\|([\d.]+)\|(\d+)"', r.text)
        bars = []
        for z in raw:
            ts = z[0]
            if ts[:8] != TODAY: continue
            bars.append({"ts": ts, "open": float(z[1]), "high": float(z[2]),
                         "low": float(z[3]), "close": float(z[4]), "volume": float(z[5])})
        bars.sort(key=lambda x:x["ts"])
        if len(bars) < 3: return code, None
        # 5분 단위 버킷
        groups = {}
        for b in bars:
            minute = int(b["ts"][10:12])
            bucket = b["ts"][:10] + f"{(minute//5)*5:02d}"
            groups.setdefault(bucket, []).append(b)
        five = []
        for k, arr in sorted(groups.items()):
            five.append({"time": k[-4:-2]+":"+k[-2:], "open":arr[0]["open"],
                         "high":max(x["high"] for x in arr), "low":min(x["low"] for x in arr),
                         "close":arr[-1]["close"], "volume":sum(x["volume"] for x in arr)})
        return code, {"bars5": five, "last5": five[-1],
                      "prev5": five[-2] if len(five)>1 else None,
                      "bars_count":len(bars)}
    except Exception as e:
        log("[WARN] 분봉 파싱", code, str(e)[:100]); return code, None

def fetch_macro():
    out = {}
    try:
        import yfinance as yf
        tickers = {"S&P500":"^GSPC", "NASDAQ":"^IXIC", "SOX":"^SOX",
                   "S&P선물":"ES=F", "NASDAQ선물":"NQ=F", "원달러":"KRW=X",
                   "WTI":"CL=F", "미10년":"^TNX", "VIX":"^VIX"}
        for label, ticker in tickers.items():
            try:
                d = yf.Ticker(ticker).history(period="5d", interval="1d")
                if len(d)>=2:
                    out[label] = round((float(d["Close"].iloc[-1])/float(d["Close"].iloc[-2])-1)*100, 2)
            except Exception: pass
    except Exception as e: log("[WARN] yfinance", str(e)[:100])
    return out

def fetch_news(name):
    cid, secret = os.getenv("NAVER_ID"), os.getenv("NAVER_SECRET")
    if not cid or not secret: return []
    r = get("https://openapi.naver.com/v1/search/news.json",
            {"query":name, "display":3, "sort":"date"},
            {"X-Naver-Client-Id":cid, "X-Naver-Client-Secret":secret})
    if not r: return []
    try:
        return [re.sub(r"<[^>]+>", "", html.unescape(x["title"])) for x in r.json().get("items",[])[:3]]
    except Exception: return []

def rank_pct(s):
    return s.rank(pct=True, method="average").fillna(.5)

def load_prev_snapshot(date, slot):
    p = DOCS / f"snap_{slot}.json"
    if not p.exists(): return None
    try:
        obj=json.loads(p.read_text(encoding="utf-8"))
        return obj if obj.get("date")==date else None
    except Exception: return None

def enrich_and_score(df, daily_map, minute_map, prev_snapshots):
    df=df.copy()
    df["avg_vol20"]=[(daily_map.get(c) or {}).get("avg_vol20") for c in df.index]
    df["ret5_pct"]=[(daily_map.get(c) or {}).get("ret5_pct") for c in df.index]
    df["ma20"]=[(daily_map.get(c) or {}).get("ma20") for c in df.index]
    df["vol_ratio"] = df.apply(lambda r: r.volume/r.avg_vol20 if pd.notna(r.avg_vol20) and r.avg_vol20 else None, axis=1)
    df["vwap_pct"] = df.apply(lambda r: (r.price/r.vwap-1)*100 if r.vwap else None, axis=1)
    df["hold"] = ((df.price>=df.open).astype(float)+(df.vwap_pct.fillna(-99)>=0).astype(float))/2
    df["liq"] = rank_pct(df.amount_eok)
    df["vol_score"] = rank_pct(df.vol_ratio)
    df["momentum"] = (df.change_pct.clip(lower=0, upper=15)/15).fillna(0)
    df["pullback"] = df.from_high_pct.apply(lambda x: 1 if x is not None and -5<=x<=-1 else .65 if x is not None and -8<=x< -5 else .25 if x is not None and x < -8 else .45)
    df["trend"] = df.apply(lambda r: float(r.price>=r.ma20) if pd.notna(r.ma20) else .5, axis=1)
    # 직전 스냅샷과 거래대금/등락률/순위 변화 비교
    for col in ["delta_amt_eok","delta_change_pp","rank_improve"]:
        df[col]=0.0
    prev_slot = {"0920":"0910", "0930":"0920", "0950":"0930"}.get(os.getenv("SLOT","now"))
    prev = prev_snapshots.get(prev_slot) if prev_slot else None
    if prev:
        old=pd.DataFrame(prev.get("rows",[]))
        if not old.empty and "code" in old:
            old=old.set_index("code")
            common=df.index.intersection(old.index)
            df.loc[common,"delta_amt_eok"]=df.loc[common,"amount_eok"]-old.loc[common,"amount_eok"]
            df.loc[common,"delta_change_pp"]=df.loc[common,"change_pct"]-old.loc[common,"change_pct"]
            if "amount_rank" in old:
                df.loc[common,"rank_improve"]=old.loc[common,"amount_rank"]-df.loc[common,"amount_rank"]
    df["accel"]=rank_pct(df.delta_amt_eok.clip(lower=0))*.7+rank_pct(df.rank_improve.clip(lower=0))*.3
    df["score"]=(100*(WEIGHTS["liq"]*df.liq+WEIGHTS["accel"]*df.accel+
        WEIGHTS["vol"]*df.vol_score+WEIGHTS["momentum"]*df.momentum+
        WEIGHTS["pullback"]*df.pullback+WEIGHTS["hold"]*df.hold+
        WEIGHTS["trend"]*df.trend)).round(1)
    # 분봉 구조 및 진입 상태
    df["minute_data"]=[minute_map.get(c) for c in df.index]
    df["pattern"]="관찰"
    df["entry_zone"]=None; df["stop_price"]=None; df["target1"]=None
    for c,r in df.iterrows():
        m=r.minute_data
        if not m: continue
        b=m["last5"]; p=m.get("prev5")
        rising = bool(p and b["close"]>=p["close"] and b["low"]>=p["low"])
        vwap_ok = bool(r.vwap and r.price>=r.vwap)
        open_ok = r.price>=r.open
        if r.change_pct>=3 and r.delta_amt_eok>0 and vwap_ok and rising:
            df.loc[c,"pattern"]="EARLY/BREAKOUT"
        elif r.change_pct>=2 and -5<=r.from_high_pct<=-1 and vwap_ok and open_ok and b["close"]>=b["open"]:
            df.loc[c,"pattern"]="PULLBACK"
        elif r.price<r.open and r.price<r.vwap if r.vwap else r.price<r.open:
            df.loc[c,"pattern"]="약세/매도압력"
        # 가격은 관측된 5분봉 지지/VWAP 주변으로만 산출
        support=max(b["low"], r.vwap) if r.vwap else b["low"]
        if r.price>=support and r.price>0:
            df.loc[c,"entry_zone"]=round(support)
            df.loc[c,"stop_price"]=round(support*.97)
            df.loc[c,"target1"]=round(support*1.06)
    return df

def save_snapshot(slot, df):
    rows=[]
    ordered=df.sort_values("amount_eok",ascending=False)
    ranks={c:i+1 for i,c in enumerate(ordered.index)}
    for c,r in df.iterrows():
        rows.append({"code":c,"amount_eok":round(float(r.amount_eok),2),
                     "change_pct":round(float(r.change_pct),3),"amount_rank":ranks[c],
                     "price":int(r.price)})
    payload={"date":TODAY,"time":dt.datetime.now(KST).strftime("%H:%M:%S"),
             "slot":slot,"rows":rows}
    (DOCS/f"snap_{slot}.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--slot",default=os.getenv("SLOT","now"),choices=SLOTS+["now"])
    args=ap.parse_args(); slot=args.slot
    os.environ["SLOT"] = slot  # 수동 실행에서도 직전 스냅샷 비교 슬롯을 정확히 적용
    log("=== KOSPI300",slot,STAMP(),"===")
    universe=naver_universe()
    if not universe:
        log("[ERROR] 유니버스 수집 실패"); return
    names=dict(universe); caprank={c:i+1 for i,(c,n) in enumerate(universe)}
    quotes=fetch_quotes(list(names))
    rows=[]
    for c,d in quotes.items():
        if c not in names: continue
        x=parse_quote(c,names[c],d,caprank[c])
        if x: rows.append(x)
    if len(rows)<30:
        log(f"[ERROR] 유효 시세 {len(rows)}개. 결과 생성 중단"); return
    full=pd.DataFrame(rows).set_index("code")
    full["amount_rank"]=full.amount_eok.rank(ascending=False,method="min")
    log(f"[INFO] 유효 시세 {len(full)}개 / 유니버스 {len(universe)}개")
    # 전 종목 시세는 확보하고, 상대적으로 비용이 큰 일봉은 거래대금 상위 120 + 상승률 상위 80 + 거래량 상위 50에 수행
    scan=set(full.amount_eok.nlargest(120).index)|set(full.change_pct.nlargest(80).index)|set(full.volume.nlargest(50).index)
    daily_map={}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs={ex.submit(fetch_daily,c):c for c in scan}
        for f in as_completed(futs):
            c,v=f.result(); daily_map[c]=v
    # 분봉은 당일 상위 거래대금/상승률/가속 후보에서 수집
    minute_codes=list((set(full.amount_eok.nlargest(45).index)|set(full.change_pct.nlargest(35).index)))
    minute_map={}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs={ex.submit(fetch_minutes,c):c for c in minute_codes}
        for f in as_completed(futs):
            c,v=f.result(); minute_map[c]=v
    prevs={s:load_prev_snapshot(TODAY,s) for s in SLOTS}
    # 1차 후보 전체에 대한 스코어. 분봉 미수집 종목은 패턴 미확인
    sub=full.loc[list(scan)].copy()
    sub["amount_rank"]=full.amount_rank.reindex(sub.index)
    scored=enrich_and_score(sub,daily_map,minute_map,prevs)
    # 상위 15개 뉴스 수집
    news_map={}
    for c in scored.sort_values("score",ascending=False).head(15).index:
        news_map[c]=fetch_news(names[c])
    macro=fetch_macro()
    # breadth: 수집된 전체 KOSPI300 기준
    adv=int((full.change_pct>0).sum()); dec=int((full.change_pct<0).sum())
    flat=int((full.change_pct==0).sum())
    market=f"상승 {adv} / 하락 {dec} / 보합 {flat} (유효시세 {len(full)})"
    # 최종 정렬: 점수 + 패턴 우선순위
    pat_order={"PULLBACK":0,"EARLY/BREAKOUT":1,"관찰":2,"약세/매도압력":3}
    scored["pat_rank"]=scored.pattern.map(pat_order).fillna(9)
    top=scored.sort_values(["pat_rank","score"],ascending=[True,False]).head(SHOW_N)
    # 별도 거래대금 급증 / 대금 상위 / 눌림목 목록
    accel=scored.sort_values(["delta_amt_eok","rank_improve"],ascending=False).head(15)
    pull=scored[scored.pattern=="PULLBACK"].sort_values("score",ascending=False).head(10)
    stamp=STAMP()
    def fmt(v,d=1): return "미확인" if pd.isna(v) else f"{v:+.{d}f}"
    lines=[]
    lines.append(f"# 코스피 시총 상위 {TOP_N} 장초 스냅샷 ({slot})")
    lines.append(f"- 생성: {stamp} / 시세 수집: 네이버 금융 비공식 실시간 API")
    lines.append(f"- 유니버스: {len(universe)} / 유효 실시간 시세: {len(full)} / 일봉 분석: {len(daily_map)} / 1분봉 수집: {len(minute_map)}")
    lines.append(f"- 시장 breadth: {market}")
    lines.append(f"- 매크로 일간 변화율(야간 실시간 등락률 아님): {json.dumps(macro,ensure_ascii=False)}")
    lines.append("- 미확인: 외국인/기관/프로그램 수급, 코스피200 선물·베이시스, 실시간 환율, NXT 프리마켓, 종목별 확정 촉매(뉴스 API 키 미설정 시)")
    lines.append("\n## 우선 후보")
    lines.append("|순위|종목(코드)|현재가|등락%|거래대금(억원)|대금순위|직전 대비 대금증가|등락 변화%p|VWAP대비|고점대비|패턴|점수|진입참고|손절참고|목표1|뉴스|")
    lines.append("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---|")
    for i,(c,r) in enumerate(top.iterrows(),1):
        news=" / ".join(news_map.get(c,[]))
        lines.append(f"|{i}|{r['name']}({c})|{r.price:,.0f}|{r.change_pct:+.2f}|{r.amount_eok:,.1f}|{r.amount_rank:.0f}|{r.delta_amt_eok:+.1f}|{r.delta_change_pp:+.2f}|{fmt(r.vwap_pct)}|{fmt(r.from_high_pct)}|{r.pattern}|{r.score:.1f}|{int(r.entry_zone) if pd.notna(r.entry_zone) else '미확인'}|{int(r.stop_price) if pd.notna(r.stop_price) else '미확인'}|{int(r.target1) if pd.notna(r.target1) else '미확인'}|{html.escape(news)}|")
    lines.append("\n## 거래대금 증가 / 순위 상승")
    lines.append("|종목|거래대금(억원)|증가(억원)|등락%|대금순위 개선|점수|")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for c,r in accel.iterrows():
        lines.append(f"|{r['name']}({c})|{r.amount_eok:,.1f}|{r.delta_amt_eok:+.1f}|{r.change_pct:+.2f}|{r.rank_improve:+.0f}|{r.score:.1f}|")
    lines.append("\n## 눌림목 후보")
    if pull.empty: lines.append("조건을 충족한 눌림목 후보 없음. 억지로 선정하지 않음.")
    else:
        for c,r in pull.iterrows():
            lines.append(f"- **{r['name']}({c})**: 현재 {r.price:,.0f}원, 고점 대비 {r.from_high_pct:.2f}%, VWAP 대비 {fmt(r.vwap_pct)}%, 5분봉 지지 {r.minute_data['last5']['low']:,.0f}원. 참고 진입 {int(r.entry_zone):,}원 / 무효화 {int(r.stop_price):,}원.")
    lines.append("\n## 해석 유의사항")
    lines.append("- 점수는 후보 간 상대평가 점수이며 상승 확률이 아님.")
    lines.append("- 진입참고 가격은 실제 체결 보장 가격이 아니며, 지지 확인 후 사용.")
    lines.append("- 당일 +10~20% 상승을 보장하지 않음. 과열 추격보다 5분봉 저점·VWAP 회복 여부를 우선.")
    lines.append("- 거래대금 증가 비교는 직전 스냅샷 파일이 같은 날짜에 보존되어 있을 때만 유효.")
    (DOCS/"index.md").write_text("\n".join(lines),encoding="utf-8")
    # 웹페이지로도 공개
    body=html.escape("\n".join(lines))
    page=f"""<!doctype html><html lang="ko"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>KOSPI300 {slot}</title><style>body{{font:14px/1.55 system-ui;margin:20px;max-width:1500px}}pre{{white-space:pre-wrap;overflow-wrap:anywhere}}h1{{font-size:20px}}</style><h1>KOSPI300 장초 분석 {slot}</h1><pre>{body}</pre></html>"""
    (DOCS/"index.html").write_text(page,encoding="utf-8")
    save_snapshot(slot,full)
    # CSV: 분석 대상 종목의 상세 데이터
    export=scored.reset_index()
    export.to_csv(DOCS/f"scan_{slot}.csv",index=False,encoding="utf-8-sig")
    log(f"[DONE] {slot} | 후보 {len(top)} | 눌림목 {len(pull)} | 출력 docs/index.html")

if __name__=="__main__":
    main()
