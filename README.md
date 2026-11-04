# KOSPI300 장초 스캔 테스트

## 실행
```bash
pip install -r requirements.txt
python main.py --slot 0910
python main.py --slot 0920
python main.py --slot 0930
python main.py --slot 0950
```

GitHub Actions는 `.github/workflows/scan.yml`을 사용합니다. GitHub 저장소의 Settings > Secrets and variables > Actions에 `NAVER_ID`, `NAVER_SECRET`을 등록하면 네이버 뉴스 검색 API를 사용할 수 있습니다. 미등록이어도 가격/거래량 분석은 동작합니다.

## 데이터 한계
- 네이버 금융의 비공식 엔드포인트를 사용하므로 응답 필드, 호출 제한, 차단 정책이 바뀔 수 있습니다.
- 현재 코드는 시총 상위 최대 300개를 대상으로 합니다. 코스피 전체 상장 종목 전수 스캔이 아닙니다.
- 외국인/기관/프로그램 수급, KOSPI200 선물·베이시스, NXT 프리마켓, 종목별 섹터 분류는 이 코드에서 확보하지 못하면 미확인입니다.
- yfinance 일봉 변화율은 야간 실시간 선물 변동률이 아닙니다.
- 스냅샷 비교를 위해 Actions가 결과 JSON을 같은 저장소에 커밋합니다. 저장소 기본 브랜치가 `main`이 아니면 workflow의 `ref`와 push 설정을 수정하세요.
- 거래대금/분봉 수집이 일부 실패하면 보고서의 수집 종목 수로 범위를 확인하세요.

## 해석
점수는 확률이 아닙니다. 진입참고가는 관측된 VWAP/5분봉 저점을 이용한 참고값이며, 실제 매수는 호가·체결·손절 가능성을 별도로 확인해야 합니다.
