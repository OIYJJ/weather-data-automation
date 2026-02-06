import os
import json
import requests
import gspread
import time
import re
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# --- 설정값 ---
API_URL = "https://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList"
STATION_ID = 108 # 서울
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/18esEBrgl-JmkwkxTeOI_7qTFmWgIKhjDoTa9wuF6o9s/edit"
SHEET_NAME = "7. weather"

def get_yearly_weather(api_key, start_date, end_date):
    """지정된 기간의 날씨 데이터를 한 번에 가져옴"""
    params = {
        'serviceKey': api_key,
        'pageNo': '1',
        'numOfRows': '400', # 1년 치 데이터를 넉넉하게 요청
        'dataType': 'JSON',
        'dataCd': 'ASOS',
        'dateCd': 'DAY',
        'startDt': start_date,
        'endDt': end_date,
        'stnIds': str(STATION_ID)
    }

    try:
        response = requests.get(API_URL, params=params)
        data = response.json()
        if data['response']['header']['resultCode'] == '00':
            return data['response']['body']['items']['item']
        else:
            print(f"API Error ({start_date}~{end_date}): {data['response']['header']['resultMsg']}")
            return []
    except Exception as e:
        print(f"Connection Error ({start_date}~{end_date}): {e}")
        return []

def calculate_di(temp, humid):
    try:
        t = float(temp)
        rh = float(humid) / 100
        di = (9/5 * t) - 0.55 * (1 - rh) * ((9/5 * t) - 26) + 32
        return round(di, 1)
    except:
        return ""

def extract_tags(text):
    if not text: return ""
    keywords = ['비', '눈', '소나기', '우박', '박무', '연무', '황사', '안개', '이슬비']
    found_tags = set()
    for word in keywords:
        if word in text: found_tags.add(word)
    return ", ".join(list(found_tags))

def main():
    api_key = os.environ.get('KMA_API_KEY')
    if not api_key:
        print("❌ KMA_API_KEY 없음")
        return

    # 구글 시트 연결
    creds_json = json.loads(os.environ['GOOGLE_SHEET_KEY'])
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(SPREADSHEET_URL).worksheet(SHEET_NAME)

    # 2016년부터 2026년까지 반복
    years = range(2016, 2027) 
    
    for year in years:
        start_dt = f"{year}0101"
        
        # 종료일 설정 (2026년은 2월 5일까지, 나머지는 12월 31일까지)
        if year == 2026:
            end_dt = "20260205"
        else:
            end_dt = f"{year}1231"
            
        print(f"🔄 {year}년 데이터 수집 중... ({start_dt} ~ {end_dt})")
        
        items = get_yearly_weather(api_key, start_dt, end_dt)
        
        if not items:
            print(f"⚠️ {year}년 데이터 없음 건너뜀")
            continue

        rows_to_add = []
        
        for weather in items:
            # 데이터 가공 (main.py와 동일 로직)
            avg_temp = weather.get('avgTa', '')
            max_temp = weather.get('maxTa', '')
            min_temp = weather.get('minTa', '')
            precipitation = weather.get('sumRn', '0.0')
            if not precipitation: precipitation = '0.0'
            humidity = weather.get('avgRhm', '')
            cloud_cover = weather.get('avgTca', '')
            di_val = calculate_di(avg_temp, humidity)
            raw_weather_text = weather.get('iscs', '')
            secondary_tags = extract_tags(raw_weather_text)
            
            # 날짜 포맷 변경 (YYYY-MM-DD)
            tm = weather.get('tm') # 2016-01-01 형태
            
            precip_type = "None"
            if "비" in raw_weather_text or "소나기" in raw_weather_text: precip_type = "Rain"
            elif "눈" in raw_weather_text: precip_type = "Snow"
            elif "진눈깨비" in raw_weather_text: precip_type = "Sleet"
            elif float(precipitation) > 0: precip_type = "Rain"

            primary_tag = "Sunny"
            try:
                rn_val = float(precipitation)
                cc_val = float(cloud_cover if cloud_cover else 0)
                if rn_val > 0 or precip_type in ["Rain", "Snow", "Sleet"]:
                    primary_tag = "Rainy" if precip_type == "Rain" else "Snowy"
                    if precip_type == "Sleet": primary_tag = "Rainy"
                elif cc_val >= 6.0: primary_tag = "Cloudy"
                elif cc_val >= 3.0: primary_tag = "Partly Cloudy"
            except: pass

            updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            row = [
                tm, weather.get('stnId'), weather.get('stnNm'),
                avg_temp, max_temp, min_temp, precipitation, humidity, cloud_cover,
                di_val, precip_type, primary_tag, secondary_tags, updated_at
            ]
            rows_to_add.append(row)
        
        # 1년치 데이터를 한 번에 시트에 추가
        if rows_to_add:
            sheet.append_rows(rows_to_add)
            print(f"✅ {year}년 데이터 {len(rows_to_add)}건 업로드 완료!")
        
        time.sleep(2) # API 부하 방지용 대기

if __name__ == "__main__":
    main()
