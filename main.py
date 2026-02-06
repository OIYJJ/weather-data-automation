import os
import json
import requests
import gspread
import re
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# --- 설정값 ---
API_URL = "https://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList"
STATION_ID = 108 # 서울
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/18esEBrgl-JmkwkxTeOI_7qTFmWgIKhjDoTa9wuF6o9s/edit"
SHEET_NAME = "7. weather"

def get_weather_data(api_key, target_date):
    """기상청 API 호출"""
    params = {
        'serviceKey': api_key,
        'pageNo': '1',
        'numOfRows': '10',
        'dataType': 'JSON',
        'dataCd': 'ASOS',
        'dateCd': 'DAY',
        'startDt': target_date,
        'endDt': target_date,
        'stnIds': str(STATION_ID)
    }

    try:
        response = requests.get(API_URL, params=params)
        try:
            data = response.json()
        except json.JSONDecodeError:
            print("JSON 디코딩 실패. 응답 내용:", response.text)
            return None
        
        if 'response' in data and 'header' in data['response']:
             if data['response']['header']['resultCode'] == '00':
                items = data['response']['body']['items']['item']
                return items[0] if items else None
             else:
                print(f"API Error: {data['response']['header']['resultMsg']}")
                return None
        else:
             print("알 수 없는 응답 구조입니다.")
             return None

    except Exception as e:
        print(f"Connection Error: {e}")
        return None

def calculate_di(temp, humid):
    """불쾌지수 계산"""
    try:
        t = float(temp)
        rh = float(humid) / 100
        di = (9/5 * t) - 0.55 * (1 - rh) * ((9/5 * t) - 26) + 32
        return round(di, 1)
    except:
        return ""

def clean_weather_text(text):
    """
    Weather_Text를 보기 좋게 정제합니다.
    입력: {박무}0020-{박무}{강도0}0300...
    출력: 박무 0020-0300, 연무... (괄호 및 강도 제거)
    """
    if not text:
        return ""
    
    # 1. { } 괄호 제거
    cleaned = text.replace("{", "").replace("}", "")
    # 2. '강도0', '강도1' 등 불필요한 기술 용어 제거
    cleaned = re.sub(r"강도\d+", "", cleaned)
    # 3. 불필요한 하이픈 반복 정리
    cleaned = cleaned.replace("--", "-")
    return cleaned

def extract_tags(text):
    """
    Secondary_Tags를 위해 핵심 날씨 현상만 추출 (중복 제거)
    """
    if not text:
        return ""
    
    # 찾고 싶은 키워드 목록
    keywords = ['비', '눈', '소나기', '우박', '박무', '연무', '황사', '안개', '이슬비']
    found_tags = set()
    
    for word in keywords:
        if word in text:
            found_tags.add(word)
            
    # 리스트로 변환하여 콤마로 연결
    return ", ".join(list(found_tags))

def update_google_sheet(row_data):
    try:
        creds_json = json.loads(os.environ['GOOGLE_SHEET_KEY'])
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_url(SPREADSHEET_URL).worksheet(SHEET_NAME)
        sheet.append_row(row_data)
        print("✅ 구글 시트 업데이트 완료!")
    except Exception as e:
        print(f"구글 시트 업데이트 실패: {e}")

def main():
    # 어제 날짜 구하기
    yesterday = datetime.now() - timedelta(days=1)
    target_date_str = yesterday.strftime("%Y%m%d")
    date_display = yesterday.strftime("%Y-%m-%d")

    api_key = os.environ.get('KMA_API_KEY')
    
    if not api_key:
        print("❌ 오류: KMA_API_KEY가 설정되지 않았습니다.")
        return

    print(f"📅 {target_date_str} 날씨 데이터 조회 시작...")
    
    weather = get_weather_data(api_key, target_date_str)

    if weather:
        # --- 기본 데이터 추출 ---
        avg_temp = weather.get('avgTa', '')
        max_temp = weather.get('maxTa', '')
        min_temp = weather.get('minTa', '')
        
        precipitation = weather.get('sumRn', '0.0')
        if not precipitation: precipitation = '0.0'
        
        humidity = weather.get('avgRhm', '')
        cloud_cover = weather.get('avgTca', '')
        di_val = calculate_di(avg_temp, humidity)
        
        raw_weather_text = weather.get('iscs', '')

        # --- 데이터 가공 로직 강화 ---
        
        # 1. 텍스트 정제
        weather_text_cleaned = clean_weather_text(raw_weather_text)
        secondary_tags = extract_tags(raw_weather_text)

        # 2. Precip_Type 결정 로직 (텍스트 포함 검사)
        precip_type = ""
        # 텍스트에 비/눈 관련 단어가 있거나, 강수량이 0보다 크면
        if "비" in raw_weather_text or "소나기" in raw_weather_text:
            precip_type = "Rain"
        elif "눈" in raw_weather_text:
            precip_type = "Snow"
        elif "진눈깨비" in raw_weather_text:
            precip_type = "Sleet"
        elif float(precipitation) > 0: # 텍스트엔 없지만 강수량이 찍힌 경우
            precip_type = "Rain"
        else:
            precip_type = "None"

        # 3. Primary_Tag 로직
        primary_tag = "Sunny"
        try:
            rn_val = float(precipitation)
            cc_val = float(cloud_cover if cloud_cover else 0)
            
            # 비가 왔거나 강수 형태가 있으면 Rainy
            if rn_val > 0 or precip_type in ["Rain", "Snow", "Sleet"]:
                primary_tag = "Rainy" if precip_type == "Rain" else "Snowy"
                if precip_type == "Sleet": primary_tag = "Rainy"
            # 구름이 많으면 Cloudy
            elif cc_val >= 6.0: # 흐림 기준
                primary_tag = "Cloudy"
            elif cc_val >= 3.0: # 구름 조금
                primary_tag = "Partly Cloudy"
            else:
                primary_tag = "Sunny"
        except:
            pass
        
        # 4. 한국 시간(KST) 구하기
        # GitHub 서버 시간(UTC) + 9시간
        kst_now = datetime.now() + timedelta(hours=9)
        updated_at = kst_now.strftime("%Y-%m-%d %H:%M:%S")

        row = [
            date_display,
            weather.get('stnId'),
            weather.get('stnNm'),
            avg_temp,
            max_temp,
            min_temp,
            precipitation,
            humidity,
            cloud_cover,
            di_val,
            precip_type,        # 수정됨
            primary_tag,
            secondary_tags,     # 수정됨 (깔끔한 단어 나열)
            weather_text_cleaned, # 수정됨 (괄호 제거)
            updated_at          # 수정됨 (한국 시간)
        ]

        update_google_sheet(row)
    else:
        print("❌ 데이터를 가져오지 못했습니다.")

if __name__ == "__main__":
    main()
