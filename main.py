import os
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# --- 설정값 ---
API_URL = "https://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList"
STATION_ID = 108 # 서울
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/18esEBrgl-JmkwkxTeOI_7qTFmWgIKhjDoTa9wuF6o9s/edit"
SHEET_NAME = "7. weather"

def get_weather_data(api_key, target_date):
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
    try:
        t = float(temp)
        rh = float(humid) / 100
        di = (9/5 * t) - 0.55 * (1 - rh) * ((9/5 * t) - 26) + 32
        return round(di, 1)
    except:
        return ""

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
        avg_temp = weather.get('avgTa', '')
        max_temp = weather.get('maxTa', '')
        min_temp = weather.get('minTa', '')
        precipitation = weather.get('sumRn', '0.0')
        if not precipitation: precipitation = '0.0'
        humidity = weather.get('avgRhm', '')
        cloud_cover = weather.get('avgTca', '')
        di_val = calculate_di(avg_temp, humidity)
        weather_text = weather.get('iscs', '')

        primary_tag = "Sunny"
        precip_type = ""
        try:
            if float(precipitation) > 0:
                primary_tag = "Rainy"
                precip_type = "Rain"
            elif float(cloud_cover if cloud_cover else 0) > 5:
                primary_tag = "Cloudy"
        except:
            pass

        secondary_tag = weather_text.replace("{", "").replace("}", "") if weather_text else ""

        row = [
            date_display, weather.get('stnId'), weather.get('stnNm'),
            avg_temp, max_temp, min_temp, precipitation, humidity, cloud_cover,
            di_val, precip_type, primary_tag, secondary_tag, weather_text,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]
        update_google_sheet(row)
    else:
        print("❌ 데이터를 가져오지 못했습니다.")

if __name__ == "__main__":
    main()