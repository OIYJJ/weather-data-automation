import os
import json
import requests
import gspread
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# --- 설정값 ---
API_URL = "https://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList"
STATION_ID = 108  # 서울
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/18esEBrgl-JmkwkxTeOI_7qTFmWgIKhjDoTa9wuF6o9s/edit"
SHEET_NAME = "7. weather"
LOCAL_TZ = ZoneInfo("Asia/Seoul")
HTTP_TIMEOUT = 60
DRY_RUN = str(os.getenv("DRY_RUN", "0")).strip() == "1"


def get_kst_yesterday():
    now_kst = datetime.now(LOCAL_TZ)
    yesterday = now_kst.date() - timedelta(days=1)
    return yesterday


def fmt_yyyymmdd(date_obj):
    return date_obj.strftime("%Y%m%d")


def fmt_yyyy_mm_dd(date_obj):
    return date_obj.strftime("%Y-%m-%d")


def get_weather_data(api_key, target_date):
    """기상청 API 호출 (단일 일자)"""
    params = {
        "serviceKey": api_key,
        "pageNo": "1",
        "numOfRows": "10",
        "dataType": "JSON",
        "dataCd": "ASOS",
        "dateCd": "DAY",
        "startDt": target_date,
        "endDt": target_date,
        "stnIds": str(STATION_ID),
    }

    try:
        response = requests.get(API_URL, params=params, timeout=HTTP_TIMEOUT)
        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError:
            print("❌ JSON 디코딩 실패. 응답 내용:", response.text[:1000])
            return None

        header = data.get("response", {}).get("header", {})
        if header.get("resultCode") != "00":
            print(f"❌ API Error: {header.get('resultMsg')}")
            return None

        body = data.get("response", {}).get("body", {})
        items = body.get("items", {}).get("item", [])

        if isinstance(items, dict):
            items = [items]

        if not items:
            print("⚠️ 해당 날짜의 날씨 데이터가 없습니다.")
            return None

        return items[0]

    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return None


def calculate_di(temp, humid):
    """불쾌지수 계산"""
    try:
        t = float(temp)
        rh = float(humid) / 100
        di = (9 / 5 * t) - 0.55 * (1 - rh) * ((9 / 5 * t) - 26) + 32
        return round(di, 1)
    except Exception:
        return ""


def extract_tags(text):
    """
    Secondary_Tags를 위해 핵심 날씨 현상만 추출 (중복 제거, 순서 고정)
    """
    if not text:
        return ""

    keywords = ["비", "눈", "소나기", "우박", "박무", "연무", "황사", "안개", "이슬비"]
    found_tags = []

    for word in keywords:
        if word in text and word not in found_tags:
            found_tags.append(word)

    return ", ".join(found_tags)


def get_gspread_client():
    google_sheet_key = os.environ.get("GOOGLE_SHEET_KEY")
    if not google_sheet_key:
        raise RuntimeError("❌ GOOGLE_SHEET_KEY가 설정되지 않았습니다.")

    creds_json = json.loads(google_sheet_key)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    return client


def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def load_existing_key_map(sheet):
    """
    key = (date, station_id) -> row_number
    """
    values = sheet.get_all_values()
    key_map = {}

    if not values:
        return key_map

    for row_num, row in enumerate(values, start=1):
        if row_num == 1:
            continue

        date_val = row[0].strip() if len(row) > 0 else ""
        station_val = row[1].strip() if len(row) > 1 else ""

        if date_val and station_val:
            key_map[(date_val, station_val)] = row_num

    return key_map


def upsert_google_sheet(row_data):
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(SPREADSHEET_URL).worksheet(SHEET_NAME)

        date_val = str(row_data[0]).strip()
        station_val = str(row_data[1]).strip()
        key = (date_val, station_val)

        existing_key_map = load_existing_key_map(sheet)

        if DRY_RUN:
            action = "update" if key in existing_key_map else "append"
            print(f"[DRY_RUN] Google Sheet {action} 예정: key={key}")
            return

        if key in existing_key_map:
            row_num = existing_key_map[key]
            end_col = col_num_to_letter(len(row_data))
            sheet.update(
                range_name=f"A{row_num}:{end_col}{row_num}",
                values=[row_data],
                value_input_option="USER_ENTERED",
            )
            print(f"✅ 구글 시트 업데이트 완료! (update, row={row_num}, key={key})")
        else:
            sheet.append_row(row_data, value_input_option="USER_ENTERED")
            print(f"✅ 구글 시트 업데이트 완료! (append, key={key})")

    except Exception as e:
        print(f"❌ 구글 시트 업데이트 실패: {e}")
        raise


def build_row(weather, date_display):
    avg_temp = weather.get("avgTa", "")
    max_temp = weather.get("maxTa", "")
    min_temp = weather.get("minTa", "")

    precipitation = weather.get("sumRn", "0.0")
    if not precipitation:
        precipitation = "0.0"

    humidity = weather.get("avgRhm", "")
    cloud_cover = weather.get("avgTca", "")
    di_val = calculate_di(avg_temp, humidity)

    raw_weather_text = weather.get("iscs", "") or ""
    secondary_tags = extract_tags(raw_weather_text)

    precip_type = "None"
    try:
        if "비" in raw_weather_text or "소나기" in raw_weather_text:
            precip_type = "Rain"
        elif "눈" in raw_weather_text:
            precip_type = "Snow"
        elif "진눈깨비" in raw_weather_text:
            precip_type = "Sleet"
        elif float(precipitation) > 0:
            precip_type = "Rain"
    except Exception:
        pass

    primary_tag = "Sunny"
    try:
        rn_val = float(precipitation)
        cc_val = float(cloud_cover if cloud_cover else 0)

        if rn_val > 0 or precip_type in ["Rain", "Snow", "Sleet"]:
            if precip_type == "Snow":
                primary_tag = "Snowy"
            else:
                primary_tag = "Rainy"
        elif cc_val >= 6.0:
            primary_tag = "Cloudy"
        elif cc_val >= 3.0:
            primary_tag = "Partly Cloudy"
    except Exception:
        pass

    updated_at = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")

    row = [
        date_display,              # A: Date
        str(weather.get("stnId")), # B: STN
        weather.get("stnNm", ""),  # C: Region
        avg_temp,                  # D: Avg_Temp
        max_temp,                  # E: Max_Temp
        min_temp,                  # F: Min_Temp
        precipitation,             # G: Precipitation
        humidity,                  # H: Humidity
        cloud_cover,               # I: Cloud_Cover
        di_val,                    # J: DI
        precip_type,               # K: Precip_Type
        primary_tag,               # L: Primary_Tag
        secondary_tags,            # M: Secondary_Tags
        updated_at,                # N: Updated_At
    ]
    return row


def main():
    yesterday = get_kst_yesterday()
    target_date_str = fmt_yyyymmdd(yesterday)
    date_display = fmt_yyyy_mm_dd(yesterday)

    api_key = os.environ.get("KMA_API_KEY")
    if not api_key:
        raise RuntimeError("❌ KMA_API_KEY가 설정되지 않았습니다.")

    print("🚀 Daily Weather Update 시작")
    print(f"   - Target Date (KST): {date_display}")
    print(f"   - DRY_RUN: {DRY_RUN}")

    weather = get_weather_data(api_key, target_date_str)

    if not weather:
        raise RuntimeError("❌ 데이터를 가져오지 못했습니다.")

    row = build_row(weather, date_display)
    upsert_google_sheet(row)

    print("🏁 Daily Weather Update 종료")


if __name__ == "__main__":
    main()
