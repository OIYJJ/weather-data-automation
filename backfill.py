import os
import json
import time
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# --- 설정값 ---
API_URL = "https://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList"
STATION_ID = 108  # 서울
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/18esEBrgl-JmkwkxTeOI_7qTFmWgIKhjDoTa9wuF6o9s/edit"
SHEET_NAME = "7. weather"
LOCAL_TZ = ZoneInfo("Asia/Seoul")
HTTP_TIMEOUT = 60
PAGE_SIZE = 200
CHUNK_DAYS = 365  # 긴 기간도 안정적으로 처리하기 위해 분할 호출


def parse_ymd(date_str: str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def fmt_yyyymmdd(date_obj):
    return date_obj.strftime("%Y%m%d")


def fmt_yyyy_mm_dd(date_obj):
    return date_obj.strftime("%Y-%m-%d")


def resolve_date_ranges():
    """
    우선순위
    1) TARGET_DATE
    2) START_DATE + END_DATE
    """
    target_date = (os.getenv("TARGET_DATE") or "").strip()
    start_date = (os.getenv("START_DATE") or "").strip()
    end_date = (os.getenv("END_DATE") or "").strip()

    if target_date:
        d = parse_ymd(target_date)
        return [(d, d)]

    if start_date and end_date:
        s = parse_ymd(start_date)
        e = parse_ymd(end_date)
        if s > e:
            raise RuntimeError("❌ START_DATE가 END_DATE보다 늦습니다.")
        return split_date_range(s, e, CHUNK_DAYS)

    raise RuntimeError("❌ TARGET_DATE 또는 START_DATE+END_DATE를 입력해야 합니다.")


def split_date_range(start_date, end_date, chunk_days=365):
    ranges = []
    cur = start_date
    while cur <= end_date:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end_date)
        ranges.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return ranges


def get_weather_range(api_key, start_date, end_date):
    """
    지정된 기간의 날씨 데이터를 pagination으로 안전하게 가져옴
    start_date/end_date: datetime.date
    """
    all_items = []
    page_no = 1

    while True:
        params = {
            "serviceKey": api_key,
            "pageNo": str(page_no),
            "numOfRows": str(PAGE_SIZE),
            "dataType": "JSON",
            "dataCd": "ASOS",
            "dateCd": "DAY",
            "startDt": fmt_yyyymmdd(start_date),
            "endDt": fmt_yyyymmdd(end_date),
            "stnIds": str(STATION_ID),
        }

        try:
            response = requests.get(API_URL, params=params, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            header = data.get("response", {}).get("header", {})
            result_code = header.get("resultCode")
            result_msg = header.get("resultMsg")

            if result_code != "00":
                print(f"❌ API Error ({fmt_yyyy_mm_dd(start_date)}~{fmt_yyyy_mm_dd(end_date)}): {result_msg}")
                return []

            body = data.get("response", {}).get("body", {})
            items = body.get("items", {}).get("item", [])

            if not items:
                break

            if isinstance(items, dict):
                items = [items]

            all_items.extend(items)

            total_count = int(body.get("totalCount", 0))
            if len(all_items) >= total_count:
                break

            if len(items) < PAGE_SIZE:
                break

            page_no += 1

        except Exception as e:
            print(f"❌ Connection Error ({fmt_yyyy_mm_dd(start_date)}~{fmt_yyyy_mm_dd(end_date)}): {e}")
            return []

    return all_items


def calculate_di(temp, humid):
    try:
        t = float(temp)
        rh = float(humid) / 100
        di = (9 / 5 * t) - 0.55 * (1 - rh) * ((9 / 5 * t) - 26) + 32
        return round(di, 1)
    except Exception:
        return ""


def extract_tags(text):
    if not text:
        return ""
    keywords = ["비", "눈", "소나기", "우박", "박무", "연무", "황사", "안개", "이슬비"]
    found_tags = []
    for word in keywords:
        if word in text and word not in found_tags:
            found_tags.append(word)
    return ", ".join(found_tags)


def build_row(weather):
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

    tm = weather.get("tm")  # YYYY-MM-DD
    stn_id = str(weather.get("stnId", "")).strip()
    stn_nm = weather.get("stnNm", "")

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

    return [
        tm,                 # A: date
        stn_id,             # B: station_id
        stn_nm,             # C: station_name
        avg_temp,           # D
        max_temp,           # E
        min_temp,           # F
        precipitation,      # G
        humidity,           # H
        cloud_cover,        # I
        di_val,             # J
        precip_type,        # K
        primary_tag,        # L
        secondary_tags,     # M
        updated_at,         # N
    ]


def get_gspread_client():
    google_sheet_key = os.environ.get("GOOGLE_SHEET_KEY")
    if not google_sheet_key:
        raise RuntimeError("❌ GOOGLE_SHEET_KEY 없음")

    creds_json = json.loads(google_sheet_key)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    return client


def load_existing_key_map(sheet):
    """
    key = (date, station_id) -> row_number
    """
    values = sheet.get_all_values()
    key_map = {}

    if not values:
        return key_map

    for row_num, row in enumerate(values, start=1):
        # 헤더 행 스킵 가정
        if row_num == 1:
            continue

        date_val = row[0].strip() if len(row) > 0 else ""
        station_val = row[1].strip() if len(row) > 1 else ""

        if date_val and station_val:
            key_map[(date_val, station_val)] = row_num

    return key_map


def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def upsert_weather_rows(sheet, rows, dry_run=False):
    if not rows:
        return {"append": 0, "update": 0}

    existing_key_map = load_existing_key_map(sheet)

    updates = []
    appends = []

    for row in rows:
        key = (str(row[0]).strip(), str(row[1]).strip())  # (date, station_id)
        if key in existing_key_map:
            row_num = existing_key_map[key]
            updates.append((row_num, row))
        else:
            appends.append(row)

    if dry_run:
        print(f"[DRY_RUN] append={len(appends)}, update={len(updates)}")
        return {"append": len(appends), "update": len(updates)}

    update_count = 0
    if updates:
        end_col = col_num_to_letter(len(rows[0]))
        batch_payload = []
        for row_num, row in updates:
            batch_payload.append({
                "range": f"A{row_num}:{end_col}{row_num}",
                "values": [row]
            })

        for i in range(0, len(batch_payload), 200):
            sheet.batch_update(batch_payload[i:i + 200], value_input_option="USER_ENTERED")
            update_count += len(batch_payload[i:i + 200])

    append_count = 0
    if appends:
        sheet.append_rows(appends, value_input_option="USER_ENTERED")
        append_count = len(appends)

    return {"append": append_count, "update": update_count}


def main():
    api_key = os.environ.get("KMA_API_KEY")
    if not api_key:
        raise RuntimeError("❌ KMA_API_KEY 없음")

    dry_run = str(os.environ.get("DRY_RUN", "0")).strip() == "1"

    date_ranges = resolve_date_ranges()

    print("🚀 Weather backfill 시작")
    print(f"   - DRY_RUN: {dry_run}")
    print(f"   - 대상 구간 수: {len(date_ranges)}")
    for s, e in date_ranges:
        print(f"   - 구간: {fmt_yyyy_mm_dd(s)} ~ {fmt_yyyy_mm_dd(e)}")

    client = get_gspread_client()
    sheet = client.open_by_url(SPREADSHEET_URL).worksheet(SHEET_NAME)

    total_append = 0
    total_update = 0

    for start_date, end_date in date_ranges:
        print(f"🔄 날씨 데이터 수집 중... ({fmt_yyyy_mm_dd(start_date)} ~ {fmt_yyyy_mm_dd(end_date)})")

        items = get_weather_range(api_key, start_date, end_date)
        if not items:
            print(f"⚠️ 데이터 없음: {fmt_yyyy_mm_dd(start_date)} ~ {fmt_yyyy_mm_dd(end_date)}")
            continue

        rows = [build_row(weather) for weather in items]
        result = upsert_weather_rows(sheet, rows, dry_run=dry_run)

        total_append += result["append"]
        total_update += result["update"]

        print(
            f"✅ 구간 완료 ({fmt_yyyy_mm_dd(start_date)} ~ {fmt_yyyy_mm_dd(end_date)}): "
            f"append={result['append']}, update={result['update']}"
        )

        time.sleep(1)

    print("🏁 Weather backfill 종료")
    print(f"   - total append: {total_append}")
    print(f"   - total update: {total_update}")


if __name__ == "__main__":
    main()
