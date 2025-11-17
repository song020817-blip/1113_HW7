import requests
import pandas as pd
import datetime as dt
import os

# =========================================================
# 1) 기본 설정
# =========================================================

SERVICE_KEY = "caf0c83652279e0202e3ee48a193e0942468afae9fc79ba6648c231af6d299fa"
BASE_URL = "http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"

# 저장 폴더 생성
SAVE_DIR = "./asos_results"
os.makedirs(SAVE_DIR, exist_ok=True)


# =========================================================
# 2) 공공데이터포털 ASOS 요청 함수
# =========================================================

def get_asos_data(start_date, start_hour, end_date, end_hour, stn_id, fname):
    """
    ASOS 시간별 기상데이터 조회 함수
    """
    params = {
        "serviceKey": SERVICE_KEY,
        "dataType": "JSON",
        "dataCd": "ASOS",
        "dateCd": "HR",
        "startDt": start_date,   # YYYYMMDD
        "startHh": start_hour,   # HH
        "endDt": end_date,
        "endHh": end_hour,
        "stnIds": stn_id
    }

    response = requests.get(BASE_URL, params=params)

    # 오류 처리
    if response.status_code != 200:
        print("❌ HTTP 오류:", response.status_code)
        return None

    data = response.json()

    try:
        rows = data["response"]["body"]["items"]["item"]
    except:
        print("❌ 데이터 없음 또는 파싱 오류:", data)
        return None

    df = pd.DataFrame(rows)
    df.to_csv(f"{SAVE_DIR}/{fname}.csv", index=False, encoding="utf-8-sig")

    print(f"✅ 저장 완료: {fname}.csv  (총 {len(df)}개 데이터)")
    return df


# =========================================================
# 3) 과제에서 요구한 3개 시간 구간 자동 조회
# =========================================================

def run_hw7():
    stn = "152"   # 울산 ASOS 지점번호

    # --- (1) 2024년 12월 04일 15~18시 ---
    get_asos_data(
        "20241204", "15",
        "20241204", "18",
        stn, "ASOS_2024-12-04_15-18"
    )

    # --- (2) 2025년 06월 04일 12~16시 ---
    get_asos_data(
        "20250604", "12",
        "20250604", "16",
        stn, "ASOS_2025-06-04_12-16"
    )

    # --- (3) 제출일-2일 = 2025년 11월 15일 00~03시 ---
    get_asos_data(
        "20251115", "00",
        "20251115", "03",
        stn, "ASOS_2025-11-15_00-03"
    )

def merge_asos_results():
    """
    3개 시간구간에서 수집한 CSV 파일을 하나로 합쳐 저장
    """
    files = [
        "./asos_results/ASOS_2024-12-04_15-18.csv",
        "./asos_results/ASOS_2025-06-04_12-16.csv",
        "./asos_results/ASOS_2025-11-15_00-03.csv"
    ]

    df_list = []
    for f in files:
        if os.path.exists(f):
            df_list.append(pd.read_csv(f))
        else:
            print(f"❌ 파일 없음: {f}")

    if not df_list:
        print("❌ 병합할 데이터가 없습니다.")
        return

    final_df = pd.concat(df_list, ignore_index=True)

    save_path = "./asos_results/ASOS_merged_all.csv"
    final_df.to_csv(save_path, index=False, encoding="utf-8-sig")

    print(f"✅ 전체 병합 저장 완료 → {save_path} (총 {len(final_df)}개 데이터)")
    return final_df

if __name__ == "__main__":
    run_hw7()
    merge_asos_results()