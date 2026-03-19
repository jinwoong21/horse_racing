from flask import Flask, render_template, request, jsonify, redirect, url_for, session, make_response, send_file, abort
from markupsafe import escape
from pathlib import Path
from urllib.parse import unquote, quote
from collections import Counter
from typing import Optional, List
from flask import send_from_directory
from datetime import datetime
from collections import defaultdict
from flask import url_for
import secrets, os
import cx_Oracle
import pandas as pd
import matplotlib as mpl
from math import isnan

# -----------------------------------------------------------------------------
# 기본 설정
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
CANDIDATES = [ROOT / "templates", ROOT.parent / "templates"]
TEMPLATE_DIR = next((p for p in CANDIDATES if p.exists()), CANDIDATES[0])

# horse_racing_prize.csv 기본 경로 (Windows 실제 경로)
DATA_PATH = Path(r"C:\IT\workspace_python\Untitled Folder\ai\datas\horse_racing_results\horse_racing_prize.csv")

app = Flask(
    __name__,
    template_folder=str(TEMPLATE_DIR),
    static_folder=str(ROOT / "static"),
    static_url_path="/static"
)
app.secret_key = os.environ.get("EMR_SECRET_KEY") or ("dev-" + secrets.token_hex(32))
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.jinja_env.auto_reload = True

# -----------------------------------------------------------------------------
# 공통 유틸
# -----------------------------------------------------------------------------
def _safe(row, *cols, default="-"):
    for c in cols:
        if c in row and pd.notna(row[c]):
            return row[c]
    return default

def _pick(row, *cols, default="-"):
    for c in cols:
        if c in row and pd.notna(row[c]):
            return row[c]
    return default

# -----------------------------------------------------------------------------
# 데이터 로더: horse_racing_prize.csv
# -----------------------------------------------------------------------------
def load_df(strict: bool = False) -> pd.DataFrame:
    """
    horse_racing_prize.csv 를 다중 경로에서 시도해 로드.
    - strict=False: 못 찾으면 빈 DF 반환
    - strict=True : 못 찾으면 예외 발생
    """
    candidates = [
        # race_day_* 변형 파일 우선 (있을 때)
        Path('/mnt/data/race_day_results_from_file_group_random.csv'),
        Path('/mnt/data/race_day_results_from_file_fixed.csv'),
        Path('/mnt/data/race_day_results_from_file.csv'),
        Path('/mnt/data/race_day_results.csv'),
        # 표준 경로들
        DATA_PATH,
        Path(r"C:\IT\workspace_python\ai\datas\horse_racing_results\horse_racing_prize.csv"),
        Path(r"C:\IT\workspace_python\Untitled Folder\ai\datas\horse_racing_prize.csv"),
        Path(r"C:\IT\workspace_python\Untitled Folder\ai\datas\horse_racing_results\horse_racing_prize.csv"),
        Path("/mnt/data/horse_racing_prize.csv"),
    ]
    csv_path = next((p for p in candidates if p and p.exists()), None)

    if csv_path is None:
        msg = f"horse_racing_prize.csv not found. tried: {[str(p) for p in candidates]}"
        app.logger.warning("[PRIZE] " + msg)
        if strict:
            raise FileNotFoundError(msg)
        return pd.DataFrame()

    df = None
    for enc in ("utf-8", "cp949", "euc-kr"):
        try:
            df = pd.read_csv(str(csv_path), encoding=enc)
            app.logger.info(f"[PRIZE] Loaded: {csv_path} (enc={enc})")
            break
        except Exception:
            continue
    if df is None:
        if strict:
            raise RuntimeError(f"Failed to read CSV: {csv_path}")
        app.logger.warning(f"[PRIZE] Read failed: {csv_path} -> empty DataFrame")
        return pd.DataFrame()

    if "race_date" in df.columns:
        df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
        df["year"] = df["race_date"].dt.year
    else:
        df["race_date"] = pd.NaT
        df["year"] = pd.NA
    return df

# -----------------------------------------------------------------------------
# 라우트: 홈/페이지
# -----------------------------------------------------------------------------
@app.route("/")
def root():
    return render_template("index.html")

@app.route("/entries")
def entries_page():
    return render_template("entries.html")

@app.route("/results")
def results_page():
    return render_template("results.html")

@app.route("/horses")
def horses_page():
    return render_template("horses.html")

@app.route("/jockeys")
def jockeys_page():
    return render_template("jockeys.html")

# -----------------------------------------------------------------------------
# API: 출전정보(원본 prize DF에서 요약)
# -----------------------------------------------------------------------------
def build_entries_rows():
    df = load_df()

    # 경주명
    if "race_name" in df.columns:
        df["_race_name_"] = df["race_name"].astype(str)
    elif "horse_name" in df.columns:
        df["_race_name_"] = df["horse_name"].astype(str)
    else:
        df["_race_name_"] = "-"

    # 경주일자 문자열
    if "race_date" in df.columns:
        df["_race_date_"] = df["race_date"].dt.strftime("%Y-%m-%d")
    else:
        df["_race_date_"] = "-"

    grp_key = ["_race_date_", "_race_name_"]
    if "race_no" in df.columns:
        df["_race_no_"] = df["race_no"]
        grp_key.append("_race_no_")
    else:
        df["_race_no_"] = None

    base_col = "horse_name" if "horse_name" in df.columns else df.columns[0]
    grouped = (
        df.groupby(grp_key)[base_col]
          .count().rename("field_size")
          .reset_index()
    )
    df = df.merge(grouped, on=grp_key, how="left")

    rep = (df.sort_values(grp_key)
             .drop_duplicates(subset=grp_key, keep="first")
             .reset_index(drop=True))

    rows = []
    for i, r in rep.iterrows():
        순 = i + 1
        경주일자 = r.get("_race_date_", "-")

        track = _safe(r, "track")
        race_no = r.get("_race_no_", None)
        if pd.isna(race_no) or race_no is None:
            경주 = track if track != "-" else "-"
        else:
            경주 = f"{track} {int(race_no)}R" if track != "-" else f"{int(race_no)}R"

        등급 = _safe(r, "grade", "class", "level")
        거리_raw = _safe(r, "distance")
        if isinstance(거리_raw, (int, float)) and pd.notna(거리_raw):
            거리 = f"{int(거리_raw)}m"
        else:
            거리 = f"{거리_raw}m" if (isinstance(거리_raw, str) and 거리_raw.isdigit()) else (거리_raw if 거리_raw != "-" else "-")

        편성 = _safe(r, "division", "class_division", "편성")
        출전 = int(r.get("field_size", 0)) if pd.notna(r.get("field_size", None)) else "-"
        경주명 = r.get("_race_name_", "-")
        출전시각 = _safe(r, "post_time", "start_time")
        비고 = _safe(r, "remark")

        if (비고 == "-" or pd.isna(비고)) and ("prize_money" in r and pd.notna(r["prize_money"])):
            try:
                비고 = f"상금 ₩ {int(float(r['prize_money'])):,.0f}"
            except Exception:
                비고 = f"상금 ₩ {r['prize_money']}"

        rows.append({
            "순": 순,
            "경주일자": 경주일자,
            "경주": 경주,
            "등급": 등급,
            "거리": 거리,
            "편성": 편성,
            "출전": 출전,
            "경주명": 경주명,
            "출전시각": 출전시각,
            "비고": 비고
        })
    return rows

@app.route("/api/entries")
def api_entries():
    try:
        return jsonify({"data": build_entries_rows()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -----------------------------------------------------------------------------
# API: entry_details.csv 기반 출전상세 (/api/entry_details)
# -----------------------------------------------------------------------------
from typing import Optional

ENTRY_DETAILS_PATHS = [
    Path(r"C:\IT\workspace_python\Untitled Folder\ai\datas\horse_racing_results\entry_details.csv"),
    Path("/mnt/data/entry_details.csv"),
]

def _read_entry_details() -> Optional[pd.DataFrame]:
    for p in ENTRY_DETAILS_PATHS:
        if p.exists():
            for enc in ("utf-8-sig","utf-8","cp949","euc-kr"):
                try:
                    return pd.read_csv(str(p), encoding=enc)
                except Exception:
                    continue
    app.logger.warning("[ENTRY] entry_details.csv not found in %s", [str(x) for x in ENTRY_DETAILS_PATHS])
    return None

def _pick_any(cols, aliases):
    # exact → lower → contains
    for a in aliases:
        if a in cols: return a
    low = {c.lower(): c for c in cols}
    for a in aliases:
        if a.lower() in low: return low[a.lower()]
    for c in cols:
        cl = c.lower()
        if any(a.lower() in cl for a in aliases):
            return c
    return None

@app.route("/api/entry_details")
def api_entry_details():
    df = _read_entry_details()
    if df is None or df.empty:
        return jsonify({"data": []})

    cols = list(df.columns)

    # 원본 컬럼 찾기 (영/한/대소문자/언더스코어 허용)
    c_order = _pick_any(cols, ["order","순","no"])
    c_date  = _pick_any(cols, ["race_date","경주일자","date"])
    c_no    = _pick_any(cols, ["race_no","경주","경주번호","rc_no"])
    c_grade = _pick_any(cols, ["grade","등급"])
    c_dist  = _pick_any(cols, ["distance","거리"])
    c_pair  = _pick_any(cols, ["pairing","편성"])
    c_entry = _pick_any(cols, ["Entry","entry","출전"])
    c_name  = _pick_any(cols, ["race_name","경주명","race"])
    c_time  = _pick_any(cols, ["playing_time","Playing time","출전시간","post_time"])

    rows = []
    for _, r in df.iterrows():
        def g(c):
            return (str(r[c]).strip() if c and c in df.columns and pd.notna(r[c]) else "")
        # 거리 표기 보정: 1200 → 1200M
        dist = g(c_dist)
        if dist and dist.isdigit():
            dist = f"{int(float(dist))}M"

        rows.append({
            "순": g(c_order) or "-",
            "경주일자": g(c_date) or "-",
            "경주": g(c_no) or "-",
            "등급": g(c_grade) or "-",
            "거리": dist or "-",
            "편성": g(c_pair) or "-",
            "출전": g(c_entry) or "-",
            "경주명": g(c_name) or "-",
            "출전시각": g(c_time) or "-",
            "비고": ""   # DataTables 마지막 컬럼 안전용
        })

    # ✅ 정렬: 경주(번호) 오름차순 → 경주일자 내림차순(최신 날짜가 먼저)
    def _parse_date_iso(s: str):
        from datetime import datetime
        try:
            return datetime.strptime(str(s), "%Y-%m-%d")
        except Exception:
            return datetime.min

    # 1) 경주(번호) 오름차순 (안정 정렬)
    try:
        rows.sort(key=lambda x: (int(x["경주"]) if str(x["경주"]).isdigit() else 9999))
    except Exception:
        rows.sort(key=lambda x: x["경주"])

    # 2) 경주일자 내림차순 (최신 날짜가 먼저)
    rows.sort(key=lambda x: _parse_date_iso(x["경주일자"]), reverse=True)

    # ✅ 순번 재부여: 화면에 1~12 → 다시 1~12 → ...
    for i, r in enumerate(rows):
        r["순"] = (i % 12) + 1

    return jsonify({"data": rows})


# -----------------------------------------------------------------------------
# API: race_day_results.csv 기반 결과 (/api/raceday_results)
# -----------------------------------------------------------------------------
RACEDAY_RESULTS_PATHS = [
    Path(r"C:\IT\workspace_python\Untitled Folder\ai\datas\horse_racing_results\race_day_results.csv"),
    Path("/mnt/data/race_day_results.csv"),
]

def _read_raceday_results() -> Optional[pd.DataFrame]:
    for p in RACEDAY_RESULTS_PATHS:
        if p.exists():
            for enc in ("utf-8-sig","utf-8","cp949","euc-kr"):
                try:
                    return pd.read_csv(str(p), encoding=enc)
                except Exception:
                    continue
    app.logger.warning("[RESULTS] race_day_results.csv not found in %s", [str(x) for x in RACEDAY_RESULTS_PATHS])
    return None

def _pick_any(cols, aliases):
    for a in aliases:
        if a in cols: return a
    low = {c.lower(): c for c in cols}
    for a in aliases:
        if a.lower() in low: return low[a.lower()]
    for c in cols:
        cl = c.lower()
        if any(a.lower() in cl for a in aliases):
            return c
    return None

def _to_int_nan(s):
    try:
        import re
        n = re.sub(r"[^\d\-]+", "", str(s))
        return int(n) if n != "" else None
    except Exception:
        return None

@app.route("/api/raceday_results")
def api_raceday_results():
    df = _read_raceday_results()
    if df is None or df.empty:
        return jsonify({"data": []})

    cols = list(df.columns)
    c_date = _pick_any(cols, ["race_date","rc_date","경주일자","경기일자","경주일","date"])
    c_name = _pick_any(cols, ["race_name","nrace_name","경주명","레이스 명","레이스이름","레이스명","race"])
    c_dist = _pick_any(cols, ["distance","거리"])
    c_horse= _pick_any(cols, ["horse_name","hrName","마명","말이름"])
    c_res  = _pick_any(cols, ["result","착순","순위","등수","finish"])
    c_jky  = _pick_any(cols, ["jockey_name","jkName","기수명","기수이름"])
    c_wth  = _pick_any(cols, ["weather","날씨"])
    c_turf = _pick_any(cols, ["turf","잔디상태","잔디","track_condition","바바"])

    def g(row, c):  # 안전 추출
        return (str(row[c]).strip() if c and c in df.columns and pd.notna(row[c]) else "")

    # 날짜/착순 숫자화
    df["__dt__"] = pd.to_datetime(df[c_date], errors="coerce") if c_date else pd.NaT

    import re
    def to_int(s):
        s = re.sub(r"[^\d]+", "", str(s))
        return int(s) if s else None
    df["__rank__"] = df[c_res].map(to_int) if c_res else None

    # 최신 날짜 ↓, 같은 날짜에서는 레이스 이름 ↑ (안정적인 그룹 순서를 위해)
    df = df.sort_values(["__dt__", c_name], ascending=[False, True], na_position="last")

    # 거리 표기 보정
    def fmt_dist(v):
        s = str(v).strip()
        if s == "": return ""
        try: return f"{int(float(s))}M"     # 1200 → 1200M
        except: return s

    out_rows = []
    # (날짜, 레이스 이름)으로 그룹 → 그룹 내부 착순 오름차순 → 레이스번호 1..12 부여
    for (dt, rn), gdf in df.groupby(["__dt__", c_name], sort=False):
        if "__rank__" in gdf.columns:
            gdf = gdf.sort_values(["__rank__"], ascending=[True], na_position="last")
        for race_no, (_, r) in enumerate(gdf.head(12).iterrows(), start=1):
            out_rows.append({
                "레이스번호": race_no,
                "경기일자": (r["__dt__"].strftime("%Y-%m-%d") if pd.notna(r["__dt__"]) else g(r, c_date)),
                "레이스 이름": (g(r, c_name) or "-"),
                "거리": (fmt_dist(g(r, c_dist)) or "-"),
                "착순": (g(r, c_res) or "-"),
                "말이름": (g(r, c_horse) or "-"),
                "기수이름": (g(r, c_jky) or "-"),
                "날씨": (g(r, c_wth) or "-"),
                "잔디상태": (g(r, c_turf) or "-"),
            })

    # 최종 정렬(규칙 고정): 경기일자 ↓ → 레이스 이름 ↑ → 레이스번호 ↑
    def date_key(s):
        try:    return datetime.strptime(str(s), "%Y-%m-%d")
        except: return datetime.min

    out_rows.sort(key=lambda x: (date_key(x["경기일자"]), x["레이스 이름"], x["레이스번호"]))
    out_rows.sort(key=lambda x: date_key(x["경기일자"]), reverse=True)

    out_rows = out_rows[:108]   # 부하 완화(선택)
    return jsonify({"data": out_rows})





@app.route("/api/results")
def api_results():
    # /api/raceday_results와 동일한 결과를 반환(호환 목적)
    return api_raceday_results()
# -----------------------------------------------------------------------------
# API: 경주마목록
# -----------------------------------------------------------------------------
def _first_flag(series):
    if series is None:
        return 0
    s = series.astype(str).str.strip().str.lower()
    aliases = {"1", "1등", "1위", "1st", "first", "win", "우승", "winner"}
    return s.isin([a.lower() for a in aliases]).sum() + s.str.contains(r"^1\s*등$", regex=True).sum()

def build_horses_rows():
    df = load_df().copy()

    # 날짜 컬럼 표준화(이미 있으시면 유지)
    if "race_date" in df.columns:
        df["_race_date_"] = pd.to_datetime(df["race_date"], errors="coerce")
    else:
        df["_race_date_"] = pd.NaT

    if "horse_name" not in df.columns:
        df["horse_name"] = df.iloc[:,0].astype(str)

    grp = df.groupby("horse_name", dropna=False)
    rows = []
    for name, g in grp:
        n_races = len(g)

        # ✅ '가장 최신 경주'의 누적상금(prize_money)을 총상금으로 사용
        g_sorted = g.sort_values("_race_date_", kind="mergesort")
        last_prize = (
            pd.to_numeric(g_sorted.get("prize_money", 0), errors="coerce")
              .fillna(0)
              .iloc[-1] if n_races else 0
        )
        avg_prize = (last_prize / n_races) if n_races else 0

        # 승률/최근경주일 등 나머지는 기존 로직 그대로...
        rs = g.get("result", g.get("착순")).astype(str).str.strip().str.lower()
        win_mask = (rs.isin({"1","1등","1위","1st","first","win","우승"}) |
                    rs.str.contains(r"^1\s*등$", regex=True))
        wins = int(win_mask.sum())
        win_rate = round((wins / n_races) * 100, 1) if n_races else 0.0

        last_dt = pd.to_datetime(g_sorted["_race_date_"], errors="coerce").max()
        last_race = last_dt.strftime("%Y-%m-%d") if pd.notna(last_dt) else "-"

        rows.append({
            "말이름": str(name),
            "출전횟수": int(n_races),
            "1위횟수": int(wins),
            "승률(%)": win_rate,
            "총상금(₩)": int(last_prize),     # ✅ 최신 누적상금
            "평균상금(₩)": int(avg_prize),     # ✅ 최신 누적상금 / 출전수
            "최근경주일": last_race,
            "주로": "-"  # 필요시 기존 로직 유지
        })

    rows = sorted(rows, key=lambda r: (r["총상금(₩)"], r["승률(%)"], r["출전횟수"]), reverse=True)
    return rows



@app.route("/api/horses")
def api_horses():
    try:
        return jsonify({"data": build_horses_rows()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -----------------------------------------------------------------------------
# 경주마 상세 (Profile)  — /horses/<name>
# -----------------------------------------------------------------------------
def _build_horse_profile(horse_name: str):
    df = load_df().copy()
    if "horse_name" not in df.columns or df.empty:
        return None

    # 대상 말 필터 (대소문자 무시)
    sel = df[df["horse_name"].astype(str).str.strip().str.casefold()
             == horse_name.strip().casefold()]
    if sel.empty:
        return None

    # 날짜 컬럼 표준화 + 정렬 (최신 prize_money를 총상금으로 쓰기 위함)
    if "race_date" in sel.columns:
        sel["_race_date_"] = pd.to_datetime(sel["race_date"], errors="coerce")
    else:
        sel["_race_date_"] = pd.NaT

    sel_sorted = sel.sort_values("_race_date_", kind="mergesort")

    # 총상금/출전/승률
    n_race = len(sel_sorted)
    rs = sel_sorted.get("result", sel_sorted.get("착순")).astype(str).str.strip().str.lower()
    win_mask = (
        rs.isin({"1","1등","1위","1st","first","win","우승"})
        | rs.str.contains(r"^1\s*등$", regex=True)
    )
    wins = int(win_mask.sum())
    win_rate = round((wins / n_race) * 100, 1) if n_race else 0.0

    # ✅ 총상금: "최신 경주"의 prize_money 값 1개만 사용 (누적의 누적 방지)
    last_prize = 0
    if "prize_money" in sel_sorted.columns:
        last_pm = pd.to_numeric(sel_sorted["prize_money"], errors="coerce").fillna(0)
        if len(last_pm):
            last_prize = int(last_pm.iloc[-1])  # 최신 행의 누적상금

    # 최근 경주일
    last_race = "-"
    if sel_sorted["_race_date_"].notna().any():
        last_dt = sel_sorted["_race_date_"].max()
        last_race = last_dt.strftime("%Y-%m-%d")

    # 메타
    age     = _pick(sel_sorted.iloc[0], "age", "horse_age", "말령")
    breed   = _pick(sel_sorted.iloc[0], "breed", "품종")
    owner   = _pick(sel_sorted.iloc[0], "owner", "owner_name", "마주", "마주이름")
    trainer = _pick(sel_sorted.iloc[0], "trainer", "trainer_name", "조교사", "조교사이름")
    farm    = _pick(sel_sorted.iloc[0], "farm", "breeder", "생산목장", "생산자")

    # 이미지 (현재 사용 중인 로직 그대로 유지)
    sdir = ROOT / "static" / "img" / "horses"
    photo_url = None
    for ext in ("png", "jpg", "jpeg", "webp"):
        p = sdir / f"{horse_name}.{ext}"
        if p.exists():
            photo_url = url_for('static', filename=f"img/horses/{horse_name}.{ext}")
            break
    if not photo_url:
        photo_url = f"https://picsum.photos/seed/{quote(horse_name)}/900/600"

        # 🐴 이미지 URL을 로컬 static 파일 경로로 변경합니다. (이전 중복 함수 부분이 이 위치로 합쳐졌습니다)

        # 한글 URL 인코딩: '청풍' -> '%EC%B2%AD%ED%92%8D'
        img_name = quote(horse_name)

        # 📌 이미지 확장자를 확인하고 수정하세요! 현재는 .jpg로 설정되어 있습니다.
        #    (이미지 파일은 C:\IT\workspace_python\project_horse\static\img\ 에 있어야 합니다.)
        photo_url = url_for('static', filename=f"img/{img_name}.jpg")

        # 예시: 파일이 .png일 경우
        # photo_url = url_for('static', filename=f"img/{img_name}.png")

    profile = {
        "이름": horse_name,
        "나이": age if age != "-" else "-",
        "말품종": breed,
        "마주이름": owner,
        "조교사이름": trainer,
        "생산목장": farm,
        "총상금": f"₩ {last_prize:,.0f}",   # ✅ 최신 prize_money 1건만
        "승률": f"{win_rate:.1f}%",
        "최근경주일": last_race,
        "전략": "",
        "photo_url": photo_url,              # 이미지 URL 적용
    }
    return profile


@app.route("/horses/<path:name>")
def horse_profile_page(name):
    name = unquote(name)
    prof = _build_horse_profile(name)
    if prof is None:
        return render_template("horse_profile.html", error=f"'{name}' 상세를 찾을 수 없습니다.", profile=None)
    return render_template("horse_profile.html", profile=prof)

# -----------------------------------------------------------------------------
# 기수정보 목록 + 프로필
# -----------------------------------------------------------------------------
DATA_DIRS = [
    Path(r"C:\IT\workspace_python\Untitled Folder\ai\datas\horse_racing_results"),
    Path("/mnt/data/Untitled Folder/ai/datas/horse_racing_results"),
    Path("/mnt/data/horse_racing_results"),
]

def _find_csv(filename: str) -> Optional[Path]:
    for d in DATA_DIRS:
        try:
            p = d / filename
            if p.exists():
                return p
        except Exception:
            continue
    return None

def _read_csv_any(path: Path):
    if not path or not path.exists():
        app.logger.error(f"[CSV] File not found: {path}")
        return None
    for enc in ("utf-8-sig","utf-8","cp949","euc-kr"):
        try:
            return pd.read_csv(str(path), encoding=enc)
        except Exception:
            continue
    for enc in ("utf-8-sig","utf-8","cp949","euc-kr"):
        try:
            return pd.read_csv(str(path), encoding=enc, sep=";")
        except Exception:
            continue
    app.logger.error(f"[CSV] Read failed: {path}")
    return None

def _pick_col(cols, aliases):
    for a in aliases:
        if a in cols: return a
    low = {c.lower(): c for c in cols}
    for a in aliases:
        if a.lower() in low: return low[a.lower()]
    for c in cols:
        cl = c.lower()
        if any(a.lower() in cl for a in aliases):
            return c
    return None

def _parse_wps(sr):
    if sr is None:
        return (0,0,0,0)
    s = sr.astype(str).str.strip().str.lower()
    total = (s != "").sum()
    is1 = s.isin({"1","1등","1위","1st","first","win","우승"}) | s.str.contains(r"^1\s*등$", regex=True)
    is2 = s.isin({"2","2등","2위","2nd","second"}) | s.str.contains(r"^2\s*등$", regex=True)
    is3 = s.isin({"3","3등","3위","3rd","third"}) | s.str.contains(r"^3\s*등$", regex=True)
    return (int(total), int(is1.sum()), int(is2.sum()), int(is3.sum()))

def _fmt_record(total, w, p, s):
    return f"{total}전({w}/{p}/{s})" if total else "0전(0/0/0)"

def build_jockey_rows():
    prize_df = load_df().copy()

    jcol = None
    for c in ["jockey","jockey_name","기수","기수이름","Jockey_name"]:
        if c in prize_df.columns:
            jcol = c; break

    if "race_date" in prize_df.columns:
        prize_df["_date_"] = pd.to_datetime(prize_df["race_date"], errors="coerce")
    else:
        prize_df["_date_"] = pd.NaT

    recent_df = prize_df.copy()
    if prize_df["_date_"].notna().any():
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=365)
        recent_df = recent_df[ recent_df["_date_"] >= cutoff ]

    jcsv = _find_csv("JockeyList.csv")
    rows = []
    if jcsv:
        jdf = _read_csv_any(jcsv)
        if jdf is not None and len(jdf):
            cols = list(jdf.columns)
            cName  = _pick_col(cols, ['Jockey_name','jockey_name','성명','이름'])
            cBirth = _pick_col(cols, ['birth_date','birh_date','생년월일'])
            cDebut = _pick_col(cols, ['debut_day','데뷔일자'])
            cYear  = _pick_col(cols, ['year_record','최근1년전적'])
            cTotal = _pick_col(cols, ['total_record','통산전적'])
            cWt    = _pick_col(cols, ['weight','기승가능중량'])

            order = 1
            for _, r in jdf.iterrows():
                name = str(r.get(cName, "")).strip()
                if not name:
                    continue

                if jcol:
                    all_j = prize_df[prize_df[jcol].astype(str).str.strip().str.casefold() == name.casefold()]
                    rec_t, rec_w, rec_p, rec_s = _parse_wps(all_j.get("result", all_j.get("착순")))
                    yr_j = recent_df[recent_df[jcol].astype(str).str.strip().str.casefold() == name.casefold()]
                    y_t, y_w, y_p, y_s = _parse_wps(yr_j.get("result", yr_j.get("착순")))

                    # winners-only
                    if rec_w <= 0:
                        continue

                    recent_str = _fmt_record(y_t, y_w, y_p, y_s)
                    all_str = _fmt_record(rec_t, rec_w, rec_p, rec_s)
                else:
                    continue

                rows.append({
                    "순": order,
                    "기수명": name,
                    "생년월일": str(r.get(cBirth, "")).strip(),
                    "데뷔일자": str(r.get(cDebut, "")).strip(),
                    "최근1년전적": (str(r.get(cYear, recent_str)).strip() or recent_str),
                    "통산전적": (str(r.get(cTotal, all_str)).strip() or all_str),
                    "기승가능중량": (str(r.get(cWt, "")).strip() or "-"),
                    "비고": ""
                })
                order += 1
    else:
        if jcol:
            for name, grp in prize_df.groupby(prize_df[jcol].astype(str).str.strip(), dropna=True):
                rec_t, rec_w, rec_p, rec_s = _parse_wps(grp.get("result", grp.get("착순")))
                if rec_w <= 0:
                    continue
                yr_grp = recent_df[recent_df[jcol].astype(str).str.strip().str.casefold() == name.casefold()]
                y_t, y_w, y_p, y_s = _parse_wps(yr_grp.get("result", yr_grp.get("착순")))
                rows.append({
                    "순": len(rows) + 1,
                    "기수명": name,
                    "생년월일": "-",
                    "데뷔일자": "-",
                    "최근1년전적": _fmt_record(y_t,y_w,y_p,y_s),
                    "통산전적": _fmt_record(rec_t,rec_w,rec_p,rec_s),
                    "기승가능중량": "-",
                    "비고": ""
                })

    return rows
# ---------------------------------------------------------------------------
# 기수 프로필(상세) + 라우트
#  - JockeyList.csv에서 생년월일/데뷔/가능중량을 읽고
#  - race CSV에서 통산/최근1년 전적을 계산
#  - 사진은 static/img/jockey/{이름}.{png|jpg|jpeg|webp}가 있으면 사용
# ---------------------------------------------------------------------------
from flask import url_for
from typing import Optional

def build_jockey_profile(name: str):
    """프로필 카드와 우승기록을 반환합니다: (profile:dict, wins:list)"""
    pname = str(name).strip()
    prize_df = load_df().copy()

    # 기수 컬럼 통일
    jcol = None
    for c in ["jockey","jockey_name","기수","기수이름","Jockey_name"]:
        if c in prize_df.columns:
            jcol = c; break

    # 날짜 보정
    if "race_date" in prize_df.columns:
        prize_df["_date_"] = pd.to_datetime(prize_df["race_date"], errors="coerce")
    else:
        prize_df["_date_"] = pd.NaT

    # 최근 1년
    recent_df = prize_df.copy()
    if prize_df["_date_"].notna().any():
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=365)
        recent_df = recent_df[ recent_df["_date_"] >= cutoff ]

    # 마스터 CSV(프로필 메타)
    jcsv = _find_csv("JockeyList.csv")
    birth = debut = wt = "-"
    if jcsv:
        jdf = _read_csv_any(jcsv)
        if jdf is not None and len(jdf):
            cols = list(jdf.columns)
            cName  = _pick_col(cols, ['Jockey_name','jockey_name','성명','이름'])
            cBirth = _pick_col(cols, ['birth_date','birh_date','생년월일'])
            cDebut = _pick_col(cols, ['debut_day','데뷔일자'])
            cWt    = _pick_col(cols, ['weight','기승가능중량'])
            hit = jdf[jdf[cName].astype(str).str.strip().str.casefold() == pname.casefold()] if cName else None
            if hit is not None and not hit.empty:
                r0 = hit.iloc[0]
                birth = str(r0.get(cBirth, "")).strip() or "-"
                debut = str(r0.get(cDebut, "")).strip() or "-"
                wt    = str(r0.get(cWt, "")).strip() or "-"

    # 통산/최근1년 전적 계산
    total_str = "0전(0/0/0)"
    year_str  = "0전(0/0/0)"
    wins = []
    if jcol:
        all_j = prize_df[ prize_df[jcol].astype(str).str.strip().str.casefold() == pname.casefold() ]
        rec_t, rec_w, rec_p, rec_s = _parse_wps(all_j.get("result", all_j.get("착순")))
        total_str = _fmt_record(rec_t, rec_w, rec_p, rec_s)

        yr_j = recent_df[ recent_df[jcol].astype(str).str.strip().str.casefold() == pname.casefold() ]
        y_t, y_w, y_p, y_s = _parse_wps(yr_j.get("result", yr_j.get("착순")))
        year_str = _fmt_record(y_t, y_w, y_p, y_s)

        # 우승(착순=1) 기록 최대 30개
        if not all_j.empty:
            rs = all_j.get("result", all_j.get("착순")).astype(str).str.strip().str.lower()
            win_mask = (rs.isin({"1","1등","1위","1st","first","win","우승"}) |
                        rs.str.contains(r"^1\s*등$", regex=True))
            wdf = all_j[win_mask].copy()
            if "race_date" in wdf.columns:
                wdf["_date_"] = pd.to_datetime(wdf["race_date"], errors="coerce")
                wdf = wdf.sort_values("_date_", ascending=False)
            for _, rr in wdf.head(30).iterrows():
                race_date = "-"
                if "race_date" in rr and pd.notna(rr["race_date"]):
                    try: race_date = pd.to_datetime(rr["race_date"]).strftime("%Y-%m-%d")
                    except Exception: race_date = str(rr["race_date"])[:10]
                race_name = str(rr.get("race_name", rr.get("race", "-")))
                horse     = str(rr.get("horse_name", rr.get("말이름", "-")))
                wins.append({"경주일자": race_date, "레이스명": race_name, "말이름": horse, "우승": "1등"})

    # 사진: static/img/jockey/{이름}.{ext}
    sdir = ROOT / "static" / "img" / "jockey"
    photo_url = None
    for ext in ("png","jpg","jpeg","webp"):
        p = sdir / f"{pname}.{ext}"
        if p.exists():
            photo_url = url_for('static', filename=f"img/jockey/{pname}.{ext}")
            break
    if not photo_url:
        photo_url = f"https://picsum.photos/seed/{pname}/800/600"

    profile = {
        "성명": pname,
        "생년월일": birth,          # 마스터 CSV 있으면 채워짐
        "데뷔일자": debut,
        "기승가능중량": wt,
        "총전적": total_str,
        "최근1년전적": year_str,
        "photo_url": photo_url
    }
    return profile, wins
#==============================추가: 기수 우승 차트용 API===============================
def _jockey_wins_payload(jockey_name: str) -> dict:
    """
    주어진 기수의 우승 데이터를 집계해 sankey/river에서 바로 쓸 수 있는 페이로드 생성.
    반환:
      {
        "horses": [{"name":"백두산","value":5}, ...],
        "races":  [{"name":"코리아컵","value":3}, ...],
        "sankey": [{"source":"코리아컵","target":"백두산","value":2}, ...],
        "river" : [{"date":"2025-01","value":1}, {"date":"2025-02","value":3}, ...]
      }
    """
    df = load_df(strict=False).copy()
    if df.empty:
        return {"horses": [], "races": [], "sankey": [], "river": []}

    # 우승 판정(여러 표기 대응)
    aliases = {"1","1등","1위","1st","first","win","우승","winner"}
    res = df.get("result", df.get("착순")).astype(str).str.strip().str.lower()
    win_mask = (res.isin({a.lower() for a in aliases}) | res.str.contains(r"^1\s*등$", regex=True))

    # 기수 컬럼 찾기
    jcol = None
    for c in ["jockey","jockey_name","기수","기수이름","Jockey_name"]:
        if c in df.columns: jcol = c; break
    if jcol is None:
        return {"horses": [], "races": [], "sankey": [], "river": []}

    sub = df[ win_mask & (df[jcol].astype(str).str.strip().str.casefold() ==
                         jockey_name.strip().casefold()) ].copy()
    if sub.empty:
        return {"horses": [], "races": [], "sankey": [], "river": []}

    # 필요한 컬럼
    hcol = next((c for c in ["horse_name","hrName","말이름","마명"] if c in sub.columns), None)
    rcol = next((c for c in ["race_name","nrace_name","race","경주명","레이스명","레이스 이름"] if c in sub.columns), None)
    dcol = next((c for c in ["race_date","rc_date","경주일자","경주일","date"] if c in sub.columns), None)

    # 집계
    horse_cnt = Counter(sub[hcol].astype(str)) if hcol else Counter()
    race_cnt  = Counter(sub[rcol].astype(str)) if rcol else Counter()

    # sankey 링크: race -> horse
    links = []
    if rcol and hcol:
        grp = sub.groupby([rcol, hcol]).size().reset_index(name="value")
        links = [{"source": str(a), "target": str(b), "value": int(v)} for a,b,v in grp.to_records(index=False)]

    # river(월별 우승 빈도)
    river = []
    if dcol:
        sub["__ym__"] = pd.to_datetime(sub[dcol], errors="coerce").dt.strftime("%Y-%m")
        ym_cnt = Counter(sub["__ym__"].dropna().astype(str))
        river = [{"date": k, "value": int(v)} for k,v in sorted(ym_cnt.items())]

    return {
        "horses": [{"name": k, "value": int(v)} for k,v in horse_cnt.items()],
        "races":  [{"name": k, "value": int(v)} for k,v in race_cnt.items()],
        "sankey": links,
        "river":  river
    }

@app.route("/api/jockey_wins/<path:name>")
def api_jockey_wins(name):
    """기수 상세 차트용 데이터"""
    payload = _jockey_wins_payload(name)
    return jsonify(payload)
#================================================================

# 라우트: /jockeys/<이름>  (템플릿: jockey_profile.html)
@app.route("/jockeys/<path:name>")
def jockey_profile_page(name):
    profile, wins = build_jockey_profile(name)
    if not profile:
        return render_template("jockey_profile.html", error=f"'{name}' 프로필을 찾을 수 없습니다.", profile=None)
    return render_template("jockey_profile.html", profile=profile, wins=wins)


@app.route("/api/jockeys")
def api_jockeys():
    try:
        return jsonify({"data": build_jockey_rows()})
    except Exception as e:
        app.logger.exception("[JOCKEY] /api/jockeys failed")
        return jsonify({"error": str(e)}), 500

# 기수 프로필 (/jockeys/<name>)
    jdf = _read_csv_any(jcsv)
    if jdf is None or jdf.empty:
        return render_template("jockey_profile.html", error="JockeyList.csv 가 비었습니다.", profile=None, wins=[])

    cols = list(jdf.columns)
    cName  = _pick_col(cols, ["Jockey_name","jockey_name","성명","이름"])
    cBirth = _pick_col(cols, ["birth_date","birh_date","생년월일"])
    cDebut = _pick_col(cols, ["debut_day","데뷔일자"])
    cYear  = _pick_col(cols, ["year_record","최근1년전적"])
    cTotal = _pick_col(cols, ["total_record","통산전적","총전적"])
    cWt    = _pick_col(cols, ["weight","기승가능중량"])

    if not cName:
        return render_template("jockey_profile.html", error="JockeyList.csv에 Jockey_name(성명) 컬럼이 없습니다.", profile=None, wins=[])

    target = jdf.loc[jdf[cName].astype(str).str.strip() == name.strip()]
    if target.empty:
        target = jdf.loc[jdf[cName].astype(str).str.replace(r"\s+","", regex=True).str.lower()
                         == name.replace(" ","").lower()]
    if target.empty:
        return render_template("jockey_profile.html", error=f"'{name}' 프로필을 찾지 못했습니다.", profile=None, wins=[])

    r = target.iloc[0]
    def g(c):
        return (str(r[c]).strip() if c and (c in jdf.columns) and pd.notna(r[c]) else "-")

    profile = {
        "성명": g(cName),
        "생년월일": g(cBirth),
        "데뷔일자": g(cDebut),
        "기승가능중량": g(cWt),
        "총전적": g(cTotal),
        "최근1년전적": g(cYear),
        "photo_url": f"https://picsum.photos/seed/{quote(name)}/800/600"
    }

    # 우승(1등) 기록 — 다양한 표기 + 느슨 매칭
    dfr = load_df(strict=False)
    wins = []
    if not dfr.empty:
        rcDate = _pick_col(dfr.columns, ["rc_date", "rcDate", "race_date", "경주일자", "경주일", "date"])
        rName  = _pick_col(dfr.columns, ["race_name", "nrace_name", "race", "경주명", "레이스명"])
        hName  = _pick_col(dfr.columns, ["horse_name", "hrName", "hr_name", "마명", "말이름"])
        jName  = _pick_col(dfr.columns, ["jockey_name", "jockey", "jkName", "jk_name", "기수명", "Jockey_name"])
        result = _pick_col(dfr.columns, ["result", "ord", "rank", "finish", "착순", "순위", "등수"])

        if rcDate and rName and hName and jName and result:
            rs = dfr[result].astype(str).str.strip().str.lower()
            win_mask = (rs.isin({"1","1등","1위","1st","first","win","우승"}) |
                        rs.str.contains(r"^1\s*등$", regex=True))

            target_name = profile["성명"].strip()
            jser = dfr[jName].astype(str).str.strip()
            strict_mask = jser.str.casefold() == target_name.casefold()
            loose_mask  = jser.str.replace(r"\s+","", regex=True) \
                               .str.contains(target_name.replace(" ",""), case=False, na=False)
            name_mask = strict_mask | loose_mask

            sub = dfr[name_mask & win_mask].copy()
            sub["__rc_date__"] = pd.to_datetime(sub[rcDate], errors="coerce")
            sub = sub.sort_values("__rc_date__", ascending=False)

            for _, w in sub.head(30).iterrows():
                wins.append({
                    "경주일자": str(w[rcDate]),
                    "레이스명": str(w[rName]),
                    "말이름": str(w[hName]),
                    "우승": "1"
                })

    return render_template("jockey_profile.html", profile=profile, wins=wins, error=None)

# -----------------------------------------------------------------------------
# 정적 CSV 서빙 (/data/<filename>)
# -----------------------------------------------------------------------------
@app.route("/data/<path:filename>")
def serve_data_csv(filename: str):
    allow = {"horse_list.csv", "horse_racing_prize.csv", "JockeyList.csv"}
    if filename not in allow:
        abort(404)

    p = _find_csv(filename)
    if not p:
        app.logger.warning(f"[DATA] CSV not found: {filename} (searched in: {', '.join(map(str, DATA_DIRS))})")
        abort(404)

    return send_file(str(p), as_attachment=False)

# -----------------------------------------------------------------------------
# 기존 TOP API (차트용)
# -----------------------------------------------------------------------------
@app.get("/api/top")
def api_top():
    n = int(request.args.get("n", 6))
    year = request.args.get("year")
    df = load_df()

    if df is None or df.empty:
        return jsonify([])

    if year is not None and "year" in df.columns and df["year"].notna().any():
        df = df[df["year"] == int(year)]

    yearly_peak = (
        df.sort_values(["horse_name", "race_date"], kind="mergesort")
          .groupby(["year", "horse_name"], as_index=False)["prize_money"]
          .max()
    )
    out = (yearly_peak.sort_values(["year", "prize_money"], ascending=[True, False])
                    .groupby("year", as_index=False)
                    .head(n))
    return jsonify(out.to_dict(orient="records"))

# -----------------------------------------------------------------------------
# 로그인 (요약본)
# -----------------------------------------------------------------------------
@app.get("/login")
def login_page():
    if session.get("SESS_KEY_UNAME"):
        return redirect(url_for("root"))
    return render_template("auth_login.html")

# ---- Oracle 연결 헬퍼: XE(11g) 우선, Easy Connect 포함 -----------------------
def _oracle_connect():
    import os
    driver_name = ""
    try:
        # 1) XE(11g)에 가장 확실: cx_Oracle (thick)
        import cx_Oracle as db
        driver_name = "cx_Oracle(thick)"
    except Exception:
        # 2) oracledb를 쓰되, 반드시 thick 모드로 전환 (11g는 thin 미지원)
        import oracledb as db
        driver_name = "oracledb"
        try:
            # ↓ 본인 PC의 Instant Client 경로로 바꾸세요 (예: 19c/21c 등)
            #    XE만 깔린 환경에서도 ORACLE_HOME 하위 bin이 있으면 그 경로를 써도 됩니다.
            db.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_23")
            driver_name = "oracledb(thick)"
        except Exception as e:
            # thick 초기화 실패 시 11g 접속은 거의 불가 → 바로 예외 발생시켜 상단에서 원인 보이게
            raise RuntimeError(f"oracledb thick 초기화 실패: {e}")

    host, port = "localhost", 1521

    # Easy Connect 우선
    ez_candidates = [
        os.getenv("ORACLE_EZCONNECT"),
        f"{host}:{port}/XE",
        f"{host}:{port}/xe",
        f"{host}:{port}/XEXDB",   # ← 추가
    ]
    for ez in filter(None, ez_candidates):
        try:
            app.logger.info(f"[LOGIN] try EZCONNECT={ez} ({driver_name})")
            return db.connect(user="IT", password="0000", dsn=ez, encoding="UTF-8")
        except Exception as e:
            app.logger.warning(f"[LOGIN] EZCONNECT={ez} failed: {e}")

    # service_name 후보
    svc_candidates = [os.getenv("ORACLE_SERVICE"), "XE", "xe", "XEXDB"]  # ← XEXDB 추가
    for svc in filter(None, svc_candidates):
        try:
            dsn = db.makedsn(host, port, service_name=svc)
            app.logger.info(f"[LOGIN] try service_name={svc} ({driver_name})")
            return db.connect(user="IT", password="0000", dsn=dsn, encoding="UTF-8")
        except Exception as e:
            app.logger.warning(f"[LOGIN] service_name={svc} failed: {e}")

    # SID 후보
    for sid in ["XE", "xe"]:
        try:
            dsn = db.makedsn(host, port, sid=sid)
            app.logger.info(f"[LOGIN] try SID={sid} ({driver_name})")
            return db.connect(user="IT", password="0000", dsn=dsn, encoding="UTF-8")
        except Exception as e:
            app.logger.warning(f"[LOGIN] sid={sid} failed: {e}")

    raise RuntimeError("Oracle 접속 실패: 모든 시나리오 불가")

# -----------------------------------------------------------------------------
# 로그인
# -----------------------------------------------------------------------------
@app.post("/login")
def login_submit():
    uid = request.form.get("uid", "").strip()
    upw = request.form.get("upw", "").strip()
    remember = bool(request.form.get("rememberCheck"))

    # 1) Oracle DB 인증
    # 1) Oracle DB 인증
    try:
        with _oracle_connect() as con:
            with con.cursor() as cur:
                # (선택) 스키마 고정: IT로 접속했어도 안전하게 고정
                cur.execute("ALTER SESSION SET CURRENT_SCHEMA = IT")

                # ✅ 포지셔널 바인드 사용 (:1, :2)
                cur.execute("""
                    SELECT uname, gubun
                      FROM USERS
                     WHERE userid = :1 AND userpw = :2
                """, [uid, upw])
                row = cur.fetchone()

        if row:
            uname, gubun = row  # ex) ('관리자','A')
            session["SESS_KEY_UNAME"] = uname
            session["SESS_KEY_GUBUN"] = gubun
            session.permanent = remember
            return redirect(url_for("root"))

    except Exception as e:
        app.logger.warning(f"[LOGIN] Oracle auth skipped: {e}")

    # 2) 데모 계정(오프라인/개발용)
    if uid == "demo" and upw == "demo":
        session["SESS_KEY_UNAME"] = "데모사용자"
        session["SESS_KEY_GUBUN"] = "demo"
        session.permanent = remember
        return redirect(url_for("root"))

    # 3) 실패 시
    return redirect(url_for("login_page", err="아이디 또는 비밀번호가 올바르지 않습니다."))

@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))

@app.route("/horse_racing/pages/login.html")
def legacy_login_html():
    return redirect(url_for("login_page"), code=301)

@app.route("/horse_racing/pages/login_html")
def legacy_login_jinja():
    return redirect(url_for("login_page"), code=301)

@app.get("/_debug/db")
def _debug_db():
    try:
        with _oracle_connect() as con:
            with con.cursor() as cur:
                cur.execute("ALTER SESSION SET CURRENT_SCHEMA = IT")

                cur.execute("select sys_context('USERENV','SESSION_USER'), sys_context('USERENV','CURRENT_SCHEMA') from dual")
                who = cur.fetchone()

                cur.execute("select count(*) from USERS")
                cnt = cur.fetchone()[0]

                # ✅ 포지셔널 바인드
                cur.execute("""
                    select uname, gubun
                      from USERS
                     where userid = :1 and userpw = :2
                """, ["adge", "1234"])
                hit = cur.fetchone()

        return {
            "connect": "OK",
            "session_user": who[0],
            "current_schema": who[1],
            "users_count": int(cnt),
            "adge_1234_match": bool(hit),
        }
    except Exception as e:
        return {"connect": "FAIL", "error": str(e)}, 500


# -----------------------------------------------------------------------------
# 실행
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    mpl.rcParams["axes.unicode_minus"] = False
    app.run(host="127.0.0.1", port=8786, debug=True)
