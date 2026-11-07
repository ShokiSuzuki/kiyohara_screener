"""
清原達郎式 ネットキャッシュ比率スクリーナー
===============================================
EDINETから有価証券報告書を取得し、XBRLを解析して
清原式ネットキャッシュ比率でスクリーニングを行うプログラム。

公式: (流動資産 + 投資有価証券 × 0.7 - 負債合計) / 時価総額

セットアップ:
    pip install requests yfinance pandas lxml

使い方:
    python kiyohara_screener.py --days 30 --ratio 0.5 --max-cap 20
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import threading
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

# --- オプション依存ライブラリ ---
try:
    import requests
except ImportError:
    sys.exit("❌ requests をインストールしてください: pip install requests")

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("⚠️  pandas が見つかりません。CSV出力はカンマ区切り形式で代替します。")

try:
    import yfinance as yf

    HAS_YF = True
except ImportError:
    HAS_YF = False
    print("⚠️  yfinance が見つかりません。時価総額はEDINETデータのみで推定します。")

# ======================================================================
# 設定
# ======================================================================

DEFAULT_API_KEY = (
    ""  # EDINET APIキー (サーバー環境変数で管理する場合は空にしてください)
)
EDINET_BASE = "https://api.edinet-fsa.go.jp/api/v2"
REQUEST_DELAY = 0.5  # APIリクエスト間の待機秒数（サーバー負荷軽減）
TIMEOUT = 30

# 清原式でよく使われる時価総額上限(億円)  ※--max-cap で変更可能
DEFAULT_MAX_CAP_OKU = 500
# 清原式でよく使われる時価総額下限(億円)  ※--min-cap で変更可能
DEFAULT_MIN_CAP_OKU = 20

# スクリーニング閾値
DEFAULT_MIN_RATIO = 1.0  # ネットキャッシュ比率の最小値


# ======================================================================
# EDINET API
# ======================================================================


class EdinetClient:
    """EDINET API v2 クライアント"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Accept-Encoding": "gzip"})

    def _get(self, path: str, params: dict, stream: bool = False) -> requests.Response:
        params = params.copy() if params else {}
        if self.api_key:
            params["Subscription-Key"] = self.api_key
        resp = self.session.get(
            f"{EDINET_BASE}/{path}",
            params=params,
            timeout=TIMEOUT,
            stream=stream,
        )
        resp.raise_for_status()
        return resp

    def get_document_list(self, date: str) -> list[dict]:
        """指定日の書類一覧を取得"""
        try:
            resp = self._get("documents.json", {"date": date, "type": 2})
            return resp.json().get("results") or []
        except Exception as e:
            print(f"  [警告] {date} の書類一覧取得失敗: {e}")
            return []

    def download_xbrl_zip(self, doc_id: str) -> io.BytesIO | None:
        """XBRLのZIPをダウンロード（type=1: XBRL）"""
        try:
            resp = self._get(f"documents/{doc_id}", {"type": 1}, stream=True)
            return io.BytesIO(resp.content)
        except Exception as e:
            print(f"  [警告] {doc_id} のダウンロード失敗: {e}")
            return None


# ======================================================================
# XBRLパーサー
# ======================================================================

# 取得したい XBRL 要素名（日本基準 / IFRS 両対応）
XBRL_TARGETS = {
    # 流動資産
    "current_assets": [
        "CurrentAssets",
        "CurrentAssetsSummaryOfBusinessResults",
        "TotalCurrentAssets",
    ],
    # 投資有価証券
    "invest_securities": [
        "InvestmentSecurities",
        "InvestmentSecuritiesNonCurrentAssets",
        "OtherInvestments",
    ],
    # 負債合計
    "total_liabilities": [
        "Liabilities",
        "TotalLiabilities",
        # "LiabilitiesAndNetAssets",  # フォールバック（使わない）
    ],
    # 発行済株式数（時価総額計算補助）
    "shares_issued": [
        "NumberOfSharesIssuedAndOutstanding",
        "TotalNumberOfIssuedShares",
        "IssuedSharesCommonStock",
    ],
    # 当期純利益 / 当期損失
    "net_income": [
        "ProfitLoss",
        "ProfitLossAttributableToOwnersOfParent",
        "NetIncomeLoss",
        "ProfitLossFromContinuingOperations",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
    ],
}


def parse_xbrl_zip(zip_bytes: io.BytesIO) -> dict | None:
    """
    ZIPからXBRLを抽出し、財務数値を取得する。
    contextRef が 'Prior' を含むものは前期データなので除外。
    単位: 百万円 or 千円 など decimals 属性で判断。
    """
    try:
        with zipfile.ZipFile(zip_bytes) as zf:
            # メインのXBRLファイルを特定（jpcrp または jppfs で始まるもの優先）
            xbrl_files = [
                n
                for n in zf.namelist()
                if n.endswith(".xbrl") and not n.startswith("__MACOSX")
            ]
            if not xbrl_files:
                return None

            # jpcrp → jppfs → その他の優先順
            xbrl_files.sort(
                key=lambda f: 0 if "jpcrp" in f else 1 if "jppfs" in f else 2
            )
            xbrl_file = xbrl_files[0]

            with zf.open(xbrl_file) as f:
                content = f.read().decode("utf-8", errors="ignore")

    except Exception as e:
        print(f"  [警告] ZIP解析エラー: {e}")
        return None

    result = {k: None for k in XBRL_TARGETS}
    result["unit_scale"] = 1_000_000  # デフォルト: 百万円

    # decimalsから単位スケールを推定
    # decimals="-6" → 百万円 / decimals="-3" → 千円
    # decimals は負の値のほかに 0 などもあり得るので、符号ありで拾う
    scale_match = re.search(r'decimals="(-?\d+)"', content)
    if scale_match:
        exp = int(scale_match.group(1))
        result["unit_scale"] = 10 ** (-exp)

    for field, tag_names in XBRL_TARGETS.items():
        for tag in tag_names:
            # contextRef に "Prior" や "Instant_Prior" を含むものは除外
            # 当期末(InstantMember, NonConsolidated, Consolidated)を優先
            # 負債合計は "Liabilities" 系のタグのみを対象にする（"LiabilitiesAndNetAssets" を避けるため）
            if field == "total_liabilities":
                target_liabilities_tags = ["Liabilities", "TotalLiabilities"]
                pattern = (
                    rf"<[^>]*:(?:{'|'.join(target_liabilities_tags)})"
                    r"(?:\s[^>]*)?"
                    r'contextRef="([^"]*)"'
                    r"[^>]*>([0-9\-]+(?:\.[0-9]+)?)</[^>]*>"
                )
                matches = re.findall(pattern, content)
                if not matches:
                    # タグ名が先に来るパターン（属性の順序が違う場合）
                    pattern2 = (
                        rf"<[^>]*:(?:{'|'.join(target_liabilities_tags)})"
                        r'[^>]*contextRef="([^"]*)"'
                        r"[^>]*>([0-9\-]+(?:\.[0-9]+)?)</[^>]*>"
                    )
                    matches = re.findall(pattern2, content)
            else:
                pattern = (
                    rf"<[^>]*:{tag}[^>]*"
                    r'contextRef="([^"]*)"'
                    r"[^>]*>([0-9\-]+(?:\.[0-9]+)?)</[^>]*>"
                )
                matches = re.findall(pattern, content)
                if not matches:
                    # タグ名が先に来るパターン
                    pattern2 = (
                        rf"<[^>]*:{tag}"
                        r'[^>]*contextRef="([^"]*)"'
                        r"[^>]*>([0-9\-]+(?:\.[0-9]+)?)</[^>]*>"
                    )
                    matches = re.findall(pattern2, content)

            # Prior（前期）を除外し、値が最大のものを採用
            valid = [
                (ctx, float(val))
                for ctx, val in matches
                if "Prior" not in ctx and float(val) > 0
            ]
            if valid:
                # 連結優先（NonConsolidated を後回し）
                consolidated = [(c, v) for c, v in valid if "NonConsolidated" not in c]
                chosen = consolidated if consolidated else valid
                # 最大値を採用（複数のcontextがある場合の安全策）
                result[field] = max(v for _, v in chosen)
                break  # このfieldは見つかったので次へ

    return result


# ======================================================================
# 時価総額取得
# ======================================================================

_market_cap_cache: dict[str, float | None] = {}
_market_cap_lock = threading.Lock()


def get_market_cap_jpy(sec_code: str, verbose: bool = False) -> float | None:
    """yfinanceで時価総額(円)を取得。キャッシュ付き（スレッドセーフ）。

    エラー発生時は `None` を返す。HTTP 404 の場合は短くログを出す。
    リトライを行い、最終的に取得できなければキャッシュに None を保存する。
    """
    if not HAS_YF:
        return None

    # 二重検査でロック取得回数を最小化
    with _market_cap_lock:
        if sec_code in _market_cap_cache:
            return _market_cap_cache[sec_code]

    ticker_sym = f"{sec_code[:4]}.T"

    try:
        info = yf.Ticker(ticker_sym).info
        cap = info.get("marketCap")
        with _market_cap_lock:
            _market_cap_cache[sec_code] = cap
        return cap
    except requests.exceptions.HTTPError as he:
        code = None
        try:
            code = he.response.status_code
        except Exception:
            pass
        if code == 404:
            if verbose:
                print(f"  [yfinance] {ticker_sym} の情報が見つかりません (HTTP 404)")
            with _market_cap_lock:
                _market_cap_cache[sec_code] = None
            return None
        # 非404 の HTTP エラーはリトライ対象
        if verbose:
            print(f"  [yfinance] HTTP error for {ticker_sym}")
    except requests.exceptions.RequestException as re:
        if verbose:
            print(f"  [yfinance] リクエストエラー {ticker_sym}: {re}")
    except Exception as e:
        # yfinance が内部で様々な例外を投げることがあるためキャッチしてログ出す
        if verbose:
            print(f"  [yfinance] {ticker_sym} の時価総額取得で例外: {e}")
    # 全て失敗
    with _market_cap_lock:
        _market_cap_cache[sec_code] = None
    return None


# ======================================================================
# 清原式スクリーニングロジック
# ======================================================================


def kiyohara_ratio(
    financials: dict, market_cap: float, unit_scale: float
) -> dict | None:
    """
    清原達郎式ネットキャッシュ比率を計算する。

    Args:
        financials: parse_xbrl_zip の戻り値
        market_cap: 時価総額(円)
        unit_scale: XBRL単位 → 円 変換係数

    Returns:
        計算結果の辞書、または None（データ不足）
    """
    ca = financials.get("current_assets")
    inv = financials.get("invest_securities") or 0.0
    tl = financials.get("total_liabilities")

    if ca is None or tl is None or market_cap is None or market_cap <= 0:
        return None

    # XBRL の数値を円に変換
    ca_jpy = ca * unit_scale
    inv_jpy = inv * unit_scale
    tl_jpy = tl * unit_scale

    net_cash = ca_jpy + inv_jpy * 0.7 - tl_jpy
    ratio = net_cash / market_cap

    # print(f"    → 流動資産: {ca_jpy:.0f}円, 投資有価証券: {inv_jpy:.0f}円, 負債合計: {tl_jpy:.0f}円")
    # print(f"    → ネットキャッシュ: {net_cash:.0f}円, 時価総額: {market_cap:.0f}円, 比率: {ratio:.4f}")

    return {
        "流動資産(億円)": round(ca_jpy / 1e8, 2),
        "投資有価証券(億円)": round(inv_jpy / 1e8, 2),
        "負債合計(億円)": round(tl_jpy / 1e8, 2),
        "ネットキャッシュ(億円)": round(net_cash / 1e8, 2),
        "時価総額(億円)": round(market_cap / 1e8, 2),
        "ネットキャッシュ比率": round(ratio, 4),
    }


# Process worker for parsing + calculation. Must be top-level for pickling.
def _process_file_in_subprocess(item: tuple) -> dict | None:
    """Subprocess worker: reads ZIP file and computes screening row.

    item: (path, sec_code, filer_name, submit_date, market_cap, min_pe, max_pe, min_ratio, max_cap_oku, min_cap_oku)
    """
    try:
        (
            path,
            sec_code,
            filer_name,
            submit_date,
            market_cap,
            min_pe,
            max_pe,
            min_ratio,
            max_cap_oku,
            min_cap_oku,
        ) = item
    except Exception:
        return None

    # read zip
    try:
        with open(path, "rb") as rf:
            zb = io.BytesIO(rf.read())
    except Exception:
        return None

    financials = parse_xbrl_zip(zb)
    if not financials:
        return None

    unit_scale = financials.pop("unit_scale", 1_000_000)

    # market_cap provided by parent
    if market_cap is None:
        return None

    # compute PER
    net_income = financials.get("net_income")
    net_income_jpy = None
    if net_income is not None:
        try:
            net_income_jpy = net_income * unit_scale
        except Exception:
            net_income_jpy = None
    pe = None
    if net_income_jpy is not None and net_income_jpy > 0:
        pe = market_cap / net_income_jpy

    # PER filter
    if min_pe is not None or max_pe is not None:
        if pe is None:
            return None
        if min_pe is not None and pe < min_pe:
            return None
        if max_pe is not None and pe > max_pe:
            return None

    # market cap filter
    if market_cap > max_cap_oku * 1e8:
        return None
    if market_cap < min_cap_oku * 1e8:
        return None

    calc = kiyohara_ratio(financials, market_cap, unit_scale)
    if calc is None:
        return None

    ratio = calc.get("ネットキャッシュ比率")
    if ratio is None:
        return None

    if ratio >= min_ratio:
        row = {
            "銘柄コード": sec_code[:4],
            "企業名": filer_name,
            "提出日": (submit_date or "")[:10],
            **calc,
            "PER": round(pe, 2) if pe is not None else None,
        }
        return row
    return None


# ======================================================================
# ドキュメント単位処理ヘルパー（並列化用）
# ======================================================================


def _process_document(
    doc: dict,
    client: EdinetClient,
    *,
    min_ratio: float,
    max_cap_oku: float,
    min_cap_oku: float,
    min_pe: float | None,
    max_pe: float | None,
    verbose: bool = False,
    progress_callback=None,
) -> dict | None:
    """
    1ドキュメント分の処理を行い、閾値を満たす場合は結果の辞書を返す。
    並列実行されることを想定している。
    """
    name = doc.get("filerName", "不明")
    code = doc.get("secCode", "")[:4]
    doc_id = doc["docID"]
    desc = doc.get("docDescription", "")

    if verbose:
        print(f"    → 処理開始: {name} ({code}) {desc}")

    # XBRL ダウンロード
    zip_bytes = client.download_xbrl_zip(doc_id)
    if not zip_bytes:
        return None

    # 解析
    financials = parse_xbrl_zip(zip_bytes)
    if not financials:
        if verbose:
            print(f"    → XBRLパース失敗: {name} ({code})")
        return None

    unit_scale = financials.pop("unit_scale", 1_000_000)
    if verbose:
        print(f"    → detected unit_scale: {unit_scale}")

    # 時価総額取得
    market_cap = get_market_cap_jpy(doc.get("secCode"), verbose=verbose)
    if market_cap is None:
        if verbose:
            print(f"    → 時価総額取得失敗: {name} ({code})")
        return None

    # PER 用の純利益（XBRL）を計算
    net_income = financials.get("net_income")
    net_income_jpy = None
    if net_income is not None:
        try:
            net_income_jpy = net_income * unit_scale
        except Exception:
            net_income_jpy = None
    pe = None
    if net_income_jpy is not None and net_income_jpy > 0:
        pe = market_cap / net_income_jpy
    if verbose:
        if net_income_jpy is None:
            print(f"    → 純利益データなし (PER計算不可): {name} ({code})")
        else:
            print(
                f"    → 純利益: {net_income_jpy / 1e8:.2f}億円, PER: {pe:.2f}"
                if pe is not None
                else f"    → 純利益: {net_income_jpy / 1e8:.2f}億円, PER: N/A"
            )

    # PERフィルタ
    if min_pe is not None or max_pe is not None:
        if pe is None:
            if verbose:
                print(
                    f"    → スキップ (PERフィルタが指定されていますがPER算出不可): {name} ({code})"
                )
            return None
        if min_pe is not None and pe < min_pe:
            if verbose:
                print(
                    f"    → スキップ (PER下限未満: {pe:.2f} < {min_pe}): {name} ({code})"
                )
            return None
        if max_pe is not None and pe > max_pe:
            if verbose:
                print(
                    f"    → スキップ (PER上限超過: {pe:.2f} > {max_pe}): {name} ({code})"
                )
            return None

    # 時価総額上限/下限チェック
    if market_cap > max_cap_oku * 1e8:
        if verbose:
            print(f"    → 時価総額超過 ({market_cap / 1e8:.0f}億円): {name} ({code})")
        return None
    if market_cap < min_cap_oku * 1e8:
        if verbose:
            print(
                f"    → 時価総額下限未満 ({market_cap / 1e8:.2f}億円): {name} ({code})"
            )
        return None

    calc = kiyohara_ratio(financials, market_cap, unit_scale)
    if calc is None:
        if verbose:
            print(f"    → 財務データ不足: {name} ({code})")
        return None

    ratio = calc["ネットキャッシュ比率"]
    label = "★ 割安候補！" if ratio >= 1.0 else ("◎ 割安" if ratio >= 0.7 else "")

    if ratio >= min_ratio:
        row = {
            "銘柄コード": code,
            "企業名": name,
            "提出日": doc.get("submitDateTime", "")[:10],
            **calc,
            "PER": round(pe, 2) if pe is not None else None,
        }
        try:
            if progress_callback:
                progress_callback(row)
        except Exception:
            pass
        if verbose:
            print(f"    → 比率: {ratio:.4f}  {label}: {name} ({code})")
        return row
    else:
        if verbose:
            print(f"    → 比率: {ratio:.4f} (閾値未満): {name} ({code})")
        return None


# ======================================================================
# メイン処理（並列化対応）
# ======================================================================


def run_screening(
    api_key: str,
    days_back: int,
    min_ratio: float,
    max_cap_oku: float,
    min_cap_oku: float,
    min_pe: float | None,
    max_pe: float | None,
    output_csv: str | None,
    filter_sec_code: str | None,
    verbose: bool,
    progress_callback=None,
    num_workers: int | None = 6,
):
    """
    パイプライン化した処理:
      1) 書類一覧収集 (既存)
      2) XBRL ZIP を並列で先にダウンロードしてキャッシュ化
      3) キャッシュファイルを並列にパースして計算

    これによりネットワーク待ち(ダウンロード)と CPU バウンドなパースを別フェーズで並列化できます。
    """
    if output_csv == "AUTO":
        output_csv = None
    client = EdinetClient(api_key)

    if filter_sec_code:
        print(
            f"  → フィルタ: sec_code が '{filter_sec_code}' で始まる銘柄のみ処理します"
        )

    # 対象日付リスト
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    dates = []
    cur = start_date
    while cur <= end_date:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    print(f"\n🔍 清原式スクリーナー 起動")
    print(f"   対象期間  : {dates[0]} ～ {dates[-1]} ({len(dates)}日間)")
    print(f"   時価総額下限: {min_cap_oku}億円")
    print(f"   時価総額上限: {max_cap_oku}億円")
    print(f"   比率閾値  : {min_ratio} 以上")
    print("=" * 60)

    # --- 1) 書類一覧収集 ---
    all_docs: list[dict] = []
    for date in dates:
        docs = client.get_document_list(date)
        yuho = [
            d
            for d in docs
            if d.get("secCode") and d.get("docTypeCode") in ("120", "130")
        ]
        if yuho:
            print(f"  [{date}] {len(yuho)}件 取得")
            all_docs.extend(yuho)
        time.sleep(REQUEST_DELAY)

    # 重複排除
    unique: dict[str, dict] = {}
    for d in all_docs:
        key = d["secCode"]
        if key not in unique or d["submitDateTime"] > unique[key]["submitDateTime"]:
            unique[key] = d
    docs_to_analyze = list(unique.values())
    print(f"\n重複排除後: {len(docs_to_analyze)}件")

    # デバッグフィルタ
    if filter_sec_code:
        before = len(docs_to_analyze)
        docs_to_analyze = [
            d
            for d in docs_to_analyze
            if d.get("secCode", "").startswith(filter_sec_code)
        ]
        print(
            f"フィルタ適用: {before} -> {len(docs_to_analyze)} 件 (secCode startswith '{filter_sec_code}')"
        )

    # 時価総額事前フィルタ（オプション）
    if HAS_YF:
        max_cap_jpy = max_cap_oku * 1e8
        min_cap_jpy = min_cap_oku * 1e8
        filtered = []
        print("\n時価総額フィルタリング中...")
        for d in docs_to_analyze:
            cap = get_market_cap_jpy(d["secCode"], verbose=verbose)
            if cap is not None and cap > max_cap_jpy:
                if verbose:
                    print(
                        f"  スキップ (時価総額超過) : {d['filerName']} ({cap / 1e8:.0f}億円)"
                    )
                continue
            if cap is not None and cap < min_cap_jpy:
                if verbose:
                    print(
                        f"  スキップ (時価総額下限未満) : {d['filerName']} ({cap / 1e8:.2f}億円)"
                    )
                continue
            filtered.append(d)
            time.sleep(REQUEST_DELAY)
        docs_to_analyze = filtered
        print(f"時価総額フィルタ後: {len(docs_to_analyze)}件")

    # --- Prepare cache dir for XBRL ZIPs ---
    cache_dir = os.path.join("outputs", "xbrl_cache")
    os.makedirs(cache_dir, exist_ok=True)

    # Small helpers inside function to avoid touching global scope
    def _zip_path_for(doc: dict) -> str:
        safe_code = (doc.get("secCode") or "unknown").replace("/", "_")
        fname = f"{safe_code}_{doc.get('docID')}.zip"
        return os.path.join(cache_dir, fname)

    def _download_doc(doc: dict) -> tuple[dict, str | None]:
        """Download a doc's XBRL zip and save to cache. Returns (doc, path or None)."""
        path = _zip_path_for(doc)
        # If already exists and non-empty, skip download
        try:
            if os.path.exists(path) and os.path.getsize(path) > 0:
                return (doc, path)
        except Exception:
            pass
        try:
            zb = client.download_xbrl_zip(doc["docID"])
            if not zb:
                return (doc, None)
            with open(path, "wb") as wf:
                wf.write(zb.getvalue())
            # small delay to avoid hammering API
            time.sleep(REQUEST_DELAY)
            return (doc, path)
        except Exception as e:
            if verbose:
                print(f"  [ダウンロード失敗] {doc.get('filerName', '?')} : {e}")
            return (doc, None)

    # --- 2) ダウンロードフェーズ: 並列で ZIP を取得 ---
    print("\n⬇️  XBRL ZIP をダウンロード (並列)")
    download_workers = max(2, min(16, (num_workers or 6)))
    download_results: list[tuple[dict, str | None]] = []
    with ThreadPoolExecutor(max_workers=download_workers) as dl_exec:
        dl_futures = {dl_exec.submit(_download_doc, d): d for d in docs_to_analyze}
        for fut in as_completed(dl_futures):
            try:
                doc, path = fut.result()
                download_results.append((doc, path))
                if path:
                    if verbose:
                        print(
                            f"  ダウンロード完了: {doc.get('filerName', '?')} -> {path}"
                        )
                else:
                    if verbose:
                        print(
                            f"  ダウンロード失敗/スキップ: {doc.get('filerName', '?')}"
                        )
            except Exception as e:
                if verbose:
                    print(f"  ダウンロードタスクエラー: {e}")

    # フェイルしたものは解析対象から除外
    docs_with_file = [(d, p) for d, p in download_results if p]
    if not docs_with_file:
        print("ダウンロードに成功した XBRL がありません。終了します。")
        return []

    # --- 3) 解析フェーズ: キャッシュファイルを並列にパース ---
    print("\n🧩 XBRL をパースしてスクリーニング (プロセス並列)")

    # Ensure market caps are available for subprocesses (parent fetch)
    items = []
    # collect market caps where missing
    missing = []
    for d, path in docs_with_file:
        cap = d.get("_market_cap_cached") if isinstance(d, dict) else None
        if cap is None:
            missing.append((d, path))
        items.append(
            (
                path,
                d.get("secCode", ""),
                d.get("filerName", ""),
                d.get("submitDateTime", ""),
                cap,
                min_pe,
                max_pe,
                min_ratio,
                max_cap_oku,
                min_cap_oku,
            )
        )

    if missing:
        cap_workers = max(1, min(8, len(missing)))
        with ThreadPoolExecutor(max_workers=cap_workers) as cap_exec:
            cap_futures = {
                cap_exec.submit(get_market_cap_jpy, d["secCode"]): (d, p)
                for d, p in missing
            }
            for fut in as_completed(cap_futures):
                d, p = cap_futures[fut]
                try:
                    cap = fut.result()
                    try:
                        d["_market_cap_cached"] = cap
                    except Exception:
                        pass
                    # update items
                    for idx, it in enumerate(items):
                        if it[0] == p:
                            items[idx] = (
                                it[0],
                                it[1],
                                it[2],
                                it[3],
                                cap,
                                it[5],
                                it[6],
                                it[7],
                                it[8],
                                it[9],
                            )
                            break
                except Exception:
                    pass

    results: list[dict] = []
    parse_workers = max(1, num_workers or 6)
    with ProcessPoolExecutor(max_workers=parse_workers) as proc_exec:
        proc_futures = {
            proc_exec.submit(_process_file_in_subprocess, it): it for it in items
        }
        completed = 0
        for fut in as_completed(proc_futures):
            completed += 1
            it = proc_futures[fut]
            try:
                row = fut.result()
                if row:
                    results.append(row)
                    if verbose:
                        print(
                            f"[{completed:03d}/{len(items):03d}] 追加: {row['企業名']} ({row['銘柄コード']})"
                        )
                else:
                    if verbose:
                        print(f"[{completed:03d}/{len(items):03d}] スキップ: {it[2]}")
            except Exception as e:
                if verbose:
                    print(f"[{completed:03d}/{len(items):03d}] エラー: {e}")

    # 以下は既存の表示/保存ロジックと同じ
    print("\n" + "=" * 60)
    print(f"🏆 スクリーニング結果 (比率 ≥ {min_ratio})")
    print("=" * 60)

    if not results:
        print("該当銘柄なし")
        return []

    results.sort(key=lambda x: x["ネットキャッシュ比率"], reverse=True)

    print(
        f"{'#':>3}  {'銘柄':>6}  {'企業名':<20}  {'比率':>8}  {'時価総額(億)':>12}  {'NC(億)':>10}  {'PER':>6}"
    )
    print("-" * 82)
    for i, r in enumerate(results, 1):
        flag = "★" if r["ネットキャッシュ比率"] >= 1.0 else " "
        per_val = f"{r['PER']:.2f}" if (r.get("PER") is not None) else "—"
        print(
            f"{i:>3}{flag} {r['銘柄コード']:>6}  {r['企業名']:<20}  "
            f"{r['ネットキャッシュ比率']:>8.4f}  "
            f"{r['時価総額(億円)']:>12.1f}  "
            f"{r['ネットキャッシュ(億円)']:>10.1f}  "
            f"{per_val:>6}"
        )

    if output_csv is None:
        today_str = datetime.now().strftime("%Y-%m-%d")
        output_csv = f"outputs/kiyohara_results_{today_str}.csv"

    if HAS_PANDAS:
        df = pd.DataFrame(results)
        df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    else:
        with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)

    print(f"\n✅ 結果を {output_csv} に保存しました ({len(results)}件)")

    tetsuban = [r for r in results if r["ネットキャッシュ比率"] >= 1.0]
    if tetsuban:
        print(f"\n★ 割安候補 (比率 ≥ 1.0): {len(tetsuban)}件")
        for r in tetsuban:
            per_text = f" PER: {r['PER']:.2f}" if (r.get("PER") is not None) else ""
            print(
                f"  {r['銘柄コード']} {r['企業名']}  比率: {r['ネットキャッシュ比率']:.4f}"
                + per_text
            )

    return results


# ======================================================================
# 日付範囲版スクリーニング（並列処理用）
# ======================================================================


def run_screening_for_date_range(
    api_key: str,
    start_date_str: str,  # "YYYY-MM-DD"
    end_date_str: str,  # "YYYY-MM-DD"
    min_ratio: float,
    max_cap_oku: float,
    min_cap_oku: float,
    min_pe: float | None,
    max_pe: float | None,
    output_csv: str | None,
    filter_sec_code: str | None,
    verbose: bool,
    progress_callback=None,
    num_workers: int | None = 6,
):
    """
    チャンク処理向けに、ダウンロード・パースを分離した高速化版。
    返り値は結果リストのみ（CSV 出力なし）。
    """
    if output_csv == "AUTO":
        output_csv = None
    client = EdinetClient(api_key)

    if filter_sec_code:
        print(
            f"  → フィルタ: sec_code が '{filter_sec_code}' で始まる銘柄のみ処理します"
        )

    # 日付リストを生成
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
    dates = []
    cur = start_dt
    while cur <= end_dt:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    print(f"\n🔍 清原式スクリーナー 起動 (チャンク処理)")
    print(f"   対象期間  : {dates[0]} ～ {dates[-1]} ({len(dates)}日間)")
    print(f"   時価総額下限: {min_cap_oku}億円")
    print(f"   時価総額上限: {max_cap_oku}億円")
    print(f"   比率閾値  : {min_ratio} 以上")
    print("=" * 60)

    # --- 1) 書類一覧収集 ---
    all_docs: list[dict] = []
    for date in dates:
        docs = client.get_document_list(date)
        yuho = [
            d
            for d in docs
            if d.get("secCode") and d.get("docTypeCode") in ("120", "130")
        ]
        if yuho:
            print(f"  [{date}] {len(yuho)}件 取得")
            all_docs.extend(yuho)
        time.sleep(REQUEST_DELAY)

    # 重複排除
    unique: dict[str, dict] = {}
    for d in all_docs:
        key = d["secCode"]
        if key not in unique or d["submitDateTime"] > unique[key]["submitDateTime"]:
            unique[key] = d
    docs_to_analyze = list(unique.values())
    print(f"\n重複排除後: {len(docs_to_analyze)}件")

    # フィルタ処理（以下、既存の run_screening と同じ）
    if filter_sec_code:
        before = len(docs_to_analyze)
        docs_to_analyze = [
            d
            for d in docs_to_analyze
            if d.get("secCode", "").startswith(filter_sec_code)
        ]
        print(f"フィルタ適用: {before} -> {len(docs_to_analyze)} 件")

    if HAS_YF:
        max_cap_jpy = max_cap_oku * 1e8
        min_cap_jpy = min_cap_oku * 1e8
        filtered = []
        print("\n時価総額フィルタリング中...")
        for d in docs_to_analyze:
            cap = get_market_cap_jpy(d["secCode"], verbose=verbose)
            if cap is not None and cap > max_cap_jpy:
                if verbose:
                    print(
                        f"  スキップ (時価総額超過) : {d['filerName']} ({cap / 1e8:.0f}億円)"
                    )
                continue
            if cap is not None and cap < min_cap_jpy:
                if verbose:
                    print(
                        f"  スキップ (時価総額下限未満) : {d['filerName']} ({cap / 1e8:.2f}億円)"
                    )
                continue
            filtered.append(d)
            time.sleep(REQUEST_DELAY)
        docs_to_analyze = filtered
        print(f"時価総額フィルタ後: {len(docs_to_analyze)}件")

    # --- Prepare cache dir for XBRL ZIPs ---
    cache_dir = os.path.join("outputs", "xbrl_cache")
    os.makedirs(cache_dir, exist_ok=True)

    def _zip_path_for(doc: dict) -> str:
        safe_code = (doc.get("secCode") or "unknown").replace("/", "_")
        fname = f"{safe_code}_{doc.get('docID')}.zip"
        return os.path.join(cache_dir, fname)

    def _download_doc(doc: dict) -> tuple[dict, str | None]:
        try:
            path = _zip_path_for(doc)
            if os.path.exists(path) and os.path.getsize(path) > 0:
                return (doc, path)
            zb = client.download_xbrl_zip(doc["docID"])
            if not zb:
                return (doc, None)
            with open(path, "wb") as wf:
                wf.write(zb.getvalue())
            time.sleep(REQUEST_DELAY)
            return (doc, path)
        except Exception as e:
            if verbose:
                print(f"  [ダウンロード失敗] {doc.get('filerName', '?')} : {e}")
            return (doc, None)

    print("\n⬇️  XBRL ZIP をダウンロード (並列)")
    download_workers = max(2, min(16, (num_workers or 6)))
    download_results: list[tuple[dict, str | None]] = []
    with ThreadPoolExecutor(max_workers=download_workers) as dl_exec:
        dl_futures = {dl_exec.submit(_download_doc, d): d for d in docs_to_analyze}
        for fut in as_completed(dl_futures):
            try:
                doc, path = fut.result()
                download_results.append((doc, path))
                if path:
                    if verbose:
                        print(
                            f"  ダウンロード完了: {doc.get('filerName', '?')} -> {path}"
                        )
                else:
                    if verbose:
                        print(
                            f"  ダウンロード失敗/スキップ: {doc.get('filerName', '?')}"
                        )
            except Exception as e:
                if verbose:
                    print(f"  ダウンロードタスクエラー: {e}")

    docs_with_file = [(d, p) for d, p in download_results if p]

    if not docs_with_file:
        print("ダウンロードに成功した XBRL がありません。終了します。")
        return []

    print("\n🧩 XBRL をパースしてスクリーニング (並列)")

    def _process_from_file(pair: tuple[dict, str]) -> dict | None:
        doc, path = pair
        name = doc.get("filerName", "不明")
        code = doc.get("secCode", "")[:4]
        try:
            with open(path, "rb") as rf:
                zb = io.BytesIO(rf.read())
        except Exception:
            if verbose:
                print(f"    → ファイル読み込み失敗: {path}")
            return None

        financials = parse_xbrl_zip(zb)
        if not financials:
            if verbose:
                print(f"    → XBRLパース失敗: {name} ({code})")
            return None

        unit_scale = financials.pop("unit_scale", 1_000_000)
        if verbose:
            print(f"    → detected unit_scale: {unit_scale} ({name})")

        market_cap = get_market_cap_jpy(doc.get("secCode"), verbose=verbose)
        if market_cap is None:
            if verbose:
                print(f"    → 時価総額取得失敗: {name} ({code})")
            return None

        # PER
        net_income = financials.get("net_income")
        net_income_jpy = None
        if net_income is not None:
            try:
                net_income_jpy = net_income * unit_scale
            except Exception:
                net_income_jpy = None
        pe = None
        if net_income_jpy is not None and net_income_jpy > 0:
            pe = market_cap / net_income_jpy

        # PER フィルタ
        if min_pe is not None or max_pe is not None:
            if pe is None:
                if verbose:
                    print(f"    → スキップ (PERフィルタ指定だがPER算出不可): {name}")
                return None
            if min_pe is not None and pe < min_pe:
                return None
            if max_pe is not None and pe > max_pe:
                return None

        # 時価総額フィルタ
        if market_cap > max_cap_oku * 1e8:
            return None
        if market_cap < min_cap_oku * 1e8:
            return None

        calc = kiyohara_ratio(financials, market_cap, unit_scale)
        if calc is None:
            return None

        ratio = calc["ネットキャッシュ比率"]
        if ratio >= min_ratio:
            row = {
                "銘柄コード": code,
                "企業名": name,
                "提出日": doc.get("submitDateTime", "")[:10],
                **calc,
                "PER": round(pe, 2) if pe is not None else None,
            }
            try:
                if progress_callback:
                    progress_callback(row)
            except Exception:
                pass
            return row
        return None

    results: list[dict] = []
    parse_workers = max(1, num_workers or 6)
    with ThreadPoolExecutor(max_workers=parse_workers) as parse_exec:
        parse_futures = {
            parse_exec.submit(_process_from_file, p): p for p in docs_with_file
        }
        completed = 0
        for fut in as_completed(parse_futures):
            completed += 1
            doc = parse_futures[fut][0]
            try:
                row = fut.result()
                if row:
                    results.append(row)
                    if verbose:
                        print(
                            f"[{completed:03d}/{len(docs_with_file):03d}] 追加: {row['企業名']} ({row['銘柄コード']})"
                        )
                else:
                    if verbose:
                        print(
                            f"[{completed:03d}/{len(docs_with_file):03d}] スキップ: {doc.get('filerName', '不明')}"
                        )
            except Exception as e:
                if verbose:
                    print(f"[{completed:03d}/{len(docs_with_file):03d}] エラー: {e}")

    print("\n" + "=" * 60)
    print(f"🏆 チャンク処理完了 ({len(results)}件)")
    print("=" * 60)

    return results


# ======================================================================
# エントリーポイント (CLI)
# ======================================================================


def main():
    parser = argparse.ArgumentParser(
        description="清原達郎式ネットキャッシュ比率スクリーナー",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--api-key", default=DEFAULT_API_KEY, help="EDINET APIキー (必須)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="直近N日間の有価証券報告書を対象 (デフォルト: 30)",
    )
    parser.add_argument(
        "--ratio",
        type=float,
        default=DEFAULT_MIN_RATIO,
        help=f"ネットキャッシュ比率の最小値 (デフォルト: {DEFAULT_MIN_RATIO})",
    )
    parser.add_argument(
        "--max-cap",
        type=float,
        default=DEFAULT_MAX_CAP_OKU,
        help=f"時価総額上限(億円) (デフォルト: {DEFAULT_MAX_CAP_OKU})",
    )
    parser.add_argument(
        "--min-cap",
        type=float,
        default=DEFAULT_MIN_CAP_OKU,
        help=f"時価総額下限(億円) (デフォルト: {DEFAULT_MIN_CAP_OKU})",
    )
    parser.add_argument(
        "--min-pe",
        type=float,
        default=None,
        help="PER下限 (例: 5) - 指定時はPERが算出可能な銘柄のみ残す",
    )
    parser.add_argument("--max-pe", type=float, default=None, help="PER上限 (例: 30)")
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="並列ワーカ数（未指定で自動決定、デフォルト6）",
    )
    parser.add_argument(
        "--output",
        nargs="?",
        const="AUTO",
        default=None,
        help=(
            "出力CSVファイル名。値を指定するとその名前で保存します。"
            " --output を単独で指定すると自動でファイル名を決定します"
            " (例: kiyohara_results_YYYY-MM-DD.csv)。"
        ),
    )
    parser.add_argument(
        "--verbose", action="store_true", help="スキップ理由など詳細ログを表示"
    )
    parser.add_argument(
        "--sec-code",
        default=None,
        help="特定の証券コード（先頭数桁でマッチ）だけを計算するデバッグ用フィルタ。例: 4287",
    )
    args = parser.parse_args()

    if not args.api_key:
        sys.exit(
            "❌ EDINET APIキーが設定されていません。\n"
            "   --api-key オプションで指定するか、\n"
            "   スクリプト冒頭の DEFAULT_API_KEY に直接記入してください。\n"
            "   APIキーは https://disclosure2.edinet-fsa.go.jp/ で取得できます。"
        )

    run_screening(
        api_key=args.api_key,
        days_back=args.days,
        min_ratio=args.ratio,
        max_cap_oku=args.max_cap,
        min_cap_oku=args.min_cap,
        min_pe=args.min_pe,
        max_pe=args.max_pe,
        output_csv=args.output,
        filter_sec_code=args.sec_code,
        verbose=args.verbose,
        num_workers=args.workers,
    )


if __name__ == "__main__":
    main()
