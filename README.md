# Kiyohara Net-Cash Screener (清原式ネットキャッシュ比率スクリーナー)

EDINET（金融庁の開示書類検索システム）から有価証券報告書を取得し、XBRLを解析して
「清原式ネットキャッシュ比率」でスクリーニングを行うツールです。

```
清原式ネットキャッシュ比率 = (流動資産 + 投資有価証券 × 0.7 - 負債合計) / 時価総額
```

比率が高いほど、時価総額に対して手元資金に余裕がある（割安な)銘柄とみなされます。

## 構成

| ファイル | 役割 |
|---|---|
| `kiyohara_screener.py` | コア処理。EDINET APIから書類一覧・XBRLを取得し、比率を計算する。CLIとしても直接実行可能 |
| `kiyohara_server.py` | Flaskアプリ。ダッシュボードの配信とスクリーニングジョブの受付・進捗/結果APIを提供 |
| `kiyohara_dashboard.html` | ブラウザ用ダッシュボードUI（`kiyohara_server.py` が配信） |
| `tasks.py` | RQ（Redis Queue）が実行するジョブ本体。日付範囲を分割して並列処理し、結果をマージ |
| `worker.py` | RQワーカーの起動スクリプト。`kiyohara` キューをリッスンしてジョブを処理 |
| `bench_runner.py` | スクリーニング処理のベンチマーク用スクリプト（ワーカー数・処理時間を計測） |
| `Dockerfile` | 本番用イメージ（Gunicornでサーバーを起動） |
| `outputs/` | スクリーニング結果CSV・XBRLキャッシュ・ジョブごとのログ/結果（gitignore対象） |

### アーキテクチャ

```
ブラウザ ──> kiyohara_server.py (Flask/Gunicorn) ──> Redis (RQキュー) ──> worker.py ──> kiyohara_screener.py ──> EDINET API
                 │                                                                            │
                 └── outputs/ ディレクトリ経由でログ・進捗・結果を共有 ──────────────────────────┘
```

ジョブは日付範囲を5日ごとのチャンクに分割し、RQ経由で並列実行、完了後にマスタージョブが結果をマージしてCSV/JSONを出力します。

## セットアップ

### 1. 環境変数

```bash
cp .env.example .env
```

`.env` を編集し、以下を設定してください（`.env` はコミットされません）。

- `EDINET_API_KEY`: EDINET APIキー（[こちら](https://disclosure2.edinet-fsa.go.jp/)で取得）
- `REDIS_URL`: Redis接続URL（Docker利用時はコンテナ名を指定。例: `redis://finance-redis:6379/0`）
- `SECRET_KEY`: Flaskのシークレットキー（任意の値に変更）

### 2. 依存パッケージ

```bash
pip install -r requirements.txt
```

## 起動方法

このアプリは **Flaskサーバー + Redis + RQワーカー** の3つが揃って初めてスクリーニング実行まで機能します。

### Dockerで起動（推奨）

```bash
# イメージをビルド
docker build -t finance:latest .

# 共有ネットワークを作成
docker network create finance-net

# Redis
docker run -d --name finance-redis --network finance-net -v finance-redis-data:/data redis:7-alpine

# Webサーバー（ダッシュボード + API）
docker run -d --name finance-app --network finance-net -p 5000:5000 \
  --env-file .env -e REDIS_URL=redis://finance-redis:6379/0 \
  -v "$(pwd)/outputs:/app/outputs" finance:latest

# RQワーカー
docker run -d --name finance-worker --network finance-net \
  --env-file .env -e REDIS_URL=redis://finance-redis:6379/0 \
  -v "$(pwd)/outputs:/app/outputs" finance:latest python worker.py
```

起動後、`http://localhost:5000` でダッシュボードにアクセスできます。

### ローカルで直接起動

別ターミナルでRedis、サーバー、ワーカーをそれぞれ起動します。

```bash
# Redis（別途インストールが必要）
redis-server

# Webサーバー
python kiyohara_server.py

# RQワーカー
python worker.py
```

### CLIから直接スクリーニングを実行（サーバー不要）

```bash
python kiyohara_screener.py --days 30 --ratio 1.0 --max-cap 500 --min-cap 20 --output
```

主なオプション:

- `--days`: 直近何日分の報告書を対象にするか（デフォルト30）
- `--ratio`: ネットキャッシュ比率の最小値（デフォルト1.0）
- `--max-cap` / `--min-cap`: 時価総額の上限/下限（億円）
- `--min-pe` / `--max-pe`: PERでの絞り込み
- `--sec-code`: 特定の証券コードのみ計算するデバッグ用フィルタ
- `--workers`: 並列ワーカー数
- `--verbose`: 詳細ログを表示

## API

| エンドポイント | 概要 |
|---|---|
| `GET /` | ダッシュボードHTMLを配信 |
| `POST /api/run` | スクリーニングジョブを投入（パラメータはJSONボディ） |
| `GET /api/status/<job_id>` | ジョブの進捗・ログ・部分結果を取得 |
| `GET /api/results/<job_id>` | 完了したジョブの最終結果を取得 |
| `GET /outputs/<filename>` | 生成されたCSVなどをダウンロード |

APIキーはクライアントから受け付けず、常にサーバー側の `EDINET_API_KEY` 環境変数を使用します。

## 注意事項

- `outputs/` にはXBRLキャッシュや過去の実行結果が蓄積されます。容量が大きくなるため定期的に整理してください。
- `.env` に実際のAPIキーを書き込んだ場合、絶対にコミットしないでください（`.gitignore` で除外済み）。
