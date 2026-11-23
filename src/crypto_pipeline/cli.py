"""Ligne de commande : lancer les jobs sans Airflow (utile pour tester et déboguer).

    python -m crypto_pipeline.cli seed
    python -m crypto_pipeline.cli ingest-klines
    python -m crypto_pipeline.cli backfill --days 2
    python -m crypto_pipeline.cli check-coins
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from crypto_pipeline import ingest, quality
from crypto_pipeline.config import load_coins


def check_coins() -> int:
    """Vérifie que chaque crypto de coins.yaml existe bien chez Binance ET CoinGecko."""
    from crypto_pipeline.clients.binance import BinanceClient
    from crypto_pipeline.clients.coingecko import CoinGeckoClient

    coins = load_coins()
    binance = BinanceClient().exchange_symbols()
    gecko = CoinGeckoClient().all_coin_ids()
    bad = False
    for c in coins:
        problems = []
        if c.binance_symbol not in binance:
            problems.append(f"paire Binance {c.binance_symbol} introuvable")
        if c.coingecko_id not in gecko:
            problems.append(f"id CoinGecko '{c.coingecko_id}' introuvable")
        if problems:
            bad = True
            print(f"❌ {c.symbol}: {', '.join(problems)}")
    print("✅ Tous les identifiants sont valides" if not bad else "→ corrigez config/coins.yaml")
    return 1 if bad else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="crypto_pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("seed", help="charger le référentiel des cryptos")
    sub.add_parser("ingest-klines", help="collecter les dernières bougies 1m")
    sub.add_parser("ingest-market", help="collecter les instantanés CoinGecko")
    sub.add_parser("ingest-social", help="collecter les métriques sociales")
    bf = sub.add_parser("backfill", help="charger l'historique")
    bf.add_argument("--days", type=int, default=1)
    sub.add_parser("aggregate", help="calculer les agrégats 5m/15m/1h/1d")
    ex = sub.add_parser("export-parquet", help="exporter un jour en Parquet")
    ex.add_argument("--day", type=date.fromisoformat, default=None, help="YYYY-MM-DD (défaut : hier)")
    sub.add_parser("quality", help="contrôles qualité")
    sub.add_parser("check-coins", help="valider coins.yaml auprès des APIs")
    args = parser.parse_args(argv)

    if args.cmd == "check-coins":
        return check_coins()
    result = {
        "seed": lambda: ingest.seed_assets(),
        "ingest-klines": lambda: ingest.ingest_klines(),
        "ingest-market": lambda: ingest.ingest_market(),
        "ingest-social": lambda: ingest.ingest_social(),
        "backfill": lambda: ingest.backfill_klines(days=args.days),
        "aggregate": lambda: ingest.aggregate(),
        "export-parquet": lambda: ingest.export_parquet(args.day),
        "quality": lambda: quality.check_klines(),
    }[args.cmd]()
    print(json.dumps(result, default=str, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
