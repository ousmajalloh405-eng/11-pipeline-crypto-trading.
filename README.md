# Pipeline de Données pour le Trading de Crypto

Pipeline d'ingénierie de données de bout en bout qui collecte **les bougies de 1 minute pour 50 cryptomonnaies** (Binance) et les **métriques de marché / sociales** (CoinGecko), orchestre le tout avec **Apache Airflow**, stocke les données dans **PostgreSQL + Parquet**, et les expose via une **API REST FastAPI** conçue pour le backtesting.

> Projet 11 sur [15 projets de données pour un portfolio professionnel](https://github.com/gpenessot/pimp-your-portfolio) — parcours Data Engineer.

## Problème métier

L'analyse des marchés crypto nécessite des données fiables, multi-sources, quasi temps réel pour des dizaines d'actifs, ainsi qu'un historique profond. Les scripts ad-hoc cassent silencieusement, laissent des trous et produisent des doublons. Ce projet les remplace par un pipeline qui **réessaie, ne duplique jamais, détecte ses propres trous et rapporte sa propre santé**.

## Architecture

```mermaid
flowchart LR
    A[API Binance<br/>bougies 1m] --> B
    C[API CoinGecko<br/>marché + social] --> B
    subgraph B[Orchestration Airflow]
      direction TB
      B1[binance_klines_1m<br/>chaque minute] --> B2[quality_check]
      B3[coingecko_market_5m]
      B4[coingecko_social_daily]
      B5[aggregate_hourly<br/>pandas]
      B6[parquet_export_daily]
      B7[backfill<br/>manuel]
    end
    B --> D[(PostgreSQL<br/>crypto.* / ops.*)]
    B6 --> E[(Parquet<br/>date=/symbol=)]
    D --> F[FastAPI<br/>REST + OpenAPI]
    F --> G[Clients de<br/>backtesting]# 11-pipeline-crypto-trading.
