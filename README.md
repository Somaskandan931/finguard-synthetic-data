# FinGuard Synthetic Dataset Generator

This repository contains a Python-based framework for generating realistic synthetic financial transaction data and AML watchlists, designed for machine learning experiments in fraud detection and anti-money laundering (AML) systems. It supports UPI-like transaction simulations with configurable fraud injection, and a simulated AML watchlist with fuzzy name variations.

## Overview

The synthetic dataset includes:
- 100,000 financial transactions among 3,000 simulated users
- Engineered features such as transaction velocity, merchant categories, channel-specific limits, device usage, and behavioral indicators
- Fraudulent transactions generated using multiple fraud strategies: structuring, round-tripping, device anomaly, and high-value spikes
- An AML watchlist of 1,000 high-risk individuals and organizations with aliases, risk scores, and transliterated name variations

This generator is designed to support research and model development for fraud detection in digital payment systems, especially in the Indian UPI context.

---

## Features

### Synthetic Transaction Generator (`synthetic_transaction_data.py`)
- Generates normal and fraudulent transactions
- Simulates realistic transaction behavior using `Faker`
- Includes engineered features:
  - Amount-to-average ratio
  - Transaction frequency (hour/day/week)
  - Risk profile
  - New recipient anomaly
  - Channel-specific limits

### AML Watchlist Generator (`aml_watchlist_generator.py`)
- Generates a list of high-risk entities with metadata (e.g., DOB, aliases, sanctions lists)
- Simulates fuzzy name variations for testing name screening and matching
- Includes risk categories like terrorism financing, money laundering, and tax evasion
