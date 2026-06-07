# Project Report: Acme Ltd Financial Data Warehouse

## 1. System Architecture & Tech Stack
* **Framework:** FastAPI (Python) - Provides high-performance RESTful routing endpoints.
* **Database:** MongoDB - NoSQL storage choice perfectly suited for handling highly heterogeneous financial payloads (stocks vs crypto dynamic attributes).
* **AI Engine:** Model Model Context Protocol (MCP) - Standardized integration layer mapping platform tools to LLM interfaces.

## 2. Core Functional Implementations
* **UC 1 (Ingestion & Provenance):** Built automated REST handlers pulling from external market APIs (CoinGecko), mapping data streams into structured collection tables tagged with absolute lineage providers (`dataSourceId`).
* **UC 2 (Consumption API):** Handled standard financial asset exploration via 5 specific target endpoints matching requirements Q1 through Q5.
* **UC 3 (Analytics Calculation):** Developed MongoDB aggregation pipeline operators returning clean statistical profiles (min, max, average) for trading analytics workflows.
* **UC 4 & Bonus (Agentic AI):** Integrated a localized MCP background communication network. This environment enables external conversational LLMs to intelligently plan multi-step data extractions over live database metrics.

## 3. Non-Functional Temporal Verification
The warehouse architecture enforces strict append-only operations. Document updates do not rewrite lines in place; instead, they gracefully sunset older variants utilizing timestamp parameters (`valid_from` / `valid_to`), ensuring perfect historical replay tracking capabilities.