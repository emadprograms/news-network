# System Architecture

## Overview
The **News Network Analysis** is a high-fidelity intelligence engine designed to convert unstructured news data into structured, machine-readable JSON datasets. It uses a Map-Reduce Map-Reduce strategy with Google's Gemini API to process high volumes of news text, implementing "Relentless Key Rotation" to bypass rate limits.

## High-Level Architecture

```mermaid
graph TD
    User[User / Analyst] -->|HTTPS| App[Streamlit App (app.py)]
    
    subgraph "Intelligence Layer (AI ETL)"
        App -->|Chunked Text| LLM[modules.llm_client]
        LLM -->|Request| Gemini[Gemini 1.5/2.0 API]
        App -->|Key Request| KM[modules.key_manager]
        KM -->|Success/Failure Report| KM
    end
    
    subgraph "Infrastructure"
        App -->|Secret Injection| Infisical[Infisical SDK]
        App -->|Query Headlines| NewsDB[Turso: NewsDatabase]
        KM -->|Manage Quotas| KeyDB[Turso: Analyst Workbench]
    end
    
    Infisical -.->|Credentials| App
    NewsDB -.->|Raw News| App
    Gemini -.->|Structured JSON| App
```

## Key Components

### 1. AI Extraction Engine (Map-Reduce)
- **Map Phase**: High-volume news data is split into ~220k token chunks. Each chunk is processed by the AI with a **Strict ETL Prompt** to extract discrete facts, metrics, and entities.
- **Aggregation Phase**: Extracted JSON objects are recovered (via multi-layered regex repair), aggregated into a master list, and presented as a high-fidelity report.

### 2. Relentless Key Rotation (`KeyManager`)
- **Multi-Database Handshake**: The system connects to two Turso databases. One for news content, and the **Analyst Workbench** for managing a pool of AI API keys.
- **Dynamic Rotation**: If an API key hits a rate limit (429) or transient error (500/503), the `KeyManager` instantly rotates to the next available priority key.
- **Reporting**: Success and failure signals are reported back to the database in real-time to manage quotas across sessions.

### 3. Frontend (Streamlit)
- **Analyst Control Panel**: Top-level form for time-travel, model selection, and execution mode (ETL vs Dry Run).
- **Instant Preview**: Immediately displays a full raw text backup (headline + body) the moment data is fetched, ensuring zero data loss if processing is interrupted.
- **Results Engine**: 
    - **Executive Summary Table**: A clean, spreadsheet-style view of extracted intelligence.
    - **Interactive JSON Tree**: Deep drill-down for programmatic data consumers.
    - **Copy-to-Clipboard**: Built-in 1-click copy for the entire JSON dataset.

## Security Model
- **Secret Management**: Powered by Infisical. All database URLs, Auth Tokens, and key identifiers are injected at runtime.
- **Zero Hardcoding**: Credentials for both the News and Management databases are never stored in the environment or codebase.

## Resilience Features
- **300s API Timeouts**: Optimized for heavy extraction tasks.
- **Robust JSON Recovery**: 5-layer recovery strategy (Markdown cleaning -> Brace search -> Whitespace stripping -> Missing comma injection -> Regex repair).
- **Safety Filter Detection**: Explicit monitoring of AI safety-block reasons to assist in ETL debugging.
