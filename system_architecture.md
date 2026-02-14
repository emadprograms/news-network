# System Architecture

## Overview
The **News Network Analysis** is a high-fidelity intelligence engine designed to convert massive volumes of unstructured news into distilled, entity-grouped insights. It uses a Map-Reduce strategy with Google's Gemini API, implementing "Relentless Key Rotation" and "High-Yield Enforcement" to ensure maximum data fidelity with zero manual oversight.

## High-Level Architecture

```mermaid
graph TD
    User[User / Analyst] -->|HTTPS| App[Streamlit App (app.py)]
    
    subgraph "High-Fidelity Intelligence Engine"
        App -->|Chunked Text| LLM[modules.llm_client]
        LLM -->|Fatal Error Check| KM[modules.key_manager]
        App -->|Yield Validation| App
        App -->|Token Distillation| TO[modules.text_optimizer]
    end
    
    subgraph "Infrastructure"
        App -->|Secret Injection| Infisical[Infisical SDK]
        App -->|Query Headlines| NewsDB[Turso: NewsDatabase]
        KM -->|Manage Quotas| KeyDB[Turso: Analyst Workbench]
    end
```

## Key Components

### 1. Minimalist AI Workflow
- **Two-Step Control**: The interface is stripped of all "fluff." Users only select a **Time Window** and a **Model**. All complex configurations are handled automatically by the engine.
- **Pure Optimized Output**: Instead of many UI tabs, the application outputs a single **Optimized Token-Light Input** block, ready for 1-click copy into a manual synthesis model.

### 2. High-Yield Data Extraction
- **Zero-Loss Slicing**: Large news items are sliced into multiple parts rather than truncated, ensuring every sentence is processed.
- **Headline-Based Yield Enforcement**: Yield is calculated based on **Headlines Recovered** (via token-overlap matching) rather than raw item count. This ensures that even if the AI merges stories (which is strictly forbidden), the system correctly identifies missing data.
- **Strict 1:1 Extraction**: The AI is constrained to create exactly **one JSON object per source headline**, preventing data loss through summarization.
- **Targeted Residual Extraction**: Instead of retrying an entire failed chunk, the system salvages successful items and identifies the "missing residue" using a robust **Token-Overlap** algorithm (85% similarity). Only the truly missing items are retried.
- **Robust Salvaging**: Uses a **Balanced-Brace Scanner** to recover complex, nested JSON objects from malformed responses, ensuring that valid data is never discarded due to syntax errors.

### 3. Intelligence Orchestration (`KeyManager` & `GeminiClient`)
- **Fatal Error Detection**: Automatically detects "Expired" or "Invalid" API keys and bans them from rotation instantly.
- **Model Isolation**: Tracks rate limits independently for each model (Flash, Pro, Lite) across both Free and Paid tiers.
- **Data Distillation Metric**: Calculates the exact "compression" ratio by comparing the total token weight of the **Raw News Input** to the final **Optimized Output**.

## Security & Resilience
- **Secret Management**: Powered by Infisical. All DB and API keys are injected at runtime with zero local persistence.
- **Parallel ETL Pipeline**: Uses `ThreadPoolExecutor` to process up to 15 chunks concurrently, maximizing throughput while managing per-key rate limits.
- **Super-Fallback Recovery**: If a chunk fails all 5 retries, the system hunts the raw buffer for any valid individual items to prevent total data loss.
