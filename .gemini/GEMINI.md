# News Network Analysis Engine

## Project Overview

**News Network** is a high-fidelity intelligence feed and analysis engine designed for aggregating, distilling, and analyzing global news, stocks, and macro-economic reports. It processes massive volumes of unstructured news into structured, entity-grouped JSON insights using a custom Map-Reduce extraction strategy with Google's Gemini API.

### High-Level Architecture

```mermaid
graph TD
    User[User / Analyst] -->|HTTPS| App[Streamlit App (streamlit_app.py)]
    
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

**Core Technologies:**
- **Language**: Python 3.12+
- **AI/LLM**: Gemini API with "Relentless Key Rotation" and "High-Yield Enforcement."
- **Database**: Turso (LibSQL) for news (`NewsDatabase`) and quota management (`KeyManager`).
- **Secret Management**: Infisical SDK for runtime secret injection.

## Key Components & Strategies

### 1. High-Yield Data Extraction
- **Zero-Loss Slicing**: Large news items are sliced into multiple parts rather than truncated, ensuring complete processing.
- **Headline-Based Yield Enforcement**: Yield is calculated based on **Headlines Recovered** (via token-overlap matching) rather than raw item count.
- **Strict 1:1 Extraction**: The AI is constrained to create exactly **one JSON object per source headline**, preventing data loss through summarization.
- **Targeted Residual Extraction**: Instead of retrying an entire failed chunk, the system salvages successful items and identifies missing residue using a **Token-Overlap** algorithm (85% similarity) for targeted retries.
- **Robust Salvaging**: Uses a **Balanced-Brace Scanner** to recover complex JSON objects from malformed responses.
- **Super-Fallback Recovery**: If all retries fail, the system hunts the raw buffer for any valid individual items.

### 2. Intelligence Orchestration (`KeyManager` & `GeminiClient`)
- **Fatal Error Detection**: Automatically detects and bans "Expired" or "Invalid" API keys instantly.
- **Model Isolation**: Tracks rate limits independently for each model across Free and Paid tiers.
- **Parallel ETL Pipeline**: Uses `ThreadPoolExecutor` to process up to 15 chunks concurrently while managing per-key rate limits.

### 3. User Interfaces
- **Streamlit App** (`streamlit_app.py`): Minimalist, "Bloomberg-terminal" style UI focusing on a **Time Window** and a **Model** selector, providing optimized token-light outputs for synthesis.
- **Discord Bot** (`discord_bot/bot.py`): Remote trigger via GitHub Actions for serverless, heavy-duty processing.
- **CLI** (`main.py`): Core engine entry point for scheduled or manual extractions.

## Building and Running

### Prerequisites
- Python 3.12+
- Access to an Infisical project containing the required secrets.
- Turso Database setup.

### Setup Environment
1. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```
2. **Install core dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Install bot dependencies:**
   ```bash
   pip install -r discord_bot/requirements.txt
   ```

### Running the Components

**1. Streamlit Application:**
```bash
streamlit run streamlit_app.py
```

**2. Core Extraction Engine (CLI):**
```bash
python main.py --date 2026-02-18 --model gemini-2.5-flash-lite-free
```

**3. Discord Bot:**
```bash
python discord_bot/bot.py
```

### Testing
```bash
pytest tests/
```

## Development Conventions

- **Modular Design**: Business logic is abstracted into `modules/`.
- **Relentless Fidelity**: Always preserve the self-healing and yield enforcement architectures when modifying the AI pipeline.
- **Secret Security**: All sensitive information must be fetched dynamically at runtime via the `InfisicalSDKClient`. No local persistence of keys.
- **Infrastructure as Code**: Offload heavy processing to GitHub Actions (`.github/workflows/manual_run.yml`).
