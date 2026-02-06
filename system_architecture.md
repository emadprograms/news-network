# System Architecture

## Overview
The **Grandmaster News Network** is a real-time market intelligence dashboard built with Streamlit. It aggregates news from a Turso database and presents it in a high-density, trader-focused UI. Security is managed via Infisical to ensure database credentials are never hardcoded.

## High-Level Architecture

```mermaid
graph TD
    User[User / Trader] -->|HTTPS| App[Streamlit App (app.py)]
    
    subgraph "Application Layer"
        App -->|UI Rendering| Frontend[Streamlit Frontend]
        App -->|Data Fetching| DBClient[modules.db_client]
        App -->|Secret Injection| Infisical[Infisical SDK]
    end
    
    subgraph "Infrastructure"
        Infisical -->|Fetch Secrets| InfisicalCloud[Infisical Cloud]
        DBClient -->|Query Data| Turso[Turso Database (LibSQL)]
    end
    
    InfisicalCloud -.->|Credentials (URL, Token)| App
    Turso -.->|News Items| App
```

## Key Components

### 1. Frontend (Streamlit)
- **Entry Point**: `app.py`
- **UI Elements**: 
    - **Control Panel**: Expandable section for Time Travel filters and Export tools.
    - **News Ticker**: CSS-animated scrolling headline bar.
    - **Tabs**: Categorized views for "Global Headlines", "Stocks News", and "Company News".
- **Styling**: Custom CSS injection for a "Dark Mode" financial terminal aesthetic.

### 2. Backend Logic
- **Database Client** (`modules/db_client.py`):
    - Wraps the `libsql_client`.
    - Handles connection initialization and error containment.
    - Provides specific methods like `fetch_news_range` and `fetch_news_by_date`.
- **Secret Management**:
    - **Provider**: Infisical.
    - **Integration**: `infisicalsdk` is initialized at startup.
    - **Workflow**: The app authenticates with Infisical using a Client ID/Secret from `secrets.toml`, then retrieves the production Database URL and Auth Token dynamically. The app cannot connect to the database without this successful handshake.

### 3. Data Storage (Turso)
- **Database**: LibSQL (SQLite compatible) hosted on Turso.
- **Data Model**: Stores news articles with timestamps, categories, and source metadata.

## Security Model
- **Zero Hardcoded Secrets**: Database credentials do not exist in the codebase or standard config files.
- **Identity**: The application uses a Universal Auth identity to authenticate with Infisical.
- **Environment**: Secrets are fetched for the specific environment (e.g., `dev`) defined in the code.

## Deployment
- The application is stateless and can be deployed on any containerized environment (Docker) or PaaS (Streamlit Cloud, Heroku) that supports Python 3.12+.
- **Requirements**: `requirements.txt` includes all necessary packages (`streamlit`, `infisicalsdk`, `libsql-client`).
