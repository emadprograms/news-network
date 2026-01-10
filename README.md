# Grandmaster News Network

 **GRANDMASTER NEWS NETWORK** is a Streamlit-based intelligence feed designed for aggregating and analyzing global news, stocks, and company-specific reports.

## Features

- **Live Intelligence Ticker**: Real-time scrolling headlines.
- **Time Machine**: Filter news by specific date and time ranges.
- **Categorized Feeds**: Dedicated tabs for Global/Macro, Stocks, and Company news.
- **Export Reports**: Generate and download text reports for AI analysis.
- **Rich UI**: High-contrast, "Bloomberg-terminal" style aesthetics.

## Setup

### Prerequisites
- Python 3.12+
- Turso Database Credentials (in `.streamlit/secrets.toml`)

### Installation

1. **Create and activate the virtual environment**:
    ```bash
    python3.12 -m venv venv
    source venv/bin/activate
    ```

2. **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

### Configuration
Ensure your `.streamlit/secrets.toml` contains the necessary Turso database credentials:
```toml
[turso_news]
db_url = "libsql://..."
auth_token = "..."
```

### Running the App

```bash
streamlit run app.py
```

## Project Structure

- `app.py`: Main application entry point.
- `modules/`: Helper modules (Database client, etc.).
- `.streamlit/`: Streamlit configuration.
- `requirements.txt`: Python dependencies.
