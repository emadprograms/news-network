import os
import json
from infisical_sdk import InfisicalSDKClient

def inject():
    client_id = os.environ.get("INFISICAL_CLIENT_ID")
    client_secret = os.environ.get("INFISICAL_CLIENT_SECRET")
    project_id = os.environ.get("INFISICAL_PROJECT_ID")
    
    if not (client_id and client_secret and project_id):
        print("Missing Infisical project configurations in environment")
        return

    client = InfisicalSDKClient(host="https://app.infisical.com")
    client.auth.universal_auth.login(client_id=client_id, client_secret=client_secret)
    
    gemini_dir = os.path.expanduser("~/.gemini")
    os.makedirs(gemini_dir, exist_ok=True)
    
    secrets = {
        "gemini_settings_json": "settings.json",
        "gemini_oauth_creds_json": "oauth_creds.json"
    }
    
    for secret_name, filename in secrets.items():
        try:
            secret = client.secrets.get_secret_by_name(
                secret_name=secret_name,
                project_id=project_id,
                environment_slug="dev",
                secret_path="/"
            )
            
            with open(os.path.join(gemini_dir, filename), 'w') as f:
                f.write(secret.secretValue)
            print(f"✅ Successfully injected {filename}")
        except Exception as e:
            print(f"❌ Failed to fetch {secret_name}: {e}")

if __name__ == "__main__":
    inject()
