import os
import json
from infisical_sdk import InfisicalSDKClient
from dotenv import load_dotenv

def upload():
    load_dotenv()
    
    client_id = os.environ.get("INFISICAL_CLIENT_ID")
    client_secret = os.environ.get("INFISICAL_CLIENT_SECRET")
    project_id = os.environ.get("INFISICAL_PROJECT_ID")
    
    if not (client_id and client_secret and project_id):
        print("Missing Infisical project configurations in .env")
        return

    client = InfisicalSDKClient(host="https://app.infisical.com")
    client.auth.universal_auth.login(client_id=client_id, client_secret=client_secret)
    
    # Files to upload
    gemini_dir = os.path.expanduser("~/.gemini")
    files = {
        "gemini_settings_json": os.path.join(gemini_dir, "settings.json"),
        "gemini_oauth_creds_json": os.path.join(gemini_dir, "oauth_creds.json")
    }
    
    for secret_name, file_path in files.items():
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()
                
            try:
                # Try to create, if exists it will fail or we can catch it
                # The SDK might have a specific upsert but let's try get/create/update logic
                try:
                    client.secrets.get_secret_by_name(
                        secret_name=secret_name,
                        project_id=project_id,
                        environment_slug="dev",
                        secret_path="/"
                    )
                    # Exists, so update
                    client.secrets.update_secret_by_name(
                        secret_name=secret_name,
                        project_id=project_id,
                        environment_slug="dev",
                        secret_path="/",
                        secret_value=content
                    )
                    print(f"Updated {secret_name} in Infisical")
                except:
                    # Doesn't exist, create
                    client.secrets.create_secret_by_name(
                        secret_name=secret_name,
                        project_id=project_id,
                        environment_slug="dev",
                        secret_path="/",
                        secret_value=content
                    )
                    print(f"Created {secret_name} in Infisical")
            except Exception as e:
                print(f"Error handling {secret_name}: {e}")
        else:
            print(f"File not found: {file_path}")

if __name__ == "__main__":
    upload()
