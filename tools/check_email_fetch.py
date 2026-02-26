import datetime
import os
import sys
import re
import imaplib
from email import message_from_bytes
import quopri
from infisical_sdk import InfisicalSDKClient
from dotenv import load_dotenv

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def fetch_stock_analysis_email(gmail_user, gmail_pass, session_date):
    print(f"📧 Searching for Stock Analysis email for session: {session_date}...")
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(gmail_user, gmail_pass)
        mail.select("inbox")

        search_date = session_date.strftime("%d-%b-%Y")
        search_query = f'(FROM "contact@stockanalysis.com" ON {search_date})'
        status, data = mail.search(None, search_query)
        
        if status != 'OK' or not data[0]:
            msg = f"No email from contact@stockanalysis.com found for {search_date}."
            return None, msg

        mail_ids = data[0].split()
        latest_id = mail_ids[-1]
        status, msg_data = mail.fetch(latest_id, '(RFC822)')
        
        if status != 'OK':
            return None, "Failed to fetch email content."

        raw_email = msg_data[0][1]
        msg_obj = message_from_bytes(raw_email)
        
        body = ""
        if msg_obj.is_multipart():
            for part in msg_obj.walk():
                content_type = part.get_content_type()
                if content_type == "text/plain":
                    body = part.get_payload(decode=True).decode(errors='ignore')
                    break
                elif content_type == "text/html":
                    body = part.get_payload(decode=True).decode(errors='ignore')
        else:
            body = msg_obj.get_payload(decode=True).decode(errors='ignore')

        if "<body" in body or "<div" in body:
            body = re.sub(r'<style.*?>.*?</style>', '', body, flags=re.DOTALL)
            body = re.sub(r'<script.*?>.*?</script>', '', body, flags=re.DOTALL)
            body = re.sub(r'<[^>]+>', '\n', body)
            body = re.sub(r'\n\s*\n', '\n', body).strip()
            try:
                body = quopri.decodestring(body).decode('utf-8', errors='ignore')
            except:
                pass

        mail.logout()
        return body, "✅ Success"
    except Exception as e:
        return None, f"Error: {str(e)}"

def main():
    load_dotenv()
    infisical = InfisicalSDKClient(host="https://app.infisical.com")
    inf_client_id = os.environ.get("INFISICAL_CLIENT_ID")
    inf_client_secret = os.environ.get("INFISICAL_CLIENT_SECRET")
    inf_project_id = os.environ.get("INFISICAL_PROJECT_ID")
    
    if not (inf_client_id and inf_client_secret and inf_project_id):
        print("❌ Error: Missing Infisical Credentials.")
        return

    infisical.auth.universal_auth.login(client_id=inf_client_id, client_secret=inf_client_secret)
    gmail_user = infisical.secrets.get_secret_by_name(secret_name="arshademad_gmail_address", project_id=inf_project_id, environment_slug="dev", secret_path="/").secretValue
    gmail_pass = infisical.secrets.get_secret_by_name(secret_name="google_news_network_app_password", project_id=inf_project_id, environment_slug="dev", secret_path="/").secretValue
    
    target_date = datetime.date(2026, 2, 13)
    content, status = fetch_stock_analysis_email(gmail_user, gmail_pass, target_date)
    
    if content:
        output_file = "last_email_check.txt"
        with open(output_file, "w") as f:
            f.write(f"EXTRACTED CONTENT FOR {target_date}:\n")
            f.write("="*40 + "\n\n")
            f.write(content)
        print(f"\n✅ Content saved to: {output_file}")
    else:
        print(f"\n❌ FAILED: {status}")

if __name__ == "__main__":
    main()
