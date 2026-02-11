import time
import requests
import logging
from modules.key_manager import KeyManager

log = logging.getLogger(__name__)

class GeminiClient:
    """
    Client for Google Gemini API using KeyManager for rate limiting and rotation.
    """
    def __init__(self, key_manager: KeyManager):
        self.key_manager = key_manager

    def generate_content(self, prompt: str, config_id: str = 'gemini-3-flash-free') -> dict:
        """
        Generates content using the specified model configuration.
        
        Returns:
            dict: {
                "success": bool,
                "content": str (if success) or error message (if fail),
                "model_used": str,
                "key_name": str
            }
        """
        # 1. Estimate Tokens
        est_tokens = self.key_manager.estimate_tokens(prompt)
        
        # 2. Get Key
        key_name, key_value, wait_time, model_id = self.key_manager.get_key(config_id, est_tokens)
        
        if wait_time == -1.0:
            return {
                "success": False, 
                "content": f"Request too large for {config_id} (Est. {est_tokens} tokens).",
                "model_used": model_id,
                "key_name": "N/A"
            }
            
        if wait_time > 0:
            return {
                "success": False,
                "content": f"Rate limit reached. Please wait {int(wait_time)} seconds.",
                "wait_seconds": wait_time,
                "model_used": model_id,
                "key_name": "N/A"
            }
            
        if not key_value:
             return {
                "success": False,
                "content": f"No API keys available for config '{config_id}'. Check Key Tier.",
                "model_used": model_id,
                "key_name": "N/A"
            }

        # 3. Execute Request
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_id}:generateContent?key={key_value}"
        headers = {'Content-Type': 'application/json'}
        payload = {
            "contents": [{
                "parts": [{"text": prompt}]
            }]
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=300)
            
            if response.status_code == 200:
                data = response.json()
                # Parse content
                try:
                    text_content = data['candidates'][0]['content']['parts'][0]['text']
                    
                    # Report Success
                    # Estimate output tokens roughly (1 char ~ 0.25 tokens)
                    out_tokens = int(len(text_content) * 0.25)
                    total_tokens = est_tokens + out_tokens
                    self.key_manager.report_usage(key_value, total_tokens, model_id)
                    
                    return {
                        "success": True,
                        "content": text_content,
                        "model_used": model_id,
                        "key_name": key_name
                    }
                except (KeyError, IndexError):
                    return {
                        "success": False,
                        "content": "Failed to parse API response structure.",
                        "model_used": model_id,
                        "key_name": key_name
                    }
            
            elif response.status_code == 429:
                self.key_manager.report_failure(key_value)
                return {
                    "success": False,
                    "content": "Rate limit exceeded (429). Key rotated.",
                    "model_used": model_id,
                    "key_name": key_name
                }
            else:
                 # Other errors (400, 500)
                 return {
                    "success": False,
                    "content": f"API Error {response.status_code}: {response.text}",
                    "model_used": model_id,
                    "key_name": key_name
                }
                
        except Exception as e:
            return {
                "success": False,
                "content": f"Connection Error: {str(e)}",
                "model_used": model_id,
                "key_name": key_name
            }
