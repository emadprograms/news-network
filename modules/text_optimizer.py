import json
from collections import defaultdict

def optimize_json_for_synthesis(json_data: list) -> str:
    """
    Groups news items by Entity and formats them into a dense, token-optimized plaintext.
    
    Args:
        json_data (list): List of news item dictionaries.
        
    Returns:
        str: A single string containing the optimized text.
    """
    if not json_data:
        return "No data available."

    # 1. Group by Primary Entity
    entity_groups = defaultdict(list)
    for item in json_data:
        # Fallback to 'Unknown Entity' if key is missing/null
        entity = item.get('primary_entity') or "Unknown Entity"
        entity_groups[entity].append(item)

    optimized_text_lines = []

    # 2. Iterate through groups and format
    # Sort entities alphabetically for consistent output
    for entity in sorted(entity_groups.keys()):
        items = entity_groups[entity]
        
        # Determine the entity type and sector from the items
        # Usually it's consistent across the items in the group, so we grab from the first
        first_item = items[0]
        entity_type = (first_item.get('entity_type') or 'COMPANY').upper()
        sector = first_item.get('sector')
        
        # Construct the prefix
        if entity_type == 'MACRO':
            prefix = "[MACRO] "
        else:
            if sector and sector.lower() != 'null':
                prefix = f"[COMPANY] [SECTOR:{sector}] "
            else:
                prefix = "[COMPANY] "
                
        # Header for the Entity
        optimized_text_lines.append(f"ENTITY: {prefix}{entity}")
        
        for i, item in enumerate(items, 1):
            category = (item.get('category') or 'GENERAL').upper()
            summary = item.get('event_summary') or 'No summary.'
            
            # Formatting Hard Data: Flatten dictionary to "Key=Value, Key=Value"
            hard_data = item.get('hard_data') or {}
            hard_data_str = ""
            if isinstance(hard_data, dict) and hard_data:
                # Filter out nulls and format
                hd_list = [f"{k}={v}" for k, v in hard_data.items() if v]
                if hd_list:
                    hard_data_str = f" Data: {', '.join(hd_list)}"

            # Formatting Quotes: Just take the first one or ignore to save tokens?
            # User guideline: "quotes (often token hogs; summarize or remove if not vital)"
            # Let's include a snippet of the first quote if present, but truncated.
            quotes = item.get('quotes', [])
            quote_str = ""
            if quotes and isinstance(quotes, list) and len(quotes) > 0:
                # Take first quote, max 100 chars
                q = str(quotes[0])
                if len(q) > 100:
                    q = q[:97] + "..."
                quote_str = f" Quote: \"{q}\""

            # Construct the dense line
            # Format: 1. [CATEGORY] Event Summary. Data: K=V, K=V. Quote: "..."
            line = f"{i}. [{category}] {summary}{hard_data_str}{quote_str}"
            optimized_text_lines.append(line)
        
        # Add a blank line between entities
        optimized_text_lines.append("")

    return "\n".join(optimized_text_lines)
