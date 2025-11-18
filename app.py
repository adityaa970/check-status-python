import requests
from datetime import datetime
import json
from flask import Flask, jsonify
import time
import re
import os
from supabase import create_client, Client
from typing import Dict, Any, Optional

app = Flask(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL") or "https://cuqnpzoldeiztgezwqqe.supabase.co"
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN1cW5wem9sZGVpenRnZXp3cXFlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MTMwOTM3OSwiZXhwIjoyMDc2ODg1Mzc5fQ.Ltm4bRsX8UBOG5XuXLEciniGohs1Bvz6iHoPJqT8k8I"

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise ValueError("Missing Supabase environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def sanitize_string(s: str) -> str:
    import urllib.parse
    
    if not s:
        return ''
    
    has_non_english = bool(re.search(r'[^\x00-\x7F]', s))
    
    if has_non_english:
        return urllib.parse.quote(s, safe='')
    else:
        result = s.lower()
        result = re.sub(r'\s+', '-', result)
        result = re.sub(r'[^\w\s-]', '-', result)
        result = re.sub(r'-+', '-', result)
        return result.strip('-')

def fetch_beta_availability(url: str) -> str:
    try:
        # Reduced timeout and smaller buffer to save memory
        response = requests.get(url, timeout=3, stream=True)
        response.raise_for_status()
        response.encoding = 'utf-8'
        
        # Read only first 10KB to save memory
        content_chunks = []
        total_size = 0
        max_size = 10240  # 10KB limit
        
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                content_chunks.append(chunk.decode('utf-8', errors='ignore'))
                total_size += len(chunk)
                if total_size > max_size:
                    break
        
        response.close()
        content = ''.join(content_chunks).lower()
        
        # Clear chunks from memory
        del content_chunks
        
        if "this beta is full" in content:
            return 'full'
        if "this beta isn't accepting any new testers right now" in content:
            return 'not accepting'
        if "join the" in content or "start testing" in content:
            return 'open'
        return 'unknown'
        
    except requests.exceptions.Timeout:
        return 'timeout'
    except requests.exceptions.RequestException as e:
        return 'error'

def get_or_create_app(app_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sanitized_name = sanitize_string(app_data['name'])
    
    try:
        result = supabase.table('apps').select('*').eq('sanitizedName', sanitized_name).execute()
        
        if result.data:
            return result.data[0]
        
        new_app = {
            'name': app_data['name'],
            'sanitizedName': sanitized_name,
            'link': app_data.get('link', ''),
            'logo': app_data.get('logo', ''),
            'appType': app_data.get('appType', 'Beta'),
            'betaAvailable': app_data.get('betaAvailable', 'unknown'),
            'clickCount': app_data.get('clickCount', 0),
            'description': app_data.get('description', ''),
            'categories': app_data.get('categories', []),
            'appStore': app_data.get('appStore', ''),
            'screenshotUrls': app_data.get('screenshotUrls', []),
            'features': app_data.get('features', []),
            'artistViewUrl': app_data.get('artistViewUrl', ''),
            'trackContentRating': app_data.get('trackContentRating', ''),
            'primaryGenreName': app_data.get('primaryGenreName', ''),
            'sellerName': app_data.get('sellerName', ''),
            'artworkUrl100': app_data.get('artworkUrl100', ''),
            'lastChecked': datetime.utcnow().isoformat()
        }
        
        result = supabase.table('apps').insert(new_app).execute()
        return result.data[0] if result.data else None
        
    except Exception as e:
        return None

def update_app_status(app_data: Dict[str, Any]) -> bool:
    sanitized_name = sanitize_string(app_data['name'])
    
    try:
        result = supabase.table('apps').select('*').eq('sanitizedName', sanitized_name).execute()
        
        if not result.data:
            get_or_create_app(app_data)
            return True
        
        current_app = result.data[0]
        
        if (current_app.get('clickCount', 0) == app_data['clickCount'] and 
            current_app.get('betaAvailable', 'unknown') == app_data['betaAvailable']):
            return False
        
        update_data = {
            'betaAvailable': app_data['betaAvailable'],
            'clickCount': app_data['clickCount'],
            'link': app_data['link'],
            'logo': app_data['logo'],
            'lastChecked': datetime.utcnow().isoformat()
        }
        
        supabase.table('apps').update(update_data).eq('sanitizedName', sanitized_name).execute()
        
        history_entry = {
            'appId': current_app['id'],
            'status': app_data['betaAvailable'],
            'clickCount': app_data['clickCount'],
            'timestamp': datetime.utcnow().isoformat()
        }
        
        supabase.table('app_history').insert(history_entry).execute()
        
        history_result = supabase.table('app_history')\
            .select('id')\
            .eq('appId', current_app['id'])\
            .order('timestamp', desc=True)\
            .execute()
        
        if len(history_result.data) > 30:
            old_ids = [item['id'] for item in history_result.data[30:]]
            if old_ids:
                supabase.table('app_history').delete().in_('id', old_ids).execute()
        
        return True
        
    except Exception as e:
        return False

def get_user_interactions() -> Dict[str, int]:
    try:
        # Use smaller chunks and limit results to save memory
        result = supabase.table('user_interactions')\
            .select('sanitizedName, clickCount')\
            .limit(1000)\
            .execute()
        
        interactions = {}
        for item in result.data:
            interactions[item['sanitizedName']] = item['clickCount']
        
        # Clear result from memory
        del result
        
        return interactions
    except Exception as e:
        return {}

def debug_app_matching(json_url: str) -> Dict[str, Any]:
    try:
        response = requests.get(json_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        apps = data[0]['apps']
        
        user_interactions = get_user_interactions()
        
        json_apps = {}
        for app in apps:
            sanitized = sanitize_string(app['name'])
            json_apps[sanitized] = app['name']
        
        matches = []
        missing = []
        
        for tracked_name, clicks in user_interactions.items():
            if tracked_name in json_apps:
                matches.append((tracked_name, json_apps[tracked_name], clicks))
            else:
                missing.append((tracked_name, clicks))
        
        return {
            "matches": len(matches),
            "missing": len(missing),
            "total_tracked": len(user_interactions),
            "total_json": len(apps),
            "match_details": matches,
            "missing_details": missing
        }
        
    except Exception as e:
        return {"error": str(e)}

def get_processing_index(counter_key: str) -> int:
    try:
        result = supabase.table('processing_indexes').select('lastChecked').eq('counterKey', counter_key).execute()
        return result.data[0]['lastChecked'] if result.data else 0
    except Exception as e:
        return 0

def update_processing_index(counter_key: str, last_checked: int) -> None:
    try:
        result = supabase.table('processing_indexes')\
            .update({'lastChecked': last_checked})\
            .eq('counterKey', counter_key)\
            .execute()
        
        if not result.data:
            supabase.table('processing_indexes').insert({
                'counterKey': counter_key,
                'lastChecked': last_checked
            }).execute()
            
    except Exception as e:
        pass

def process_apps_from_json(json_url: str, click_threshold: int, counter_key: str, max_apps_to_process: int = 1) -> Dict[str, Any]:
    try:
        # Reduce timeout to prevent memory buildup
        response = requests.get(json_url, timeout=10, stream=True)
        response.raise_for_status()
        
        # Parse JSON in smaller chunks to save memory
        data = response.json()
        
        # Clear response from memory immediately
        response.close()
        del response

        if not data or 'apps' not in data[0]:
            raise Exception('No app data found to process.')

        # Get user interactions with limit to reduce memory
        user_interactions = get_user_interactions()
        
        if not user_interactions:
            return {"error": "No user interactions found"}
        
        start_index = get_processing_index(counter_key)
        
        total_apps = len(data[0]['apps'])
        count = 0
        checked = 0
        apps_below_threshold = 0

        # Process only a small number of apps to stay within memory limits
        max_check_limit = min(max_apps_to_process * 10, 20)  # Max 20 apps checked
        
        while count < max_apps_to_process and checked < max_check_limit and checked < total_apps:
            app_index = (start_index + checked) % total_apps
            app = data[0]['apps'][app_index].copy()  # Make a copy to avoid modifying original
            sanitized_app_name = sanitize_string(app['name'])
            app['clickCount'] = user_interactions.get(sanitized_app_name, 0)

            if app['clickCount'] >= click_threshold:
                app['betaAvailable'] = fetch_beta_availability(app['link'])
                if update_app_status(app):
                    count += 1
                
                # Force garbage collection after processing each app
                import gc
                gc.collect()
                
                # Longer delay to prevent memory buildup
                time.sleep(1.0)
            else:
                apps_below_threshold += 1

            checked += 1
            
            # Clear app data from memory
            del app

        # Clear large data structures from memory
        del data
        del user_interactions
        
        # Force garbage collection
        import gc
        gc.collect()

        new_last_checked_index = (start_index + checked) % total_apps
        update_processing_index(counter_key, new_last_checked_index)

        return {"message": f"Processed {count} apps for {counter_key}.", "details": {
            "checked": checked,
            "processed": count,
            "below_threshold": apps_below_threshold,
            "click_threshold": click_threshold
        }}
        
    except Exception as e:
        # Force garbage collection on error
        import gc
        gc.collect()
        return {"error": str(e)}

def parse_markdown(markdown_content: str) -> list:
    app_pattern = r"\*\*(.*?)\*\*:.*?\[!\[App Logo\]\((.*?)\)\]\((.*?)\)"
    matches = re.findall(app_pattern, markdown_content)
    return [{"name": match[0], "logo": match[1], "link": match[2]} for match in matches]

def process_apps(file_url: str, click_threshold: int, counter_key: str) -> Dict[str, Any]:
    try:
        response = requests.get(file_url, timeout=30)
        if not response.text:
            raise Exception('Failed to fetch Markdown content from GitHub')

        markdown_content = response.text
        data = parse_markdown(markdown_content)

        if not data:
            raise Exception('No app data found to process.')

        user_interactions = get_user_interactions()
        last_checked = get_processing_index(counter_key)
        start_index = last_checked
        count = 0

        for i in range(start_index, start_index + 20):
            app_index = i % len(data)
            app = data[app_index]
            sanitized_app_name = sanitize_string(app['name'])
            app['clickCount'] = user_interactions.get(sanitized_app_name, 0)

            if app['clickCount'] >= click_threshold:
                app['betaAvailable'] = fetch_beta_availability(app['link'])
                if update_app_status(app):
                    count += 1

        new_last_checked_index = (start_index + 20) % len(data)
        update_processing_index(counter_key, new_last_checked_index)

        return {"message": f"App statuses updated successfully for {counter_key}."}
        
    except Exception as e:
        return {"error": str(e)}

@app.route('/check_apps', methods=['GET'])
def check_apps():
    json_file = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json'
    result = process_apps_from_json(json_file, click_threshold=50, counter_key='lastChecked_check_apps', max_apps_to_process=3)
    return jsonify(result)

@app.route('/check_all', methods=['GET'])
def check_all():
    json_file = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json'
    result = process_apps_from_json(json_file, click_threshold=0, counter_key='lastChecked_check_all', max_apps_to_process=3)
    return jsonify(result)

@app.route('/daily_stat', methods=['GET'])
def daily_stat():
    file_url = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/daily-next.md'
    result = process_apps(file_url, click_threshold=0, counter_key='lastChecked_dailyNext')
    return jsonify(result)

@app.route('/health', methods=['GET'])
def health():
    # Force garbage collection on health check
    import gc
    gc.collect()
    return jsonify({
        "status": "healthy", 
        "timestamp": datetime.utcnow().isoformat(),
        "memory_efficient": True,
        "render_optimized": True
    })

@app.route('/keep_alive', methods=['GET'])
def keep_alive():
    # Force garbage collection on keep alive
    import gc
    gc.collect()
    return jsonify({
        "status": "alive",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_strategy": "render_free_tier"
    })

@app.route('/quick_check', methods=['GET'])
def quick_check():
    json_file = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json'
    result = process_apps_from_json(json_file, click_threshold=10, counter_key='lastChecked_quick', max_apps_to_process=1)
    return jsonify(result)

@app.route('/debug_matching')
def debug_matching():
    json_url = "https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json"
    result = debug_app_matching(json_url)
    return jsonify(result)

@app.route('/add_real_app_for_testing/<app_name>')
def add_real_app_for_testing(app_name: str):
    try:
        json_url = "https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json"
        response = requests.get(json_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        apps = data[0]['apps']
        
        found_app = None
        for app in apps:
            if app['name'].lower() == app_name.lower():
                found_app = app
                break
        
        if not found_app:
            return jsonify({"error": f"App '{app_name}' not found in JSON"}), 404
        
        sanitized_name = sanitize_string(found_app['name'])
        
        try:
            result = supabase.table('user_interactions')\
                .update({'clickCount': 5})\
                .eq('sanitizedName', sanitized_name)\
                .execute()
            
            if not result.data:
                supabase.table('user_interactions').insert({
                    'sanitizedName': sanitized_name,
                    'clickCount': 5
                }).execute()
        except Exception as e:
            supabase.table('user_interactions')\
                .update({'clickCount': 5})\
                .eq('sanitizedName', sanitized_name)\
                .execute()
        
        return jsonify({
            "success": True,
            "message": f"Added '{found_app['name']}' with 5 clicks",
            "sanitizedName": sanitized_name,
            "originalName": found_app['name']
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/test_with_sample_data', methods=['GET'])
def test_with_sample_data():
    try:
        sample_interactions = [
            ("TestApp1", "testapp1", 25),
            ("TestApp2", "testapp2", 10),
            ("TestApp3", "testapp3", 100),
            ("PopularApp", "popularapp", 75),
            ("NewApp", "newapp", 3)
        ]
        
        for app_name, sanitized_name, click_count in sample_interactions:
            try:
                result = supabase.table('user_interactions')\
                    .update({'clickCount': click_count, 'appName': app_name})\
                    .eq('sanitizedName', sanitized_name)\
                    .execute()
                
                if not result.data:
                    supabase.table('user_interactions').insert({
                        'appName': app_name,
                        'sanitizedName': sanitized_name,
                        'clickCount': click_count
                    }).execute()
                    
            except Exception as e:
                pass
        
        return jsonify({
            "message": "Sample user interaction data created",
            "sample_data": [
                {"name": name, "clicks": clicks} for name, _, clicks in sample_interactions
            ],
            "next_step": "Now try: curl http://127.0.0.1:5000/check_all"
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(
        debug=False,
        host="0.0.0.0", 
        port=port,
        threaded=True
    )
