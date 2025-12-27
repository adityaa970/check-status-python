import requests
from datetime import datetime
import json
from flask import Flask, jsonify, request
import time
import re
import os
from supabase import create_client, Client
from typing import Dict, Any, Optional

app = Flask(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL") or "https://cuqnpzoldeiztgezwqqe.supabase.co"
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN1cW5wem9sZGVpenRnZXp3cXFlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MTMwOTM3OSwiZXhwIjoyMDc2ODg1Mzc5fQ.Ltm4bRsX8UBOG5XuXLEciniGohs1Bvz6iHoPJqT8k8I"
DEFAULT_NOTIFICATION_URL = "https://telegram-js-xi.vercel.app"

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

def update_app_status(app_data: Dict[str, Any]) -> Dict[str, Any]:
    sanitized_name = sanitize_string(app_data['name'])
    
    try:
        result = supabase.table('apps').select('*').eq('sanitizedName', sanitized_name).execute()
        
        if not result.data:
            get_or_create_app(app_data)
            return {'updated': True, 'status_changed': False, 'previous_status': None}
        
        current_app = result.data[0]
        previous_status = current_app.get('betaAvailable', 'unknown')
        
        if (current_app.get('clickCount', 0) == app_data['clickCount'] and 
            current_app.get('betaAvailable', 'unknown') == app_data['betaAvailable']):
            return {'updated': False, 'status_changed': False, 'previous_status': previous_status}
        
        status_changed_to_open = (
            previous_status in ['full', 'not accepting', 'error', 'unknown'] and 
            app_data['betaAvailable'] == 'open'
        )
        
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
        
        return {
            'updated': True, 
            'status_changed': status_changed_to_open, 
            'previous_status': previous_status,
            'current_status': app_data['betaAvailable'],
            'click_count': app_data['clickCount'],
            'name': app_data['name']
        }
        
    except Exception as e:
        return {'updated': False, 'status_changed': False, 'previous_status': None}

def check_if_notification_sent(app_name: str, current_status: str, previous_status: str) -> bool:
    """Check if a notification was already sent for this specific status change"""
    try:
        sanitized_name = sanitize_string(app_name)
        result = supabase.table('telegram_posts')\
            .select('timestamp, version')\
            .eq('appname', app_name)\
            .execute()
        
        if not result.data:
            return False
        
        last_post = result.data[0]
        last_version = last_post.get('version', '')
        
        # Check if we already sent a notification for this specific status change
        status_change_pattern = f'python_status_change_{previous_status}_to_{current_status}'
        if status_change_pattern in last_version:
            return True
            
        return False
        
    except Exception as e:
        return False

def record_notification_sent(app_name: str, current_status: str, previous_status: str) -> None:
    """Record that a notification was sent for this app status change"""
    try:
        version = f'python_status_change_{previous_status}_to_{current_status}_{datetime.utcnow().isoformat()}'
        supabase.table('telegram_posts').upsert({
            'appname': app_name,
            'timestamp': datetime.utcnow().isoformat(),
            'version': version
        }, {
            'onConflict': 'appname'
        }).execute()
    except Exception as e:
        pass

def send_telegram_notification(apps_to_notify: list, base_url: str = DEFAULT_NOTIFICATION_URL) -> Dict[str, Any]:
    try:
        if not apps_to_notify:
            return {'success': False, 'message': 'No apps to notify'}
        
        api_url = f"{base_url}/api/sendTelegramFromPython"
        
        payload = {
            'apps': apps_to_notify
        }
        
        response = requests.post(api_url, json=payload, timeout=30)
        
        if response.status_code == 200:
            return {'success': True, 'data': response.json(), 'sent_count': len(apps_to_notify)}
        else:
            return {'success': False, 'error': f'HTTP {response.status_code}', 'response': response.text}
            
    except requests.exceptions.RequestException as e:
        return {'success': False, 'error': str(e)}
    except Exception as e:
        return {'success': False, 'error': str(e)}

def get_user_interactions() -> Dict[str, int]:
    try:
        result = supabase.table('user_interactions')\
            .select('sanitizedName, clickCount')\
            .limit(1000)\
            .execute()
        
        interactions = {}
        for item in result.data:
            interactions[item['sanitizedName']] = item['clickCount']
        
        del result
        
        return interactions
    except Exception as e:
        return {}



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

def process_apps_from_json(json_url: str, click_threshold: int, counter_key: str, max_apps_to_process: int = 1, send_notifications: bool = False, notification_base_url: str = DEFAULT_NOTIFICATION_URL) -> Dict[str, Any]:
    try:
        response = requests.get(json_url, timeout=10, stream=True)
        response.raise_for_status()
        
        data = response.json()
        
        response.close()
        del response

        if not data or 'apps' not in data[0]:
            raise Exception('No app data found to process.')

        user_interactions = get_user_interactions()
        
        if not user_interactions:
            return {"error": "No user interactions found"}
        
        start_index = get_processing_index(counter_key)
        
        total_apps = len(data[0]['apps'])
        count = 0
        checked = 0
        apps_below_threshold = 0
        apps_to_notify = []

        max_check_limit = min(max_apps_to_process * 10, 20)
        
        while count < max_apps_to_process and checked < max_check_limit and checked < total_apps:
            app_index = (start_index + checked) % total_apps
            app = data[0]['apps'][app_index].copy()
            sanitized_app_name = sanitize_string(app['name'])
            app['clickCount'] = user_interactions.get(sanitized_app_name, 0)

            if app['clickCount'] >= click_threshold:
                app['betaAvailable'] = fetch_beta_availability(app['link'])
                update_result = update_app_status(app)
                
                if update_result['updated']:
                    count += 1
                    
                    # Only send notification if status changed to open AND we haven't already notified about this change
                    if (send_notifications and 
                        update_result['status_changed'] and 
                        update_result['current_status'] == 'open' and
                        not check_if_notification_sent(
                            app['name'], 
                            update_result['current_status'], 
                            update_result['previous_status']
                        )):
                        
                        apps_to_notify.append({
                            'name': app['name'],
                            'clickCount': app['clickCount'],
                            'betaAvailable': app['betaAvailable'],
                            'previousStatus': update_result['previous_status']
                        })
                        
                        # Record that we're about to send a notification for this change
                        record_notification_sent(
                            app['name'],
                            update_result['current_status'],
                            update_result['previous_status']
                        )
                
                import gc
                gc.collect()
                time.sleep(1.0)
            else:
                apps_below_threshold += 1

            checked += 1
            del app

        notification_result = None
        if send_notifications and apps_to_notify:
            notification_result = send_telegram_notification(apps_to_notify, notification_base_url)

        del data
        del user_interactions
        
        import gc
        gc.collect()

        new_last_checked_index = (start_index + checked) % total_apps
        update_processing_index(counter_key, new_last_checked_index)

        result = {
            "message": f"Processed {count} apps for {counter_key}.", 
            "details": {
                "checked": checked,
                "processed": count,
                "below_threshold": apps_below_threshold,
                "click_threshold": click_threshold,
                "notifications_sent": len(apps_to_notify) if apps_to_notify else 0
            }
        }
        
        if notification_result:
            result["notification_result"] = notification_result
            
        return result
        
    except Exception as e:
        import gc
        gc.collect()
        return {"error": str(e)}

def parse_markdown(markdown_content: str) -> list:
    app_pattern = r"\*\*(.*?)\*\*:.*?\[!\[App Logo\]\((.*?)\)\]\((.*?)\)"
    matches = re.findall(app_pattern, markdown_content)
    return [{"name": match[0], "logo": match[1], "link": match[2]} for match in matches]

def process_apps(file_url: str, click_threshold: int, counter_key: str, send_notifications: bool = False, notification_base_url: str = DEFAULT_NOTIFICATION_URL) -> Dict[str, Any]:
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
        apps_to_notify = []

        for i in range(start_index, start_index + 20):
            app_index = i % len(data)
            app = data[app_index]
            sanitized_app_name = sanitize_string(app['name'])
            app['clickCount'] = user_interactions.get(sanitized_app_name, 0)

            if app['clickCount'] >= click_threshold:
                app['betaAvailable'] = fetch_beta_availability(app['link'])
                update_result = update_app_status(app)
                
                if update_result['updated']:
                    count += 1
                    
                    # Only send notification if status changed to open AND we haven't already notified about this change
                    if (send_notifications and 
                        update_result['status_changed'] and 
                        update_result['current_status'] == 'open' and
                        not check_if_notification_sent(
                            app['name'], 
                            update_result['current_status'], 
                            update_result['previous_status']
                        )):
                        
                        apps_to_notify.append({
                            'name': app['name'],
                            'clickCount': app['clickCount'],
                            'betaAvailable': app['betaAvailable'],
                            'previousStatus': update_result['previous_status']
                        })
                        
                        # Record that we're about to send a notification for this change
                        record_notification_sent(
                            app['name'],
                            update_result['current_status'],
                            update_result['previous_status']
                        )

        notification_result = None
        if send_notifications and apps_to_notify:
            notification_result = send_telegram_notification(apps_to_notify, notification_base_url)

        new_last_checked_index = (start_index + 20) % len(data)
        update_processing_index(counter_key, new_last_checked_index)

        result = {
            "message": f"App statuses updated successfully for {counter_key}.",
            "details": {
                "processed": count,
                "notifications_sent": len(apps_to_notify) if apps_to_notify else 0
            }
        }
        
        if notification_result:
            result["notification_result"] = notification_result
            
        return result
        
    except Exception as e:
        return {"error": str(e)}

@app.route('/check_apps', methods=['GET'])
def check_apps():
    json_file = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json'
    notification_url = request.args.get('notification_url', DEFAULT_NOTIFICATION_URL)
    
    result = process_apps_from_json(
        json_file, 
        click_threshold=20, 
        counter_key='lastChecked_check_apps', 
        max_apps_to_process=3,
        send_notifications=True,
        notification_base_url=notification_url
    )
    return jsonify(result)

@app.route('/check_apps_with_notifications', methods=['GET'])
def check_apps_with_notifications():
    json_file = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json'
    notification_url = request.args.get('notification_url', DEFAULT_NOTIFICATION_URL)
    
    result = process_apps_from_json(
        json_file, 
        click_threshold=10, 
        counter_key='lastChecked_with_notifications', 
        max_apps_to_process=3,
        send_notifications=True,
        notification_base_url=notification_url
    )
    return jsonify(result)

@app.route('/check_all', methods=['GET'])
def check_all():
    json_file = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json'
    notification_url = request.args.get('notification_url', DEFAULT_NOTIFICATION_URL)
    
    result = process_apps_from_json(
        json_file, 
        click_threshold=0, 
        counter_key='lastChecked_check_all', 
        max_apps_to_process=3,
        send_notifications=True,
        notification_base_url=notification_url
    )
    return jsonify(result)

@app.route('/daily_stat', methods=['GET'])
def daily_stat():
    file_url = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/daily-next.md'
    notification_url = request.args.get('notification_url', DEFAULT_NOTIFICATION_URL)
    
    result = process_apps(
        file_url, 
        click_threshold=0, 
        counter_key='lastChecked_dailyNext',
        send_notifications=True,
        notification_base_url=notification_url
    )
    return jsonify(result)

@app.route('/health', methods=['GET'])
def health():
    import gc
    gc.collect()
    return jsonify({
        "status": "healthy", 
        "timestamp": datetime.utcnow().isoformat()
    })

@app.route('/keep_alive', methods=['GET'])
def keep_alive():
    import gc
    gc.collect()
    return jsonify({
        "status": "alive",
        "timestamp": datetime.utcnow().isoformat()
    })

@app.route('/quick_check', methods=['GET'])
def quick_check():
    json_file = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json'
    notification_url = request.args.get('notification_url', DEFAULT_NOTIFICATION_URL)
    
    result = process_apps_from_json(
        json_file, 
        click_threshold=10, 
        counter_key='lastChecked_quick', 
        max_apps_to_process=3,
        send_notifications=True,
        notification_base_url=notification_url
    )
    return jsonify(result)



if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(
        debug=False,
        host="0.0.0.0", 
        port=port,
        threaded=True
    )
