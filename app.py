import requests
from datetime import datetime, timezone
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

def fetch_app_info_from_itunes(app_name: str) -> Optional[Dict[str, Any]]:
    """Fetch app information from iTunes Search API"""
    try:
        # Properly encode the app name for URL
        import urllib.parse
        encoded_app_name = urllib.parse.quote(app_name)
        search_url = f"https://itunes.apple.com/search?term={encoded_app_name}&entity=software"
        search_response = requests.get(search_url, timeout=10)
        
        if search_response.status_code == 200:
            search_data = search_response.json()
            if search_data['resultCount'] > 0:
                # Find the best match (partial match if exact not found)
                for app_info in search_data['results']:
                    track_name = app_info.get('trackName', '').lower()
                    if app_name.lower() in track_name:  # Partial match
                        screenshots = app_info.get("screenshotUrls", [])
                        features = app_info.get("features", [])
                        logo_url = app_info.get("artworkUrl100")
                        logo_url_200 = logo_url.replace('/100x100bb.jpg', '/200x200bb.jpg') if logo_url else ''
                        app_store_url = app_info.get("trackViewUrl", "")
                        artist_view_url = app_info.get("artistViewUrl", "")
                        track_content_rating = app_info.get("contentAdvisoryRating", "")
                        primary_genre = app_info.get("primaryGenreName", "")
                        seller_name = app_info.get("sellerName", "")
                        description = app_info.get("description", "No description available.")
                        categories = app_info.get("genres", [])
                        
                        return {
                            "name": app_info.get("trackName"),
                            "description": description,
                            "developer": app_info.get("artistName"),
                            "rating": app_info.get("averageUserRating"),
                            "price": app_info.get("formattedPrice"),
                            "genres": app_info.get("genres"),
                            "release_date": app_info.get("releaseDate"),
                            "screenshotUrls": screenshots,
                            "features": features,
                            "artworkUrl100": logo_url,
                            "logo": logo_url_200,
                            "appStore": app_store_url,
                            "artistViewUrl": artist_view_url,
                            "trackContentRating": track_content_rating,
                            "primaryGenreName": primary_genre,
                            "sellerName": seller_name,
                            "categories": categories
                        }
        return None
    except Exception as e:
        try:
            print(f"Error fetching iTunes info for {app_name}: {e}")
        except UnicodeEncodeError:
            print(f"Error fetching iTunes info for [App with special characters]: {e}")
        return None

def enrich_app_with_itunes_data(app_data: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich app data with iTunes information if missing key details"""
    # Only fetch iTunes data if app is missing critical information
    needs_enrichment = (
        (not app_data.get('screenshotUrls') or len(app_data.get('screenshotUrls', [])) == 0) and
        (not app_data.get('description') or app_data.get('description') == 'No description available.' or app_data.get('description') == '')
    )
    
    if needs_enrichment:
        try:
            print(f"Enriching app data for: {app_data['name']} (missing screenshots and description)")
        except UnicodeEncodeError:
            print(f"Enriching app data for: [App with special characters] (missing screenshots and description)")
        
        itunes_info = fetch_app_info_from_itunes(app_data['name'])
        
        if itunes_info:
            # Merge iTunes data with existing app data, preferring existing data when available
            app_data['screenshotUrls'] = app_data.get('screenshotUrls') or itunes_info.get('screenshotUrls', [])
            app_data['description'] = (app_data.get('description') if app_data.get('description') not in ['No description available.', '', None] 
                                     else itunes_info.get('description', 'No description available.'))
            app_data['categories'] = app_data.get('categories') or itunes_info.get('categories', [])
            app_data['features'] = app_data.get('features') or itunes_info.get('features', [])
            app_data['appStore'] = app_data.get('appStore') or itunes_info.get('appStore', '')
            app_data['artistViewUrl'] = app_data.get('artistViewUrl') or itunes_info.get('artistViewUrl', '')
            app_data['trackContentRating'] = app_data.get('trackContentRating') or itunes_info.get('trackContentRating', '')
            app_data['primaryGenreName'] = app_data.get('primaryGenreName') or itunes_info.get('primaryGenreName', '')
            app_data['sellerName'] = app_data.get('sellerName') or itunes_info.get('sellerName', '')
            app_data['artworkUrl100'] = app_data.get('artworkUrl100') or itunes_info.get('artworkUrl100', '')
            
            # Update logo if we got a better quality one from iTunes
            if not app_data.get('logo') and itunes_info.get('logo'):
                app_data['logo'] = itunes_info['logo']
            
            try:
                print(f"Successfully enriched app data for: {app_data['name']}")
            except UnicodeEncodeError:
                print("Successfully enriched app data for: [App with special characters]")
        else:
            try:
                print(f"Could not fetch iTunes data for: {app_data['name']}")
            except UnicodeEncodeError:
                print("Could not fetch iTunes data for: [App with special characters]")
    
    return app_data

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
            'lastChecked': datetime.now(timezone.utc).isoformat()
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
        current_click_count = current_app.get('clickCount', 0)
        new_click_count = app_data['clickCount']
        
        # Check if any important data has changed (not just status and clicks)
        # Normalize values for proper comparison
        current_screenshots = current_app.get('screenshotUrls') or []
        new_screenshots = app_data.get('screenshotUrls') or []
        current_description = current_app.get('description') or ''
        new_description = app_data.get('description') or ''
        current_categories = current_app.get('categories') or []
        new_categories = app_data.get('categories') or []
        current_features = current_app.get('features') or []
        new_features = app_data.get('features') or []
        current_appstore = current_app.get('appStore') or ''
        new_appstore = app_data.get('appStore') or ''
        current_artist_url = current_app.get('artistViewUrl') or ''
        new_artist_url = app_data.get('artistViewUrl') or ''
        current_logo = current_app.get('logo') or ''
        new_logo = app_data.get('logo') or ''
        
        data_unchanged = (
            current_click_count == new_click_count and 
            current_app.get('betaAvailable', 'unknown') == app_data['betaAvailable'] and
            current_screenshots == new_screenshots and
            current_description == new_description and
            current_categories == new_categories and
            current_features == new_features and
            current_appstore == new_appstore and
            current_artist_url == new_artist_url and
            current_logo == new_logo
        )
        
        if data_unchanged:
            return {'updated': False, 'status_changed': False, 'previous_status': previous_status}
        
        status_changed_to_open = (
            previous_status in ['full', 'not accepting', 'error', 'unknown'] and 
            app_data['betaAvailable'] == 'open'
        )
        
        update_data = {
            'betaAvailable': app_data['betaAvailable'],
            'clickCount': new_click_count,  # Ensure we're using the new click count
            'link': app_data['link'],
            'logo': app_data['logo'],
            'screenshotUrls': app_data.get('screenshotUrls', []),
            'description': app_data.get('description', ''),
            'categories': app_data.get('categories', []),
            'features': app_data.get('features', []),
            'appStore': app_data.get('appStore', ''),
            'artistViewUrl': app_data.get('artistViewUrl', ''),
            'trackContentRating': app_data.get('trackContentRating', ''),
            'primaryGenreName': app_data.get('primaryGenreName', ''),
            'sellerName': app_data.get('sellerName', ''),
            'artworkUrl100': app_data.get('artworkUrl100', ''),
            'lastChecked': datetime.now(timezone.utc).isoformat()
        }
        
        update_result = supabase.table('apps').update(update_data).eq('sanitizedName', sanitized_name).execute()
        
        if not update_result.data:
            try:
                print(f"Warning: No rows updated for app {app_data['name']}")
            except UnicodeEncodeError:
                print(f"Warning: No rows updated for app [App with special characters]")
        
        history_entry = {
            'appId': current_app['id'],
            'status': app_data['betaAvailable'],
            'clickCount': app_data['clickCount'],
            'timestamp': datetime.now(timezone.utc).isoformat()
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
            'click_count': new_click_count,
            'name': app_data['name']
        }
        
    except Exception as e:
        try:
            print(f"Error updating app status for {app_data.get('name', 'Unknown')}: {str(e)}")
        except UnicodeEncodeError:
            print(f"Error updating app status for [App with special characters]: {str(e)}")
        return {'updated': False, 'status_changed': False, 'previous_status': None, 'error': str(e)}

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
        version = f'python_status_change_{previous_status}_to_{current_status}_{datetime.now(timezone.utc).isoformat()}'
        supabase.table('telegram_posts').upsert({
            'appname': app_name,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'version': version
        }, {
            'onConflict': 'appname'
        }).execute()
    except Exception as e:
        pass

def send_email_notification(apps_to_notify: list, base_url: str = DEFAULT_NOTIFICATION_URL) -> Dict[str, Any]:
    try:
        if not apps_to_notify:
            return {'success': False, 'message': 'No apps to notify via email'}
        
        api_url = f"{base_url}/api/sendEmailToList"
        
        # Format apps data for email notification
        email_apps = []
        for app in apps_to_notify:
            email_app = {
                'name': app['name'],
                'betaAvailable': app.get('betaAvailable', app.get('status', 'open')),
                'clickCount': app.get('clickCount', 0),
                'categories': app.get('categories', []),
                'logo': app.get('logo', ''),
                'timestamp': app.get('timestamp', datetime.now(timezone.utc).isoformat())
            }
            email_apps.append(email_app)
        
        payload = {
            'apps': email_apps
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
        # Fetch ALL user interactions without limit using pagination
        all_interactions = []
        page_size = 1000
        page = 0
        
        while True:
            result = supabase.table('user_interactions')\
                .select('sanitizedName, clickCount')\
                .order('clickCount', desc=True)\
                .range(page * page_size, (page + 1) * page_size - 1)\
                .execute()
            
            if not result.data or len(result.data) == 0:
                break
                
            all_interactions.extend(result.data)
            
            if len(result.data) < page_size:
                break
                
            page += 1
        
        if not all_interactions:
            print("Warning: No user interactions found")
            return {}
        
        interactions = {}
        for item in all_interactions:
            sanitized_name = item.get('sanitizedName')
            click_count = item.get('clickCount', 0)
            if sanitized_name:
                interactions[sanitized_name] = click_count
        
        print(f"Loaded {len(interactions)} user interactions (all data)")
        
        return interactions
    except Exception as e:
        print(f"Error fetching user interactions: {str(e)}")
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

def process_apps_from_api(api_url: str, click_threshold: int, counter_key: str, max_apps_to_process: int = 1, send_notifications: bool = False, notification_base_url: str = DEFAULT_NOTIFICATION_URL) -> Dict[str, Any]:
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        response.close()
        del response

        if not data or 'apps' not in data:
            raise Exception('No app data found to process.')

        apps_data = data['apps']
        
        # Get current user interactions for accurate click counts
        user_interactions = get_user_interactions()
        
        if not user_interactions:
            return {"error": "No user interactions found - cannot determine click counts"}
        
        start_index = get_processing_index(counter_key)
        
        total_apps = len(apps_data)
        count = 0
        checked = 0
        apps_below_threshold = 0
        apps_to_notify = []

        max_check_limit = min(max_apps_to_process * 3, total_apps)
        try:
            print(f"process_apps_from_api: total_apps={total_apps}, start_index={start_index}, max_check_limit={max_check_limit}, max_apps_to_process={max_apps_to_process}")
        except Exception:
            pass
        
        while count < max_apps_to_process and checked < max_check_limit and checked < total_apps:
            app_index = (start_index + checked) % total_apps
            app = apps_data[app_index].copy()

            # Update click count from user_interactions instead of using stale API data
            sanitized_app_name = sanitize_string(app.get('name', ''))
            api_click = app.get('clickCount', 0)
            user_click = user_interactions.get(sanitized_app_name, 0)
            app['clickCount'] = user_click
            try:
                print(f"Checking app index={app_index} name={app.get('name')} sanitized={sanitized_app_name} api_click={api_click} user_click={user_click}")
            except Exception:
                pass

            if app['clickCount'] >= click_threshold:
                # Enrich app with iTunes data if missing details
                app = enrich_app_with_itunes_data(app)
                
                # App already has betaAvailable status from API, but let's check it fresh
                app['betaAvailable'] = fetch_beta_availability(app['link'])
                update_result = update_app_status(app)
                
                count += 1  # Count all qualifying apps, not just updated ones
                
                if update_result['updated']:
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
                            'previousStatus': update_result['previous_status'],
                            'categories': app.get('categories', []),
                            'logo': app.get('logo', ''),
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        })
                        
                        # Record that we're about to send a notification for this change
                        record_notification_sent(
                            app['name'],
                            update_result['current_status'],
                            update_result['previous_status']
                        )
                
                try:
                    if update_result.get('updated'):
                        print(f"Updated {app.get('name')} -> clickCount {user_click}")
                    else:
                        print(f"No update for {app.get('name')} (unchanged)")
                except Exception:
                    pass
                import gc
                gc.collect()
                time.sleep(1.0)
            else:
                apps_below_threshold += 1
                try:
                    print(f"Skipped {app.get('name')} below threshold (clicks={app['clickCount']})")
                except Exception:
                    pass

            checked += 1
            del app

        notification_result = None
        email_notification_result = None
        if send_notifications and apps_to_notify:
            notification_result = send_telegram_notification(apps_to_notify, notification_base_url)
            email_notification_result = send_email_notification(apps_to_notify, notification_base_url)

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
            result["telegram_notification"] = notification_result
            
        if email_notification_result:
            result["email_notification"] = email_notification_result
            
        return result
        
    except Exception as e:
        import gc
        gc.collect()
        return {"error": str(e)}

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
            return {"error": "No user interactions found - cannot determine click counts"}
        
        start_index = get_processing_index(counter_key)
        
        total_apps = len(data[0]['apps'])
        count = 0
        checked = 0
        apps_below_threshold = 0
        apps_to_notify = []

        max_check_limit = min(max_apps_to_process * 3, total_apps)
        
        while count < max_apps_to_process and checked < max_check_limit and checked < total_apps:
            app_index = (start_index + checked) % total_apps
            app = data[0]['apps'][app_index].copy()
            sanitized_app_name = sanitize_string(app['name'])
            app['clickCount'] = user_interactions.get(sanitized_app_name, 0)

            if app['clickCount'] >= click_threshold:
                # Enrich app with iTunes data if missing details
                app = enrich_app_with_itunes_data(app)
                
                app['betaAvailable'] = fetch_beta_availability(app['link'])
                update_result = update_app_status(app)
                
                count += 1  # Count all qualifying apps, not just updated ones
                
                # Log only errors
                if 'error' in update_result:
                    try:
                        print(f"Failed to update {app['name']}: {update_result.get('error')}")
                    except UnicodeEncodeError:
                        print(f"Failed to update [App with special characters]: {update_result.get('error')}")
                
                if update_result['updated']:
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
                            'previousStatus': update_result['previous_status'],
                            'categories': app.get('categories', []),
                            'logo': app.get('logo', ''),
                            'timestamp': datetime.now(timezone.utc).isoformat()
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
        email_notification_result = None
        if send_notifications and apps_to_notify:
            notification_result = send_telegram_notification(apps_to_notify, notification_base_url)
            email_notification_result = send_email_notification(apps_to_notify, notification_base_url)

        del data
        del user_interactions
        
        import gc
        gc.collect()

        new_last_checked_index = (start_index + checked) % total_apps
        update_processing_index(counter_key, new_last_checked_index)

        result = {
            "message": f"Processed {count} qualifying apps for {counter_key}.", 
            "details": {
                "checked": checked,
                "processed": count,
                "below_threshold": apps_below_threshold,
                "click_threshold": click_threshold,
                "notifications_sent": len(apps_to_notify) if apps_to_notify else 0
            }
        }
        
        if notification_result:
            result["telegram_notification"] = notification_result
            
        if email_notification_result:
            result["email_notification"] = email_notification_result
            
        return result
        
    except Exception as e:
        import gc
        gc.collect()
        return {"error": str(e)}

def parse_markdown(markdown_content: str) -> list:
    app_pattern = r"\*\*(.*?)\*\*:.*?\[!\[App Logo\]\((.*?)\)\]\((.*?)\)"
    matches = re.findall(app_pattern, markdown_content)
    return [{"name": match[0], "logo": match[1], "link": match[2]} for match in matches]

def process_apps(file_url: str, click_threshold: int, counter_key: str, max_apps_to_check: int = 20, send_notifications: bool = False, notification_base_url: str = DEFAULT_NOTIFICATION_URL) -> Dict[str, Any]:
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

        for i in range(start_index, start_index + max_apps_to_check):
            app_index = i % len(data)
            app = data[app_index]
            sanitized_app_name = sanitize_string(app['name'])
            app['clickCount'] = user_interactions.get(sanitized_app_name, 0)

            if app['clickCount'] >= click_threshold:
                # Enrich app with iTunes data if missing details
                app = enrich_app_with_itunes_data(app)
                
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
                            'previousStatus': update_result['previous_status'],
                            'categories': app.get('categories', []),
                            'logo': app.get('logo', ''),
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        })
                        
                        # Record that we're about to send a notification for this change
                        record_notification_sent(
                            app['name'],
                            update_result['current_status'],
                            update_result['previous_status']
                        )

        notification_result = None
        email_notification_result = None
        if send_notifications and apps_to_notify:
            # Send both Telegram and Email notifications
            notification_result = send_telegram_notification(apps_to_notify, notification_base_url)
            email_notification_result = send_email_notification(apps_to_notify, notification_base_url)

        new_last_checked_index = (start_index + max_apps_to_check) % len(data)
        update_processing_index(counter_key, new_last_checked_index)

        result = {
            "message": f"App statuses updated successfully for {counter_key}.",
            "details": {
                "processed": count,
                "notifications_sent": len(apps_to_notify) if apps_to_notify else 0
            }
        }
        
        if notification_result:
            result["telegram_notification"] = notification_result
            
        if email_notification_result:
            result["email_notification"] = email_notification_result
            
        return result
        
    except Exception as e:
        return {"error": str(e)}

@app.route('/check_supabase_api', methods=['GET'])
def check_supabase_api():
    api_url = 'https://telegram-js-xi.vercel.app/api/codewingFinalSupabase?cct=80'
    notification_url = request.args.get('notification_url', DEFAULT_NOTIFICATION_URL)
    
    try:
        click_threshold = int(request.args.get('click_threshold', '20'))
    except (ValueError, TypeError):
        click_threshold = 20
        
    try:
        max_apps_to_process = int(request.args.get('max_apps_to_process', '3'))
    except (ValueError, TypeError):
        max_apps_to_process = 3
    
    result = process_apps_from_api(
        api_url, 
        click_threshold=click_threshold, 
        counter_key='lastChecked_supabase_api', 
        max_apps_to_process=max_apps_to_process,
        send_notifications=True,
        notification_base_url=notification_url
    )
    return jsonify(result)

@app.route('/check_apps', methods=['GET'])
def check_apps():
    json_file = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json'
    notification_url = request.args.get('notification_url', DEFAULT_NOTIFICATION_URL)
    
    try:
        click_threshold = int(request.args.get('click_threshold', '20'))
    except (ValueError, TypeError):
        click_threshold = 20
        
    try:
        max_apps_to_process = int(request.args.get('max_apps_to_process', '50'))
    except (ValueError, TypeError):
        max_apps_to_process = 50
    
    result = process_apps_from_json(
        json_file, 
        click_threshold=click_threshold, 
        counter_key='lastChecked_check_apps', 
        max_apps_to_process=max_apps_to_process,
        send_notifications=True,
        notification_base_url=notification_url
    )
    return jsonify(result)

@app.route('/check_apps_with_notifications', methods=['GET'])
def check_apps_with_notifications():
    json_file = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json'
    notification_url = request.args.get('notification_url', DEFAULT_NOTIFICATION_URL)
    
    try:
        click_threshold = int(request.args.get('click_threshold', '10'))
    except (ValueError, TypeError):
        click_threshold = 10
        
    try:
        max_apps_to_process = int(request.args.get('max_apps_to_process', '50'))
    except (ValueError, TypeError):
        max_apps_to_process = 50
    
    result = process_apps_from_json(
        json_file, 
        click_threshold=click_threshold, 
        counter_key='lastChecked_with_notifications', 
        max_apps_to_process=max_apps_to_process,
        send_notifications=True,
        notification_base_url=notification_url
    )
    return jsonify(result)

@app.route('/daily_stat', methods=['GET'])
def daily_stat():
    file_url = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/daily-next.md'
    notification_url = request.args.get('notification_url', DEFAULT_NOTIFICATION_URL)
    
    try:
        click_threshold = int(request.args.get('click_threshold', '0'))
    except (ValueError, TypeError):
        click_threshold = 0
    
    result = process_apps(
        file_url, 
        click_threshold=click_threshold, 
        counter_key='lastChecked_dailyNext',
        max_apps_to_check=5,
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
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

@app.route('/keep_alive', methods=['GET'])
def keep_alive():
    import gc
    gc.collect()
    return jsonify({
        "status": "alive",
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

@app.route('/enrich_apps', methods=['GET'])
def enrich_apps():
    """Manually enrich apps with iTunes data for apps missing details"""
    try:
        notification_url = request.args.get('notification_url', DEFAULT_NOTIFICATION_URL)
        
        try:
            click_threshold = int(request.args.get('click_threshold', '10'))
        except (ValueError, TypeError):
            click_threshold = 10
            
        try:
            max_apps_to_process = int(request.args.get('max_apps_to_process', '5'))
        except (ValueError, TypeError):
            max_apps_to_process = 5
        
        # Get apps from Supabase that need enrichment
        result = supabase.table('apps')\
            .select('*')\
            .gte('clickCount', click_threshold)\
            .order('clickCount', desc=True)\
            .limit(max_apps_to_process * 2)\
            .execute()
        
        if not result.data:
            return jsonify({"message": "No apps found to enrich", "enriched": 0})
        
        enriched_count = 0
        processed_count = 0
        
        for app in result.data[:max_apps_to_process]:
            processed_count += 1
            
            # Check if app needs enrichment (only missing critical data)
            needs_enrichment = (
                (not app.get('screenshotUrls') or len(app.get('screenshotUrls', [])) == 0) and
                (not app.get('description') or app.get('description') in ['No description available.', ''])
            )
            
            if needs_enrichment:
                try:
                    print(f"Enriching app: {app['name']}")
                except UnicodeEncodeError:
                    print("Enriching app: [App with special characters]")
                
                # Create app data structure for enrichment
                app_data = {
                    'name': app['name'],
                    'screenshotUrls': app.get('screenshotUrls', []),
                    'description': app.get('description', ''),
                    'categories': app.get('categories', []),
                    'features': app.get('features', []),
                    'appStore': app.get('appStore', ''),
                    'artistViewUrl': app.get('artistViewUrl', ''),
                    'trackContentRating': app.get('trackContentRating', ''),
                    'primaryGenreName': app.get('primaryGenreName', ''),
                    'sellerName': app.get('sellerName', ''),
                    'artworkUrl100': app.get('artworkUrl100', ''),
                    'logo': app.get('logo', ''),
                    'link': app.get('link', ''),
                    'clickCount': app.get('clickCount', 0),
                    'betaAvailable': app.get('betaAvailable', 'unknown')
                }
                
                # Enrich with iTunes data
                enriched_app_data = enrich_app_with_itunes_data(app_data)
                
                # Update in database if we got new data
                if enriched_app_data != app_data:
                    update_result = update_app_status(enriched_app_data)
                    if update_result['updated']:
                        enriched_count += 1
                        try:
                            print(f"Successfully enriched and saved: {app['name']}")
                        except UnicodeEncodeError:
                            print("Successfully enriched and saved: [App with special characters]")
                
                time.sleep(0.5)  # Rate limit iTunes API calls
        
        return jsonify({
            "message": f"Enriched {enriched_count} apps out of {processed_count} processed",
            "details": {
                "processed": processed_count,
                "enriched": enriched_count,
                "click_threshold": click_threshold
            }
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/sync_all_click_counts', methods=['GET'])
def sync_all_click_counts():
    """Sync ALL click counts from user_interactions to apps table"""
    try:
        print("Starting full click count synchronization...")
        
        # Get all user interactions
        user_interactions = get_user_interactions()
        if not user_interactions:
            return jsonify({"error": "No user interactions found"}), 500
        
        # Get all apps that need updating
        result = supabase.table('apps')\
            .select('id, name, sanitizedName, clickCount')\
            .execute()
        
        if not result.data:
            return jsonify({"error": "No apps found"}), 500
        
        apps_to_update = []
        for app in result.data:
            sanitized_name = app['sanitizedName']
            current_clicks = app.get('clickCount', 0)
            new_clicks = user_interactions.get(sanitized_name, 0)
            
            if new_clicks != current_clicks:
                apps_to_update.append({
                    'id': app['id'],
                    'name': app['name'],
                    'sanitizedName': sanitized_name,
                    'old_clicks': current_clicks,
                    'new_clicks': new_clicks
                })
        
        if not apps_to_update:
            return jsonify({
                "message": "All click counts are already synchronized",
                "apps_checked": len(result.data),
                "apps_updated": 0
            })
        
        # Update in batches
        updated_count = 0
        batch_size = 100
        
        for i in range(0, len(apps_to_update), batch_size):
            batch = apps_to_update[i:i + batch_size]
            
            for app in batch:
                try:
                    update_result = supabase.table('apps')\
                        .update({\
                            'clickCount': app['new_clicks'],\
                            'lastChecked': datetime.now(timezone.utc).isoformat()\
                        })\
                        .eq('id', app['id'])\
                        .execute()
                    
                    if update_result.data:
                        updated_count += 1
                        
                except Exception as e:
                    print(f"Error updating {app['name']}: {str(e)}")
        
        return jsonify({
            "message": f"Successfully synchronized {updated_count} apps",
            "details": {
                "apps_checked": len(result.data),
                "apps_needing_update": len(apps_to_update),
                "apps_updated": updated_count,
                "user_interactions_loaded": len(user_interactions)
            }
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/quick_check', methods=['GET'])
def quick_check():
    json_file = 'https://raw.githubusercontent.com/aditya9738d/codewings_files/main/codewingiTune.json'
    notification_url = request.args.get('notification_url', DEFAULT_NOTIFICATION_URL)
    
    try:
        click_threshold = int(request.args.get('click_threshold', '10'))
    except (ValueError, TypeError):
        click_threshold = 10
        
    try:
        max_apps_to_process = int(request.args.get('max_apps_to_process', '30'))
    except (ValueError, TypeError):
        max_apps_to_process = 30
    
    result = process_apps_from_json(
        json_file, 
        click_threshold=click_threshold, 
        counter_key='lastChecked_quick', 
        max_apps_to_process=max_apps_to_process,
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
