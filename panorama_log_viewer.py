import rumps
import requests
import xml.etree.ElementTree as ET
import os
import json
from datetime import datetime
import time
from collections import defaultdict
from pathlib import Path

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")


class PanoramaAdminLogAppV2(rumps.App):
    VERSION = "1.2.023"

    def __init__(self):
        icon_file = "pan-logo-1.png"
        icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), icon_file)
        if not os.path.exists(icon_path):
            icon_path = None
        self.system_logs = []
        self.failed_commits = []
        self.panorama_id = None
        self.panoramas = {}

        # Set up secure cache directory
        self.cache_dir = self.get_secure_cache_dir()
        self.ensure_cache_dir_exists()

        self.load_config()
        title = "Panorama Logs"  # Initial title, will be updated by update_title()
        super().__init__(name="Panorama Logs", title=title, icon=icon_path)

        # About at the top
        self.menu.add(rumps.MenuItem("About", callback=self.show_about))

        # Help button
        self.menu.add(rumps.MenuItem("Help", callback=self.show_help))

        # Options submenu
        self.options_menu = rumps.MenuItem("Options", callback=None)
        self.hide_panorama_users_item = rumps.MenuItem("Hide Panorama Users", callback=self.toggle_hide_panorama_users)
        self.display_menu_icon_item = rumps.MenuItem("Display Menu Icon", callback=self.toggle_display_menu_icon)
        self.options_menu.add(self.hide_panorama_users_item)
        self.options_menu.add(self.display_menu_icon_item)
        self.menu.add(self.options_menu)

        # Update the option menu items based on current settings
        self.update_hide_panorama_users_menu()
        self.update_display_menu_icon_menu()
        self.update_title()

        # Separator
        self.menu.add(rumps.separator)

        # Create Sync submenu
        self.sync_menu = rumps.MenuItem("Sync", callback=None)
        self.sync_menu.add(rumps.MenuItem("Refresh Logs", callback=self.refresh_logs))
        self.sync_menu.add(rumps.MenuItem("Parse Saved Config Logs", callback=self.parse_saved_config_logs))
        self.sync_menu.add(rumps.MenuItem("Force Clear and Reload Logs", callback=self.force_clear_and_reload_logs))
        self.sync_menu.add(rumps.separator)
        self.sync_menu.add(rumps.MenuItem("Sync 5000 Logs", callback=self.pull_extended_logs))
        self.sync_menu.add(rumps.MenuItem("Sync 10000 Logs", callback=self.pull_10000_logs))
        self.sync_menu.add(rumps.separator)
        self.sync_menu.add(rumps.MenuItem("Clear All Cache", callback=self.clear_all_cache))
        self.menu.add(self.sync_menu)

        # Don't create switch menu here - let build_switch_panorama_menu() handle it completely
        # Build the Switch Panorama submenu first (will be added in build method)
        self.build_switch_panorama_menu()

        # Add separator between Switch Panorama and Search Logs
        self.menu.add(rumps.separator)

        # Then add Search Logs after Switch Panorama
        self.menu.add(rumps.MenuItem("Search Logs", callback=self.search_logs))

        self.config_log_menu = rumps.MenuItem("Show Config Log Entries", callback=None)
        self.system_log_menu = rumps.MenuItem("Show System Log Entries", callback=None)
        self.failed_commit_menu = rumps.MenuItem("Show Failed Commits", callback=None)

        self.menu.add(self.config_log_menu)
        self.menu.add(self.system_log_menu)
        self.menu.add(self.failed_commit_menu)

        # Refresh logs
        self.refresh_logs(None)

    def get_secure_cache_dir(self):
        """Get secure application cache directory."""
        if hasattr(os, 'getuid'):  # Unix-like systems (macOS/Linux)
            # Use macOS standard cache directory
            cache_base = os.path.expanduser('~/Library/Caches')
            app_cache = os.path.join(cache_base, 'PanoramaAdminLogs')
        else:
            # Fallback for other systems
            import tempfile
            app_cache = os.path.join(tempfile.gettempdir(), 'PanoramaAdminLogs')

        return app_cache

    def ensure_cache_dir_exists(self):
        """Create cache directory with secure permissions."""
        try:
            os.makedirs(self.cache_dir, mode=0o700, exist_ok=True)  # Owner read/write/execute only
            print(f"Cache directory: {self.cache_dir}")
        except Exception as e:
            print(f"Warning: Could not create secure cache directory: {e}")
            # Fallback to current directory
            self.cache_dir = os.path.dirname(os.path.abspath(__file__))

    def get_cache_file_path(self, log_type):
        """Get secure path for cache files."""
        # Sanitize panorama name for filename
        safe_name = "".join(c for c in self.panorama if c.isalnum() or c in ".-_")
        filename = f"{safe_name}_raw_{log_type}_log.xml"
        return os.path.join(self.cache_dir, filename)

    def clear_cache_files(self, panorama_name=None):
        """Clear cache files for specific panorama or all."""
        try:
            if panorama_name:
                # Clear specific panorama files
                safe_name = "".join(c for c in panorama_name if c.isalnum() or c in ".-_")
                for log_type in ['config', 'system']:
                    cache_file = os.path.join(self.cache_dir, f"{safe_name}_raw_{log_type}_log.xml")
                    if os.path.exists(cache_file):
                        os.remove(cache_file)
                        print(f"Removed cache file: {cache_file}")
            else:
                # Clear all cache files
                if os.path.exists(self.cache_dir):
                    for file in os.listdir(self.cache_dir):
                        if file.endswith('.xml'):
                            file_path = os.path.join(self.cache_dir, file)
                            os.remove(file_path)
                            print(f"Removed cache file: {file_path}")
                    print("Cleared all cache files")
        except Exception as e:
            print(f"Error clearing cache files: {e}")

    def clear_log_menus(self):
        """Completely clear and recreate log menu objects to prevent duplicates."""
        try:
            # Remove old menu objects completely
            if hasattr(self, 'config_log_menu'):
                self.menu.pop("Show Config Log Entries", None)
            if hasattr(self, 'system_log_menu'):
                self.menu.pop("Show System Log Entries", None)
            if hasattr(self, 'failed_commit_menu'):
                self.menu.pop("Show Failed Commits", None)
        except Exception as e:
            print(f"Error removing old log menus: {e}")

        # Create brand new menu objects
        self.config_log_menu = rumps.MenuItem("Show Config Log Entries", callback=None)
        self.system_log_menu = rumps.MenuItem("Show System Log Entries", callback=None)
        self.failed_commit_menu = rumps.MenuItem("Show Failed Commits", callback=None)

        # Add them back to the main menu
        self.menu.add(self.config_log_menu)
        self.menu.add(self.system_log_menu)
        self.menu.add(self.failed_commit_menu)

        print("Cleared and recreated all log menus")

    def clear_all_cache(self, _):
        """Menu callback to clear all cache files."""
        self.clear_cache_files()
        rumps.alert("Cache Cleared", "All cached log files have been removed.")

    def pull_10000_logs(self, _):
        try:
            # Clear existing data structures
            self.config_logs = []
            self.system_logs = []
            self.failed_commits = []

            # Clear existing menus completely
            self.clear_log_menus()

            # Download logs in chunks due to API 5000-log limit
            print("Downloading 10,000 logs in two 5000-log chunks due to API limitations...")

            # First chunk: most recent 5000 logs
            self.download_and_merge_logs("config", nlogs=5000)
            self.download_and_merge_logs("system", nlogs=5000)

            # Second chunk: next 5000 logs (using skip parameter)
            print("Downloading second chunk of 5000 logs...")
            self.download_and_merge_logs_with_skip("config", nlogs=5000, skip=5000)
            self.download_and_merge_logs_with_skip("system", nlogs=5000, skip=5000)

            # Parse all collected logs
            self.parse_saved_config_logs(None)
            self.parse_saved_system_logs()

            # Rebuild menus with fresh data
            self.build_config_log_menu()
            self.build_system_log_menu()
            self.build_failed_commit_menu()
            rumps.alert("Sync Complete", "10,000 log sync completed successfully (2 x 5000 chunks).")
        except Exception as e:
            rumps.alert("Sync Failed", f"An error occurred during sync:\n{e}")

    def search_logs(self, _):
        import subprocess

        try:
            # Use AppleScript for better focus handling
            search_script = '''
            tell application "System Events"
                activate
                display dialog "Enter keyword to search in config logs:" default answer "" with title "Search Logs"
            end tell
            '''
            result = subprocess.run(['osascript', '-e', search_script],
                                    capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                return  # User cancelled

            # Extract the text from AppleScript result
            term = result.stdout.strip().split('text returned:')[1].split(',')[0].strip()
            if not term:
                return

        except subprocess.TimeoutExpired:
            rumps.alert("Timeout", "Search dialog timed out.")
            return
        except Exception as e:
            print(f"AppleScript dialog error: {e}")
            # Fallback to rumps dialog
            term = rumps.Window("Enter keyword to search in config logs:", "Search Logs").run().text.strip()
            if not term:
                return

        matches = []
        for log in getattr(self, 'config_logs', []):
            for value in log.values():
                if term.lower() in str(value).lower():
                    matches.append(log)
                    break
        if not matches:
            rumps.alert("No matching entries found.")
            return

        # Generate a unique search result filename in cache directory
        base = "search_results"
        i = 1
        while os.path.exists(os.path.join(self.cache_dir, f"{base}_{i:03}.txt")):
            i += 1
        filename = os.path.join(self.cache_dir, f"{base}_{i:03}.txt")

        # Write results to the unique filename for user access
        with open(filename, "w") as f:
            for log in matches:
                for k, v in sorted(log.items()):
                    f.write(f"{k}: {v}\n")
                f.write("\n" + "-" * 80 + "\n\n")

        rumps.alert("Search Results Exported", f"{len(matches)} result(s) exported to {filename}")

        menu_items = [f"{log.get('Admin', 'Unknown')} | {log.get('Command Type', '')} @ {log.get('Received', '')}" for
                      log in matches]
        prompt = "Select a matching log:\n" + "\n".join(f"{i + 1}. {item}" for i, item in enumerate(menu_items))

        try:
            # Use AppleScript for selection dialog
            choice_script = f'''
            tell application "System Events"
                activate
                display dialog "{prompt}" default answer "" with title "Search Results"
            end tell
            '''
            result = subprocess.run(['osascript', '-e', choice_script],
                                    capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                choice = result.stdout.strip().split('text returned:')[1].split(',')[0].strip()
            else:
                return  # User cancelled
        except Exception as e:
            print(f"AppleScript choice dialog error: {e}")
            # Fallback to rumps dialog
            choice = rumps.Window(prompt, "Search Results").run().text.strip()

        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(matches):
                self.show_entry_details(matches[idx])

    def load_config(self):
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r") as f:
                data = json.load(f)
                self.panoramas = data.get("panoramas", {})
                self.panorama = data.get("active", "")
                self.hide_panorama_users = data.get("hide_panorama_users", False)
                self.display_menu_icon = data.get("display_menu_icon", False)
                info = self.panoramas.get(self.panorama, {})
                self.api_key = info.get("api_key")
                self.panorama_id = info.get("id")
        else:
            self.panoramas = {}
            self.panorama = ""
            self.api_key = ""
            self.panorama_id = None
            self.hide_panorama_users = False
            self.display_menu_icon = False

    def save_config(self):
        data = {
            "panoramas": self.panoramas,
            "active": self.panorama,
            "hide_panorama_users": self.hide_panorama_users,
            "display_menu_icon": self.display_menu_icon
        }
        with open(CONFIG_PATH, "w") as f:
            json.dump(data, f)

    def prompt_for_credentials(self, _=None):
        import subprocess

        try:
            # Use AppleScript for better focus handling
            panorama_script = '''
            tell application "System Events"
                activate
                display dialog "Enter Panorama URL or IP:" default answer "" with title "Panorama Login"
            end tell
            '''
            result = subprocess.run(['osascript', '-e', panorama_script],
                                    capture_output=True, text=True, timeout=30)
            print(
                f"Panorama dialog result: {result.returncode}, stdout: {repr(result.stdout)}, stderr: {repr(result.stderr)}")

            if result.returncode != 0:
                print("User cancelled panorama dialog or error occurred")
                return  # User cancelled

            # Extract the text from AppleScript result - more robust parsing
            try:
                if 'text returned:' in result.stdout:
                    panorama = result.stdout.strip().split('text returned:')[1].split(',')[0].strip()
                    # Remove any quotes and newlines that might be around the text
                    panorama = panorama.strip('"').strip("'").strip('\n').strip()
                else:
                    print("No 'text returned:' found in panorama output")
                    return self.prompt_for_credentials_fallback()
            except Exception as e:
                print(f"Error parsing panorama result: {e}")
                return self.prompt_for_credentials_fallback()

            if not panorama:
                print("Empty panorama value")
                return

            print(f"Panorama extracted: {repr(panorama)}")

            user_script = '''
            tell application "System Events"
                activate
                display dialog "Enter Username:" default answer "" with title "Panorama Login"
            end tell
            '''
            result = subprocess.run(['osascript', '-e', user_script],
                                    capture_output=True, text=True, timeout=30)
            print(
                f"Username dialog result: {result.returncode}, stdout: {repr(result.stdout)}, stderr: {repr(result.stderr)}")

            if result.returncode != 0:
                print("User cancelled username dialog or error occurred")
                return  # User cancelled

            # Extract the text from AppleScript result - more robust parsing
            try:
                if 'text returned:' in result.stdout:
                    user = result.stdout.strip().split('text returned:')[1].split(',')[0].strip()
                    # Remove any quotes and newlines that might be around the text
                    user = user.strip('"').strip("'").strip('\n').strip()
                else:
                    print("No 'text returned:' found in username output")
                    return self.prompt_for_credentials_fallback()
            except Exception as e:
                print(f"Error parsing username result: {e}")
                return self.prompt_for_credentials_fallback()

            if not user:
                print("Empty username value")
                return

            print(f"Username extracted: {repr(user)}")

            # Try a simpler AppleScript approach for password without "with hidden answer"
            # This might avoid the system crash while still providing focus
            print("Trying simplified AppleScript for password")
            password_script = '''
            tell application "System Events"
                activate
                display dialog "Enter Password (will be visible):" default answer "" with title "Panorama Login"
            end tell
            '''
            result = subprocess.run(['osascript', '-e', password_script],
                                    capture_output=True, text=True, timeout=30)
            print(
                f"Password dialog result: {result.returncode}, stdout: {repr(result.stdout)}, stderr: {repr(result.stderr)}")

            if result.returncode != 0:
                print("Password dialog failed, trying fallback")
                # If AppleScript fails, try the fallback method completely
                return self.prompt_for_credentials_fallback()

            # Extract the text from AppleScript result
            try:
                if 'text returned:' in result.stdout:
                    password = result.stdout.strip().split('text returned:')[1].split(',')[0].strip()
                    # Remove any quotes and newlines that might be around the text
                    password = password.strip('"').strip("'").strip('\n').strip()
                else:
                    print("No 'text returned:' found in password output")
                    return self.prompt_for_credentials_fallback()
            except Exception as e:
                print(f"Error parsing password result: {e}")
                return self.prompt_for_credentials_fallback()

            if not password:
                print("Empty password value")
                return

            print(f"Password entered (length): {len(password)}")

        except subprocess.TimeoutExpired:
            print("AppleScript dialog timed out")
            rumps.alert("Timeout", "Login dialog timed out.")
            return
        except Exception as e:
            print(f"AppleScript dialog error: {e}")
            # Fallback to rumps dialogs
            return self.prompt_for_credentials_fallback()

        print(f"Proceeding with API call: panorama={panorama}, user={user}")

        # Continue with API key generation
        keygen_url = f"https://{panorama}/api/?type=keygen&user={user}&password={password}"
        try:
            r = requests.get(keygen_url, verify=False, timeout=10)
            root = ET.fromstring(r.text)
            key = root.findtext(".//key")
            if not key:
                rumps.alert("Login Failed",
                            "Unable to retrieve API key. Please check your credentials and Panorama URL.")
                return
            if key:
                new_id = len(self.panoramas) + 1
                self.panoramas[panorama] = {"api_key": key, "id": new_id}
                self.panorama = panorama
                self.api_key = key
                self.panorama_id = new_id
                self.save_config()
                self.update_title()
                self.build_switch_panorama_menu()  # Rebuild switch menu
                self.refresh_logs(None)
                print("Successfully added new panorama")
        except Exception as e:
            print(f"API call failed: {e}")
            rumps.alert("Login Failed", str(e))

    def prompt_for_credentials_fallback(self, _=None):
        """Fallback method using rumps dialogs if AppleScript fails."""
        panorama = rumps.Window("Enter Panorama URL or IP:", "Login").run().text.strip()
        if not panorama:
            return
        user = rumps.Window("Enter Username:", "Login").run().text.strip()
        if not user:
            return
        password = rumps.Window("Enter Password:", "Login", dimensions=(300, 100)).run().text.strip()
        if not password:
            return

        keygen_url = f"https://{panorama}/api/?type=keygen&user={user}&password={password}"
        try:
            r = requests.get(keygen_url, verify=False, timeout=10)
            root = ET.fromstring(r.text)
            key = root.findtext(".//key")
            if not key:
                rumps.alert("Login Failed",
                            "Unable to retrieve API key. Please check your credentials and Panorama URL.")
                return
            if key:
                new_id = len(self.panoramas) + 1
                self.panoramas[panorama] = {"api_key": key, "id": new_id}
                self.panorama = panorama
                self.api_key = key
                self.panorama_id = new_id
                self.save_config()
                self.update_title()
                self.build_switch_panorama_menu()  # Rebuild switch menu
                self.refresh_logs(None)
        except Exception as e:
            rumps.alert("Login Failed", str(e))

    def refresh_logs(self, _):
        # Clear cached logs before loading new ones
        self.config_logs = []
        self.system_logs = []
        self.failed_commits = []

        if not self.api_key or not self.panorama:
            self.prompt_for_credentials()
            return

        # Clear menus completely using the new helper method
        self.clear_log_menus()

        # Delete old cache files before loading new ones
        config_file = self.get_cache_file_path("config")
        system_file = self.get_cache_file_path("system")
        if os.path.exists(config_file):
            os.remove(config_file)
            print(f"Removed old config file: {config_file}")
        if os.path.exists(system_file):
            os.remove(system_file)
            print(f"Removed old system file: {system_file}")

        # Download fresh logs
        self.download_and_merge_logs("config")
        self.download_and_merge_logs("system")

        # Parse the fresh logs
        self.parse_saved_config_logs(None)
        self.parse_saved_system_logs()

        # Rebuild menus with fresh data
        self.build_config_log_menu()
        self.build_system_log_menu()
        self.build_failed_commit_menu()

        print(
            f"Refreshed logs for {self.panorama}: {len(getattr(self, 'config_logs', []))} config, {len(self.system_logs)} system")

    def download_and_merge_logs(self, log_type, nlogs=None):
        nlogs_param = f"&nlogs={nlogs}" if nlogs else ""
        print(f"Requesting {log_type} logs from {self.panorama} (nlogs={nlogs})...")
        base_url = f"https://{self.panorama}/api/?type=log&log-type={log_type}{nlogs_param}&key={self.api_key}"

        try:
            r = requests.get(base_url, verify=False, timeout=10)
            print(f"API Response status: {r.status_code}")
            root = ET.fromstring(r.text)

            # Check for API errors
            status = root.get('status')
            if status == 'error':
                error_msg = root.findtext('.//msg')
                print(f"API Error: {error_msg}")
                return

            job_id = root.findtext(".//job")
            if not job_id:
                print(f"No job ID received for {log_type} logs")
                print(f"Response: {r.text[:500]}...")  # Print first 500 chars for debugging
                return

            # GUI notification: job started
            try:
                rumps.notification("Panorama Logs", f"Downloading {log_type} logs",
                                   f"Job started (requesting {nlogs or 'default'} logs)...")
            except Exception as e:
                print(f"Notification error (ignored): {e}")

            print(f"Log job {job_id} started, waiting for completion...")
            # Increase to 60 attempts (~2 minutes)
            for attempt in range(60):
                # GUI status: update menu bar title with progress (only if not using icon-only mode)
                if not self.display_menu_icon:
                    self.title = f"Fetching {log_type} logs... ({attempt + 1}/60)"
                print(f"Checking status for job {job_id}... ({attempt + 1}/60)")
                status_url = f"https://{self.panorama}/api/?type=log&log-type={log_type}&action=get&job-id={job_id}&key={self.api_key}"
                job_r = requests.get(status_url, verify=False, timeout=10)
                job_root = ET.fromstring(job_r.text)
                status = job_root.findtext(".//status")

                print(f"Job status: {status}")

                if status == "FIN":
                    # Count how many entries we actually got
                    entries_received = len(job_root.findall(".//entry"))
                    print(f"Job completed! Received {entries_received} entries (requested {nlogs or 'default'})")

                    # Use secure cache file path
                    cache_file = self.get_cache_file_path(log_type)

                    # Check for existing entries to merge
                    if os.path.exists(cache_file):
                        try:
                            existing_tree = ET.parse(cache_file)
                            existing_root = existing_tree.getroot()
                            existing_entries = existing_root.findall(".//entry")
                            latest = max((entry.findtext("time_generated") for entry in existing_entries if
                                          entry.find("time_generated")), default="")
                        except Exception as e:
                            print(f"Error reading existing cache file: {e}")
                            latest = ""
                    else:
                        latest = ""

                    new_entries = []
                    for entry in job_root.findall(".//entry"):
                        tg = entry.findtext("time_generated")
                        if tg and tg > latest:
                            new_entries.append(entry)

                    if new_entries:
                        if os.path.exists(cache_file):
                            tree = ET.parse(cache_file)
                            root_element = tree.getroot().find(".//log")
                        else:
                            root_element = ET.Element("log")
                            tree = ET.ElementTree(ET.Element("response"))
                            ET.SubElement(tree.getroot(), "result").append(root_element)

                        for entry in new_entries:
                            root_element.append(entry)

                        # Write to secure cache location
                        tree.write(cache_file)
                        # Set restrictive permissions on the cache file
                        os.chmod(cache_file, 0o600)

                    print(f"Merged {len(new_entries)} new {log_type} log entries into secure cache.")
                    print(f"Total entries received from API: {entries_received}")
                    break
                elif status == "FAIL":
                    error_details = job_root.findtext(".//details")
                    print(f"Job failed: {error_details}")
                    break

                time.sleep(2)
            else:
                print(f"Job {job_id} did not complete in time for {log_type} logs.")

            # After loop, reset app title
            self.update_title()

        except Exception as e:
            print(f"Error downloading {log_type} logs: {e}")
        finally:
            # Always reset title after attempt, in case of early return/exception
            self.update_title()

    def download_and_merge_logs_with_skip(self, log_type, nlogs=None, skip=0):
        """Download logs with skip parameter for pagination."""
        nlogs_param = f"&nlogs={nlogs}" if nlogs else ""
        skip_param = f"&skip={skip}" if skip > 0 else ""
        print(f"Requesting {log_type} logs from {self.panorama} (nlogs={nlogs}, skip={skip})...")
        base_url = f"https://{self.panorama}/api/?type=log&log-type={log_type}{nlogs_param}{skip_param}&key={self.api_key}"

        try:
            r = requests.get(base_url, verify=False, timeout=10)
            print(f"API Response status: {r.status_code}")
            root = ET.fromstring(r.text)

            # Check for API errors
            status = root.get('status')
            if status == 'error':
                error_msg = root.findtext('.//msg')
                print(f"API Error: {error_msg}")
                return

            job_id = root.findtext(".//job")
            if not job_id:
                print(f"No job ID received for {log_type} logs (skip={skip})")
                print(f"Response: {r.text[:500]}...")  # Print first 500 chars for debugging
                return

            # GUI notification: job started
            try:
                rumps.notification("Panorama Logs", f"Downloading {log_type} logs",
                                   f"Chunk {2 if skip > 0 else 1} started ({nlogs or 'default'} logs)...")
            except Exception as e:
                print(f"Notification error (ignored): {e}")

            print(f"Log job {job_id} started, waiting for completion...")
            # Increase to 60 attempts (~2 minutes)
            for attempt in range(60):
                # GUI status: update menu bar title with progress (only if not using icon-only mode)
                chunk_label = f"chunk {2 if skip > 0 else 1}"
                if not self.display_menu_icon:
                    self.title = f"Fetching {log_type} logs {chunk_label}... ({attempt + 1}/60)"
                print(f"Checking status for job {job_id}... ({attempt + 1}/60)")
                status_url = f"https://{self.panorama}/api/?type=log&log-type={log_type}&action=get&job-id={job_id}&key={self.api_key}"
                job_r = requests.get(status_url, verify=False, timeout=10)
                job_root = ET.fromstring(job_r.text)
                status = job_root.findtext(".//status")

                print(f"Job status: {status}")

                if status == "FIN":
                    # Count how many entries we actually got
                    entries_received = len(job_root.findall(".//entry"))
                    print(
                        f"Job completed! Received {entries_received} entries (requested {nlogs or 'default'}, skip={skip})")

                    # Use secure cache file path
                    cache_file = self.get_cache_file_path(log_type)

                    # For skip requests, we always append (don't check for latest time)
                    # since we're getting older logs
                    if skip > 0:
                        new_entries = job_root.findall(".//entry")
                    else:
                        # For the first request, still check for duplicates
                        if os.path.exists(cache_file):
                            try:
                                existing_tree = ET.parse(cache_file)
                                existing_root = existing_tree.getroot()
                                existing_entries = existing_root.findall(".//entry")
                                latest = max((entry.findtext("time_generated") for entry in existing_entries if
                                              entry.find("time_generated")), default="")
                            except Exception as e:
                                print(f"Error reading existing cache file: {e}")
                                latest = ""
                        else:
                            latest = ""

                        new_entries = []
                        for entry in job_root.findall(".//entry"):
                            tg = entry.findtext("time_generated")
                            if tg and tg > latest:
                                new_entries.append(entry)

                    if new_entries:
                        if os.path.exists(cache_file):
                            tree = ET.parse(cache_file)
                            root_element = tree.getroot().find(".//log")
                        else:
                            root_element = ET.Element("log")
                            tree = ET.ElementTree(ET.Element("response"))
                            ET.SubElement(tree.getroot(), "result").append(root_element)

                        for entry in new_entries:
                            root_element.append(entry)

                        # Write to secure cache location
                        tree.write(cache_file)
                        # Set restrictive permissions on the cache file
                        os.chmod(cache_file, 0o600)

                    print(f"Merged {len(new_entries)} new {log_type} log entries into secure cache (skip={skip}).")
                    break
                elif status == "FAIL":
                    error_details = job_root.findtext(".//details")
                    print(f"Job failed: {error_details}")
                    break

                time.sleep(2)
            else:
                print(f"Job {job_id} did not complete in time for {log_type} logs (skip={skip}).")

            # After loop, reset app title
            self.update_title()

        except Exception as e:
            print(f"Error downloading {log_type} logs with skip={skip}: {e}")
        finally:
            # Always reset title after attempt, in case of early return/exception
            self.update_title()

    def pull_extended_logs(self, _):
        try:
            # Clear existing data structures
            self.config_logs = []
            self.system_logs = []
            self.failed_commits = []

            # Clear existing menus completely
            self.clear_log_menus()

            # Download and process new logs
            self.download_and_merge_logs("config", nlogs=5000)
            self.download_and_merge_logs("system", nlogs=5000)
            self.parse_saved_config_logs(None)
            self.parse_saved_system_logs()

            # Rebuild menus with fresh data
            self.build_config_log_menu()
            self.build_system_log_menu()
            self.build_failed_commit_menu()
            rumps.alert("Sync Complete", "Log sync completed successfully.")
        except Exception as e:
            rumps.alert("Sync Failed", f"An error occurred during sync:\n{e}")

    def parse_saved_config_logs(self, _):
        try:
            cache_file = self.get_cache_file_path("config")
            if not os.path.exists(cache_file):
                print(f"No config cache file found: {cache_file}")
                self.config_logs = []
                return

            tree = ET.parse(cache_file)
            root = tree.getroot()
            self.failed_commits = []
            self.config_logs = []
            seen_log_ids = set()

            for i, entry in enumerate(root.findall(".//entry"), start=1):
                log_id = entry.attrib.get("logid")
                if log_id and log_id in seen_log_ids:
                    continue
                if log_id:
                    seen_log_ids.add(log_id)

                log = {
                    "Log ID": log_id,
                    "Received": entry.findtext("receive_time"),
                    "Firewall Serial": entry.findtext("serial"),
                    "Device Name": entry.findtext("device_name"),
                    "Source IP": entry.findtext("host"),
                    "Command Type": entry.findtext("cmd"),
                    "Admin": entry.findtext("admin"),
                    "Access Method": entry.findtext("client"),
                    "Result": entry.findtext("result"),
                    "Config Section": entry.findtext("path"),
                    "Full Path": entry.findtext("full-path"),
                }
                self.config_logs.append(log)
                if log["Result"] and "fail" in log["Result"].lower():
                    self.failed_commits.append(log)

            print(f"Parsed {len(self.config_logs)} config logs from cache")
        except Exception as e:
            print(f"Config Parse Error: {e}")
            self.config_logs = []
            self.failed_commits = []

    def parse_saved_system_logs(self):
        try:
            cache_file = self.get_cache_file_path("system")
            if not os.path.exists(cache_file):
                print(f"No system cache file found: {cache_file}")
                self.system_logs = []
                return

            tree = ET.parse(cache_file)
            root = tree.getroot()
            self.system_logs = []
            seen_log_ids = set()

            for i, entry in enumerate(root.findall(".//entry"), start=1):
                log_id = entry.attrib.get("logid")
                if log_id in seen_log_ids:
                    continue
                seen_log_ids.add(log_id)
                self.system_logs.append({
                    "Entry #": i,
                    "Log ID": log_id,
                    "Time": entry.findtext("time_generated"),
                    "Type": entry.findtext("type"),
                    "Severity": entry.findtext("severity"),
                    "Event": entry.findtext("eventid"),
                    "Description": entry.findtext("opaque"),
                    "Admin": entry.findtext("admin"),
                    "Host": entry.findtext("host"),
                    "Client": entry.findtext("client"),
                })

            print(f"Parsed {len(self.system_logs)} system logs from cache")
        except Exception as e:
            print(f"System Parse Error: {e}")
            self.system_logs = []

    def build_config_log_menu(self):
        if hasattr(self.config_log_menu, 'menu'):
            self.config_log_menu.menu = {}
        admin_groups = defaultdict(list)
        for log in getattr(self, "config_logs", []):
            admin = log.get("Admin", "Unknown")
            # Skip Panorama system users if the option is enabled
            if self.hide_panorama_users and admin.startswith("Panorama-"):
                continue
            admin_groups[admin].append(log)
        for admin, logs in admin_groups.items():
            admin_menu = rumps.MenuItem(f"Admin: {admin}", callback=None)
            type_groups = defaultdict(list)
            for log in logs:
                cmd = log.get("Command Type", "Unknown")
                type_groups[cmd].append(log)
            for cmd_type, typed_logs in type_groups.items():
                type_menu = rumps.MenuItem(cmd_type, callback=None)
                for log in typed_logs:
                    try:
                        dt = datetime.strptime(log.get("Received", ""), "%Y/%m/%d %H:%M:%S")
                        time_str = dt.strftime("%b %d, %Y %I:%M %p")
                    except Exception:
                        time_str = log.get("Received", "")
                    # Extended emoji selection logic for command types
                    cmd_type_lower = cmd_type.lower()
                    if "set" in cmd_type_lower:
                        emoji = "⚙️"
                    elif "edit" in cmd_type_lower:
                        emoji = "✏️"
                    elif "revert" in cmd_type_lower:
                        emoji = "↩️"
                    elif "commit-and-push" in cmd_type_lower:
                        emoji = "📤"
                    elif "commit" in cmd_type_lower:
                        emoji = "✅"
                    elif "delete" in cmd_type_lower:
                        emoji = "🗑️"
                    elif "add" in cmd_type_lower:
                        emoji = "➕"
                    elif "move" in cmd_type_lower:
                        emoji = "📦"
                    elif "rename" in cmd_type_lower:
                        emoji = "📝"
                    elif "multi-clone" in cmd_type_lower:
                        emoji = "🧬"
                    elif "multi-move" in cmd_type_lower:
                        emoji = "🛫"
                    elif "upload" in cmd_type_lower:
                        emoji = "📤"
                    elif "request" in cmd_type_lower:
                        emoji = "📥"
                    elif "clone" in cmd_type_lower:
                        emoji = "🔁"
                    elif "override" in cmd_type_lower:
                        emoji = "⛔"
                    else:
                        emoji = "📜"
                    config_section = log.get("Config Section", "")
                    if config_section and config_section.lower() != "none":
                        formatted_path = " | ".join(config_section.split())
                        label = f"{emoji} {formatted_path} | 🕒 {time_str}"
                    else:
                        label = f"{emoji} {admin} | 🕒 {time_str}"
                    hover_item = rumps.MenuItem(label, callback=lambda _, l=log: self.show_entry_details(l))
                    type_menu.add(hover_item)
                admin_menu.add(type_menu)
            self.config_log_menu.add(admin_menu)

    def build_system_log_menu(self):
        if hasattr(self.system_log_menu, 'menu'):
            self.system_log_menu.menu = {}
        groups = defaultdict(list)
        for log in self.system_logs:
            groups[log.get("Severity", "Unknown")].append(log)
        for sev, logs in groups.items():
            submenu = rumps.MenuItem(f"Severity: {sev}", callback=None)
            for log in logs:
                label = f"{log.get('Type', '')} | {log.get('Severity', '')} | {log.get('Event', '')} | {log.get('Admin', '')} | {log.get('Time', '')}"
                submenu.add(rumps.MenuItem(label, callback=lambda _, l=log: self.show_system_entry_details(l)))
            self.system_log_menu.add(submenu)

    def build_failed_commit_menu(self):
        if hasattr(self.failed_commit_menu, 'menu'):
            self.failed_commit_menu.menu = {}
        groups = defaultdict(list)
        for log in self.failed_commits:
            admin = log.get("Admin", "Unknown")
            # Skip Panorama system users if the option is enabled
            if self.hide_panorama_users and admin.startswith("Panorama-"):
                continue
            groups[admin].append(log)
        for admin, logs in groups.items():
            submenu = rumps.MenuItem(f"{admin} failed: {len(logs)}", callback=None)
            for log in logs:
                try:
                    dt = datetime.strptime(log.get("Received", ""), "%Y/%m/%d %H:%M:%S")
                    time_str = dt.strftime("%b %d, %Y %I:%M %p")
                except:
                    time_str = log.get("Received", "")
                config_section = log.get("Config Section", "")
                if config_section and config_section.lower() != "none":
                    formatted_path = " | ".join(config_section.split())
                    label = f"{formatted_path} @ {time_str}"
                else:
                    label = f"{log.get('Command Type', '')[:20]}... @ {time_str}"
                submenu.add(rumps.MenuItem(label, callback=lambda _, l=log: self.show_entry_details(l)))
            self.failed_commit_menu.add(submenu)

    def show_entry_details(self, log):
        detail = ""
        for k, v in sorted(log.items()):
            if k == "Config Section":
                formatted_path = " | ".join(v.split()) if v else ""
                detail += f"\n{k}: {formatted_path}\n\n"
            elif k == "Full Path":
                detail += f"\n{k}: {v}\n\n"
            else:
                detail += f"{k}: {v}\n"

        result = rumps.Window(
            default_text=detail,
            title="Config Log Entry Details",
            ok="Copy",
            cancel="Close",
            dimensions=(600, 400)
        ).run()

        # If user clicked "Copy", copy to clipboard
        if result.clicked == 1:  # OK button (Copy) was clicked
            import subprocess
            try:
                subprocess.run(['pbcopy'], input=detail.encode('utf-8'), check=True)
                rumps.alert("Copied", "Log details copied to clipboard!")
            except Exception as e:
                rumps.alert("Copy Failed", f"Could not copy to clipboard: {e}")

    def show_system_entry_details(self, log):
        detail = ""
        for k, v in log.items():
            if k == "Description":
                detail += f"\n{k}: {v}\n\n"
            else:
                detail += f"{k}: {v}\n"

        result = rumps.Window(
            default_text=detail,
            title="System Log Entry Details",
            ok="Copy",
            cancel="Close",
            dimensions=(600, 400)
        ).run()

        # If user clicked "Copy", copy to clipboard
        if result.clicked == 1:  # OK button (Copy) was clicked
            import subprocess
            try:
                subprocess.run(['pbcopy'], input=detail.encode('utf-8'), check=True)
                rumps.alert("Copied", "Log details copied to clipboard!")
            except Exception as e:
                rumps.alert("Copy Failed", f"Could not copy to clipboard: {e}")

    def toggle_hide_panorama_users(self, _):
        """Toggle the hide panorama users setting."""
        self.hide_panorama_users = not self.hide_panorama_users
        self.save_config()
        self.update_hide_panorama_users_menu()

        # Rebuild the config log menu to apply the filter
        self.build_config_log_menu()
        self.build_failed_commit_menu()

        status = "enabled" if self.hide_panorama_users else "disabled"
        rumps.alert("Setting Updated", f"Hide Panorama Users is now {status}")

    def update_hide_panorama_users_menu(self):
        """Update the menu item to show current state."""
        if self.hide_panorama_users:
            self.hide_panorama_users_item.title = "✓ Hide Panorama Users"
        else:
            self.hide_panorama_users_item.title = "Hide Panorama Users"

    def toggle_display_menu_icon(self, _):
        """Toggle the display menu icon setting."""
        self.display_menu_icon = not self.display_menu_icon
        self.save_config()
        self.update_display_menu_icon_menu()
        self.update_title()

        status = "enabled" if self.display_menu_icon else "disabled"
        rumps.alert("Setting Updated", f"Display Menu Icon is now {status}")

    def update_display_menu_icon_menu(self):
        """Update the menu item to show current state."""
        if self.display_menu_icon:
            self.display_menu_icon_item.title = "✓ Display Menu Icon"
        else:
            self.display_menu_icon_item.title = "Display Menu Icon"

    def update_title(self):
        """Update the menu bar title based on current settings."""
        if self.display_menu_icon:
            # Show only the icon (no text title)
            self.title = ""
        elif self.panorama:
            # Show just the Panorama name
            self.title = self.panorama
        else:
            # Show "Panorama Logs" when not connected
            self.title = "Panorama Logs"

    def force_clear_and_reload_logs(self, _):
        # Clear cache files for current panorama
        self.clear_cache_files(self.panorama)
        self.refresh_logs(None)

    def clear_credentials(self, _):
        if os.path.exists(CONFIG_PATH):
            os.remove(CONFIG_PATH)
        # Also clear all cache files
        self.clear_cache_files()
        self.api_key = ""
        self.panoramas = {}
        self.panorama_id = None
        self.panorama = ""  # Clear the panorama name
        self.update_title()  # This will show "Panorama Logs" since panorama is empty
        rumps.alert("Credentials and cache cleared. Restart app.")

    def show_about(self, _):
        cache_size = "Unknown"
        try:
            if os.path.exists(self.cache_dir):
                total_size = sum(os.path.getsize(os.path.join(self.cache_dir, f))
                                 for f in os.listdir(self.cache_dir)
                                 if os.path.isfile(os.path.join(self.cache_dir, f)))
                cache_size = f"{total_size / 1024 / 1024:.1f} MB"
        except:
            pass

        msg = f"Panorama Admin Log Viewer\nVersion: {self.VERSION}\n\nCache Directory: {self.cache_dir}\nCache Size: {cache_size}"
        rumps.alert("About - Panorama Log Viewer", msg)

    def show_help(self, _):
        help_text = """PANORAMA LOG VIEWER - HELP GUIDE

🔧 SYNC MENU:
• Refresh Logs - Downloads latest logs (default amount)
• Parse Saved Config Logs - Re-processes cached config logs
• Force Clear and Reload Logs - Deletes cache and downloads fresh logs
• Sync 5000 Logs - Downloads up to 5000 recent logs
• Sync 10000 Logs - Downloads up to 10000 recent logs (2x5000 chunks)
• Clear All Cache - Removes all cached log files

🔄 SWITCH PANORAMA MENU:
• Listed Panoramas - Switch between configured Panorama devices
• ➕ Add New Panorama - Configure a new Panorama device
• Clear Cached Credentials - Remove all saved login information

🔍 SEARCH LOGS:
• Search through config logs by keyword
• Results are exported to a text file and displayed for selection
• Click on a result to view detailed log entry information

📋 LOG VIEWING MENUS:
• Show Config Log Entries - Browse configuration changes by admin/type
• Show System Log Entries - Browse system events by severity
• Show Failed Commits - View only failed configuration commits

📊 LOG ORGANIZATION:
Config logs are organized by: Admin → Command Type → Individual Entries
System logs are organized by: Severity Level → Individual Entries
Failed commits are organized by: Admin → Failed Entries

🔍 LOG DETAILS:
Click any log entry to see full configuration path, timestamps, admin info, command type, result status, source IP and access method.

💾 CACHE SYSTEM:
Logs are cached locally for faster access with secure permissions. Use sync options to update with latest logs. Clear cache if experiencing issues.

⚙️ BEST PRACTICES:
• Use "Refresh Logs" for daily monitoring
• Use "Sync 5000/10000 Logs" for historical analysis
• Use "Search Logs" to find specific changes
• Monitor "Failed Commits" for troubleshooting

🚨 TROUBLESHOOTING:
• If logs don't appear: Try "Force Clear and Reload Logs"
• If app seems slow: Use "Clear All Cache"
• If connection fails: Check credentials in Switch Panorama"""

        rumps.alert("Help - Panorama Log Viewer", help_text)

    def switch_panorama(self, _):
        # No-op, replaced by submenu
        pass

    def build_switch_panorama_menu(self):
        """Populate the Switch Panorama submenu."""

        # COMPLETELY recreate the switch menu to prevent duplicates
        # Remove the old switch menu entirely by title
        if hasattr(self, 'switch_menu'):
            try:
                self.menu.pop("Switch Panorama", None)
            except Exception as e:
                print(f"Error removing old switch menu: {e}")

        # Create a brand new switch menu object
        self.switch_menu = rumps.MenuItem("Switch Panorama", callback=None)

        # Add panorama entries
        for name in self.panoramas.keys():
            # Mark current panorama with checkmark
            display_name = f"✓ {name}" if name == self.panorama else name
            self.switch_menu.add(
                rumps.MenuItem(display_name, callback=lambda _, n=name: self.switch_to_panorama(n))
            )

        # Add separator if there are panoramas
        if self.panoramas:
            self.switch_menu.add(rumps.separator)

        # Add the "Add New Panorama" option
        self.switch_menu.add(
            rumps.MenuItem("➕ Add New Panorama", callback=self.prompt_for_credentials)
        )

        # Add separator and clear credentials option
        self.switch_menu.add(rumps.separator)
        self.switch_menu.add(
            rumps.MenuItem("Clear Cached Credentials", callback=self.clear_credentials)
        )

        # Add the recreated menu back to the main menu
        self.menu.add(self.switch_menu)

        print(f"Rebuilt switch menu with {len(self.panoramas)} panoramas")

    def switch_to_panorama(self, name):
        """Switch the active Panorama context and refresh logs."""

        print(f"Starting switch to Panorama: {name}")

        # 1. Clear ALL cached data structures completely
        self.config_logs = []
        self.system_logs = []
        self.failed_commits = []

        # 2. Clear and recreate all log menus to prevent persistence
        self.clear_log_menus()

        # 3. Clear cache files from previous Panorama to prevent contamination
        if hasattr(self, 'panorama') and self.panorama and self.panorama != name:
            print(f"Clearing cache for previous panorama: {self.panorama}")
            # Don't delete cache files, just clear from memory
            # Cache files can be useful for quick switching back

        # 4. Update Panorama configuration
        self.panorama = name
        info = self.panoramas.get(name)
        if not isinstance(info, dict):
            rumps.alert("Invalid config", f"Configuration for {name} is corrupted. Please re-authenticate.")
            self.prompt_for_credentials()
            return

        self.api_key = info.get("api_key")
        self.panorama_id = info.get("id")
        self.save_config()
        self.update_title()

        # 5. Update the switch menu to show current selection
        self.build_switch_panorama_menu()

        print(f"Switched to Panorama: {name}, cleared all menus")

        # 6. Force a complete refresh with clean slate
        self.refresh_logs(None)


if __name__ == "__main__":
    requests.packages.urllib3.disable_warnings()
    PanoramaAdminLogAppV2().run()