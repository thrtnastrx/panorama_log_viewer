# Panorama Log Viewer

A macOS menu bar application for viewing Palo Alto Panorama logs.  View recent changes sorted by admin, view failed commits by admin, auto copy and export to file.  Quickly search logs for keywords and export to file, or view in the app.  

## Features

- 🔄 **Multi-Panorama Support** - Manage multiple Panorama devices
- 📊 **Smart Log Organization** - Browse logs by admin, command type, and severity
- 🔍 **Advanced Search** - Find specific configuration changes quickly
- 📋 **Copy Functionality** - Copy log details to clipboard
- ⚙️ **Customizable Options** - Hide system users, icon-only mode
- 💾 **Secure Caching** - Local storage with encrypted credentials
- 🚀 **Batch Sync** - Download up to 10,000 logs in chunks

## Installation

1. Download the latest release from the [Releases](../../releases) page
2. Unzip and drag **Panorama Log Viewer.app** to your Applications folder
3. Launch the app - it will appear in your menu bar

## Usage
- **Note** - If options appear "greyed out" it's because there is an open window somewhere.

### First Setup
1. Click the menu bar icon
2. Go to **Switch Panorama** → **➕ Add New Panorama**
3. Enter your Panorama URL, username, and password
4. The app will automatically fetch and display your logs

### Daily Use
- **Refresh Logs** - Get the latest log entries (~500 logs)
- **Sync 5000/10000 Logs** - Download extended history (May take up to 2 min)
- **Search Logs** - Find specific configuration changes
- **Options** - Customize display and filtering

## System Requirements

- macOS 10.14 or later
- Network access to Panorama devices
- Valid Panorama admin credentials

## Version

Current version: 1.2.023

## License

MIT License - see [LICENSE](LICENSE) file for details.
