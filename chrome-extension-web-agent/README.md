# Web Agent Chrome Extension (DeepSeek)

This extension is the DeepSeek-powered browser agent copied from your ZIP and ready to load in Chrome.

## Install (Unpacked)

1. Open `chrome://extensions/`
2. Enable **Developer mode**
3. Click **Load unpacked**
4. Select this folder:
   - `<project-root>/chrome-extension-web-agent`

## Configure DeepSeek

1. Click the extension icon
2. Open **Settings**
3. Paste your **DeepSeek API key**
4. Keep model as `deepseek-chat` (default), or choose another supported model
5. Save settings

The extension background script calls DeepSeek at:
- `https://api.deepseek.com/chat/completions`
