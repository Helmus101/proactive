# Weave Todo App

An Electron application with Chrome extension integration that uses AI to generate Weave to-do lists based on your browsing data, emails, and calendar events.

## Features

- **Google OAuth Integration**: Connect your Google account to access Gmail and Calendar data
- **Chrome Extension Data Collection**: Automatically collects browsing data for AI analysis
- **AI-Powered Task Generation**: Uses DeepSeek AI to analyze your data and create proactive to-do lists
- **Voice-to-Agent (VTA) Control**: Global hotkey voice input that can drive an adaptive desktop automation loop
- **No Database Required**: Uses local storage for data persistence
- **Task Execution**: Execute tasks directly from the app
- **Real-time Sync**: Seamless data sync between extension and desktop app

## Installation

### Prerequisites

- Node.js (v16 or higher)
- npm or yarn
- Google OAuth credentials
- DeepSeek API key

### Setup

1. **Clone and Install Dependencies**
```bash
cd weave
npm install
```

2. **Environment Configuration**
```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret
DEEPSEEK_API_KEY=your_deepseek_api_key
OPENAI_API_KEY=your_openai_api_key
ANTHROPIC_API_KEY=your_anthropic_api_key
COMPUTER_USE_PROVIDER=anthropic
COMPUTER_USE_MODEL=claude-3-5-sonnet-20241022
```

For local-first speech-to-text (for whisper.cpp / WhisperKit wrappers), optionally add:
```
VOICE_LOCAL_STT_COMMAND=/absolute/path/to/your/stt-wrapper
VOICE_LOCAL_STT_ARGS_JSON=["--input","{{audio_path}}","--format","json"]
VOICE_LOCAL_STT_TIMEOUT_MS=20000
```

3. **Google OAuth Setup**
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select existing one
   - Enable Google+ API, Gmail API, and Calendar API
   - Create OAuth 2.0 credentials for a "Desktop app"
   - Add `http://localhost:3001/oauth2callback` to authorized redirect URIs
   - Copy Client ID and Client Secret to your `.env` file

4. **DeepSeek API Setup**
   - Sign up at [DeepSeek](https://platform.deepseek.com/)
   - Get your API key
   - Add it to your `.env` file

## Usage

### Running the App

```bash
# Development mode
npm run dev

# Production mode
npm start
```

### Installing the Chrome Extension

1. Open Chrome and go to `chrome://extensions/`
2. Enable "Developer mode"
3. Click "Load unpacked"
4. Select the `chrome-extension` directory from this project
5. The extension will start collecting browsing data automatically

### Using the App

1. **Connect Google Account**: Click "Connect Google Account" to authenticate
2. **Sync Data**: Use "Sync Data" to collect emails and calendar events
3. **Generate Tasks**: Click "Generate Proactive Todos" to analyze your data with AI
4. **Manage Tasks**: View, prioritize, and execute tasks from the main dashboard

## Architecture

### Components

- **Electron Main Process** (`main.js`): Handles OAuth, data storage, and AI integration
- **Renderer Process** (`renderer/`): Frontend UI built with HTML/CSS/JavaScript
- **Chrome Extension** (`chrome-extension/`): Collects browsing data
- **Preload Script** (`preload.js`): Secure bridge between main and renderer processes
- **Desktop Agent Layer** (`services/desktop-control.js`, `services/agent/agentPlanner.js`): Adaptive observe→plan→act loop with AX tree + optional screenshot fallback

### Voice-to-Agent Pipeline

The app now supports a hybrid VTA flow inspired by "Wispr Flow + Manus":

1. Global voice shortcut starts a session and floating HUD.
2. Speech is transcribed with local-first strategy:
   - Browser native speech recognition when available.
   - Local STT adapter command (optional, e.g. whisper.cpp wrapper).
   - OpenAI cloud transcription fallback.
3. Transcript is converted into a desktop goal and sent to the adaptive agent loop.
4. Before execution, the app builds a JSON execution plan (bootstrap apps + loop settings).
5. Target apps and browser surfaces are pre-launched in the background via AppleScript/open.
6. Agent iterates: observe UI state -> plan next atomic action -> execute -> re-observe.
7. Observation uses DOM/CDP in managed browser and Accessibility tree in native apps.
8. The loop stops only after visible completion checks pass.

### Data Flow

1. Chrome extension collects browsing data
2. Extension sends data to Electron app via HTTP endpoint
3. Electron app stores data locally using electron-store
4. User triggers AI analysis
5. DeepSeek API processes data and generates tasks
6. Tasks are displayed and can be executed

### Data Storage

The app uses `electron-store` for local data persistence:
- Google OAuth tokens
- User data (emails, calendar, browsing)
- Generated tasks
- Extension settings

## Security Considerations

- OAuth tokens are stored locally and encrypted
- No sensitive data is sent to external services except AI API
- Chrome extension respects user privacy settings
- Data collection can be disabled via extension popup

## Development

### Project Structure

```
weave/
├── main.js                 # Electron main process
├── preload.js             # Preload script
├── package.json           # Dependencies and scripts
├── renderer/              # Frontend application
│   ├── index.html        # Main UI
│   ├── styles.css        # Styles
│   └── app.js           # Frontend logic
├── chrome-extension/      # Chrome extension
│   ├── manifest.json     # Extension manifest
│   ├── background.js     # Background script
│   ├── content.js        # Content script
│   ├── popup.html        # Extension popup
│   ├── popup.js          # Popup logic
│   └── icons/            # Extension icons
└── README.md             # This file
```

### Building for Production

```bash
npm run build
```

This creates a distributable package in the `dist/` directory.

## Troubleshooting

### Common Issues

1. **OAuth Fails**: Check that your redirect URI matches exactly in Google Console
2. **Extension Not Connecting**: Ensure the Electron app is running on localhost:3001
3. **AI API Fails**: Verify your DeepSeek API key is valid and has credits
4. **Data Not Syncing**: Check browser console for extension errors

### Debug Mode

Run with `npm run dev` to open DevTools automatically.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

MIT License - see LICENSE file for details.
