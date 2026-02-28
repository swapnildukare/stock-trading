#!/bin/bash

# Setup script for Trading development environment
# This script:
# 1. Checks if UV is installed (installs if missing)
# 2. Installs dependencies with UV
# 3. Activates the virtual environment
# 4. Runs the swing trading scanner

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

msg_fail() {
    echo -e "${RED}❌ $1${NC}"
}

msg_yellow() {
    echo -e "${YELLOW}$1${NC}"
}

msg_green() {
    echo -e "${GREEN}$1${NC}"
}

msg_green "🚀 Setting up Swing Trading development environment"

# Safe exit: use 'return' when sourced so the parent shell isn't killed
_bail() { msg_fail "$1"; return 1 2>/dev/null || exit 1; }

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" \
    || { _bail "Failed to determine script directory"; return 1; }

cd "$SCRIPT_DIR"

# Use 'trader' as the virtual environment name
export UV_PROJECT_ENVIRONMENT=trader

# Check if UV is installed
if ! command -v uv &>/dev/null; then
    msg_yellow "📦 UV not found. Installing UV..."

    curl -LsSf https://astral.sh/uv/install.sh | sh \
        || { _bail "Failed to install UV"; return 1; }

    export PATH="$HOME/.local/bin:$PATH"
fi

# Verify UV is now available
command -v uv &>/dev/null \
    || { _bail "UV is not available after installation"; return 1; }

# Install dependencies
msg_yellow "📦 Installing dependencies with UV..."
uv sync \
    || { _bail "Dependency installation failed"; return 1; }

# Activate virtual environment
msg_green "✅ Dependencies installed successfully!"
msg_yellow "🔌 Activating virtual environment..."

source trader/bin/activate \
    || { _bail "Failed to activate virtual environment"; return 1; }

msg_green "🎉 Environment setup complete!"
msg_yellow "💡 The virtual environment is now active."
msg_yellow "💡 To deactivate, run: deactivate"
msg_yellow "💡 To reactivate later, run: source trader/bin/activate"
