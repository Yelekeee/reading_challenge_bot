#!/bin/bash
# Run this script once on your Oracle Cloud server to set up the bot.
# Usage: bash deploy.sh

set -e

echo "==> Installing system dependencies..."
sudo apt update && sudo apt install -y python3 python3-pip python3-venv git

echo "==> Cloning repository..."
# Replace the URL below with your actual GitHub repo URL
git clone https://github.com/YOUR_USERNAME/reading_challenge_bot.git
cd reading_challenge_bot

echo "==> Creating .env file..."
echo "BOT_TOKEN=your_token_here" > .env
echo "DATABASE_PATH=/home/ubuntu/reading_challenge_bot/challenge.db" >> .env
echo ""
echo "  !! Edit .env now and paste your real BOT_TOKEN, then press Enter to continue."
read -r

echo "==> Creating Python virtual environment..."
python3 -m venv .venv
.venv/bin/pip install --upgrade pip -q
.venv/bin/pip install -r requirements.txt -q

echo "==> Installing systemd service..."
sudo cp reading_challenge_bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable reading_challenge_bot
sudo systemctl start reading_challenge_bot

echo ""
echo "==> Done! Bot is running. Check status with:"
echo "    sudo systemctl status reading_challenge_bot"
echo "    sudo journalctl -u reading_challenge_bot -f"
