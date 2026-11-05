@echo off
chcp 65001 >nul
title FamilyBudget GitHub-Secrets - FGSeRaser
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0GITHUB_SECRETS_EINRICHTEN.ps1"
