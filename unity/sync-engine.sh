#!/usr/bin/env bash
# Rebuild FotP.Engine targeting netstandard2.1 and copy DLL into Unity project.
set -e
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
dotnet build "$REPO_ROOT/src/FotP.Engine/FotP.Engine.csproj" -c Release -f netstandard2.1
cp "$REPO_ROOT/src/FotP.Engine/bin/Release/netstandard2.1/FotP.Engine.dll" \
   "$REPO_ROOT/unity/Assets/Plugins/FotP.Engine.dll"
echo "FotP.Engine.dll synced to unity/Assets/Plugins/"
