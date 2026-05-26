---
title: Twickell
emoji: 🤖
colorFrom: purple
colorTo: yellow
sdk: docker
pinned: false
app_port: 7860
storage: small
---

My Orby — twickell.com

This Space uses HuggingFace Persistent Storage (small tier, ~$5/mo)
mounted at /data. Set the env var ORBI_DATA_DIR=/data in the Space
settings so the app reads/writes the persistent volume — otherwise
data is wiped on every container restart.
