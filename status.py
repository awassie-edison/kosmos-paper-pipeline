"""Pipeline status tracking — writes status.json for external monitoring."""

import json
import logging
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)


class StatusTracker:
    """Append-only status log written to status.json."""

    def __init__(self, path: Path):
        self.path = path
        if self.path.exists():
            with open(self.path) as f:
                self._data = json.load(f)
        else:
            self._data = {"current": {}, "history": []}

    def update(self, step: str, message: str, details: dict | None = None):
        entry = {
            "current_step": step,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "details": details or {},
        }
        self._data["current"] = entry
        self._data["history"].append(entry)
        self._write()
        log.info("[%s] %s %s", step, message, details or "")

    def _write(self):
        with open(self.path, "w") as f:
            json.dump(self._data, f, indent=2)
