"""Job execution handlers."""

import os
import time
from typing import Any, Callable, Dict


def count_words(payload: str, job_id: int) -> int:
    """Deterministic word counter handler.

    Counts words in payload (or file path if existing) and writes count to outputs/{job_id}.txt.
    """
    if os.path.exists(payload):
        with open(payload, "r", encoding="utf-8") as f:
            text = f.read()
    else:
        text = payload

    word_count = len(text.split())
    os.makedirs("outputs", exist_ok=True)
    with open(os.path.join("outputs", f"{job_id}.txt"), "w", encoding="utf-8") as f:
        f.write(str(word_count))
    return word_count


def sleep_handler(payload: str, job_id: int) -> str:
    """Sleep handler for testing fault injection, timeouts, and process kill."""
    secs = float(payload) if payload else 10.0
    time.sleep(secs)
    os.makedirs("outputs", exist_ok=True)
    with open(os.path.join("outputs", f"{job_id}.txt"), "w", encoding="utf-8") as f:
        f.write("done")
    return "done"


HANDLERS: Dict[str, Callable[[str, int], Any]] = {
    "count_words": count_words,
    "sleep": sleep_handler,
}


def get_handler(job_type: str) -> Callable[[str, int], Any]:
    """Resolves handler function for a job type."""
    if job_type in HANDLERS:
        return HANDLERS[job_type]
    raise ValueError(f"Unknown job type: {job_type}")
