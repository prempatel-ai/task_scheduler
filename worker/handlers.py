"""Job execution handlers."""

import os
import time
from typing import Any, Callable, Dict


def count_words(payload: str, job_id: int) -> int:
    """Deterministic word counter handler."""
    if os.getenv("CRASH_AT") == "mid_job":
        os._exit(1)

    output_dir = os.getenv("OUTPUTS_DIR", "outputs")
    os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(payload):
        with open(payload, "r", encoding="utf-8") as f:
            text = f.read()
    else:
        text = payload

    word_count = len(text.split())
    out_file = os.path.join(output_dir, f"{job_id}.txt")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(str(word_count))
    return word_count


def sleep_handler(payload: str, job_id: int) -> str:
    """Sleep handler for testing fault injection, timeouts, and process kill."""
    if os.getenv("CRASH_AT") == "mid_job":
        os._exit(1)

    default_sleep = float(os.getenv("JOB_SLEEP", "10"))
    secs = float(payload) if (payload and payload.strip()) else default_sleep
    time.sleep(secs)

    output_dir = os.getenv("OUTPUTS_DIR", "outputs")
    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, f"{job_id}.txt")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("done")
    return "done"


def always_crash(payload: str, job_id: int):
    """Handler that immediately crashes via os._exit(1)."""
    os._exit(1)


HANDLERS: Dict[str, Callable[[str, int], Any]] = {
    "count_words": count_words,
    "sleep": sleep_handler,
    "always_crash": always_crash,
}


def get_handler(job_type: str) -> Callable[[str, int], Any]:
    """Resolves handler function for a job type."""
    if job_type in HANDLERS:
        return HANDLERS[job_type]
    raise ValueError(f"Unknown job type: {job_type}")
