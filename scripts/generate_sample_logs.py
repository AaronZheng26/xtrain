from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


NORMAL_PATHS = [
    "/",
    "/login",
    "/api/profile",
    "/api/metrics",
    "/dashboard",
    "/assets/app.js",
    "/healthz",
]

ANOMALY_PATHS = [
    "/wp-admin",
    "/api/admin/export",
    "/.env",
    "/../../etc/passwd",
    "/vendor/phpunit/phpunit/src/Util/PHP/eval-stdin.php",
]

USER_AGENTS = [
    "Mozilla/5.0",
    "curl/8.5.0",
    "python-requests/2.32.3",
    "Go-http-client/1.1",
]


def build_row(index: int, start_time: datetime) -> dict[str, object]:
    anomaly = index % 10 == 0
    event_time = start_time + timedelta(seconds=index * random.randint(1, 3))
    source_ip = f"10.0.{random.randint(1, 8)}.{random.randint(2, 254)}"
    dest_ip = f"172.16.0.{random.randint(10, 30)}" if not anomaly or index % 25 else ""
    method = random.choice(["GET", "POST", "PUT"])
    path = random.choice(ANOMALY_PATHS if anomaly else NORMAL_PATHS)
    status_code = random.choice([401, 403, 404, 500]) if anomaly else random.choice([200, 200, 200, 201, 204])
    request_duration = round(random.uniform(1.2, 4.8), 3) if anomaly else round(random.uniform(0.05, 1.2), 3)
    bytes_sent = random.randint(8000, 50000) if anomaly else random.randint(120, 8000)
    error_count = random.randint(2, 8) if anomaly else random.randint(0, 1)
    auth_failures = random.randint(3, 10) if anomaly else random.randint(0, 2)
    severity = "high" if anomaly else random.choice(["low", "medium"])
    message = (
        f"Suspicious request burst detected for {path}"
        if anomaly
        else f"Request served normally for {path}"
    )

    return {
        "timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "source_ip": source_ip,
        "dest_ip": dest_ip,
        "method": method,
        "path": path,
        "status_code": status_code,
        "bytes_sent": bytes_sent,
        "request_duration": request_duration,
        "error_count": error_count,
        "auth_failures": auth_failures,
        "user_agent": random.choice(USER_AGENTS),
        "severity": severity,
        "message": message,
        "label": "anomaly" if anomaly else "normal",
    }


def generate_dataset(rows: int) -> list[dict[str, object]]:
    random.seed(42)
    start_time = datetime(2026, 4, 7, 9, 0, 0)
    return [build_row(index, start_time) for index in range(rows)]


def write_csv(output_path: Path, rows: list[dict[str, object]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sample security log CSV data.")
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Number of rows to generate.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("storage") / "samples" / "security_logs_1000.csv",
        help="Output CSV path.",
    )
    args = parser.parse_args()

    rows = generate_dataset(max(args.rows, 1))
    write_csv(args.output, rows)
    anomaly_count = sum(1 for row in rows if row["label"] == "anomaly")
    print(
        {
            "output": str(args.output),
            "rows": len(rows),
            "anomalies": anomaly_count,
            "normals": len(rows) - anomaly_count,
        }
    )


if __name__ == "__main__":
    main()
