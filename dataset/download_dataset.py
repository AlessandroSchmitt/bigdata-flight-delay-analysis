from pathlib import Path

import kagglehub


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "raw"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

path = kagglehub.dataset_download(
    "hrishitpatil/flight-data-2024",
    output_dir=str(OUTPUT_DIR),
)

print(f"Dataset downloaded to: {path}")
