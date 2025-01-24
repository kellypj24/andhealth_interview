import os
from pathlib import Path


def check_json_start(file_path: str):
    """Examine the beginning of the JSON file"""
    path = Path(file_path)
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"\nFile size: {size_mb:.1f} MB")

    with path.open("rb") as file:
        # Read first 1000 bytes and decode
        start_content = file.read(1000).decode("utf-8", errors="replace")
        print("\nFirst 1000 bytes of content:")
        print(start_content)

        # Check for common JSON formatting issues
        print("\nBasic checks:")
        print(f"Starts with curly brace: {start_content.lstrip().startswith('{')}")
        print(f"Contains newlines: {'\\n' in start_content}")

        # Count opening/closing braces in sample
        print(f"Opening curly braces: {start_content.count('{')}")
        print(f"Closing curly braces: {start_content.count('}')}")
        print(f"Opening square brackets: {start_content.count('[')}")
        print(f"Closing square brackets: {start_content.count(']')}")


if __name__ == "__main__":
    file_path = "/Users/pjkelly/andhealth_interview/OPA_CE_DAILY_PUBLIC.JSON"
    check_json_start(file_path)
