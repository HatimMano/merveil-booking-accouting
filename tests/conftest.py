"""pytest configuration: add the project root to sys.path."""

import sys
from pathlib import Path

# Ensure the project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent))
