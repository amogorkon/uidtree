from pathlib import Path

import uuid


unique_index_path = Path(__file__) / f"tmp/bplustree-testfile-{uuid.uuid4()}.index"
wal_path = unique_index_path.with_name(f"{unique_index_path.name}-wal")
