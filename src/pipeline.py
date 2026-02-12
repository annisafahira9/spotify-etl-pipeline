from __future__ import annotations

from src.db import connect, migrate, get_db_path

def main() -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    migrate(conn)
    print(f"DB ready at: {db_path}")

if __name__ == "__main__":
    main()
