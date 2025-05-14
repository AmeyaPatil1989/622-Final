import sqlite3
from typing import List, Dict

class AccountType:
    """
    Maps to the ACCOUNT_TYPE table in 622_final.db, which has columns:
      - account_type (TEXT PRIMARY KEY)
      - type_name    (TEXT)
    """
    def __init__(self, db_path: str = "622_final.db"):
        self.db_path = db_path

    def create(self, account_type: str, type_name: str):
        """INSERT a new account type."""
        sql = "INSERT INTO ACCOUNT_TYPE (account_type, type_name) VALUES (?, ?)"
        self._run(sql, (account_type, type_name))

    def get_by_id(self, account_type: str) -> Dict:
        """SELECT one record by primary key account_type."""
        sql = "SELECT account_type, type_name FROM ACCOUNT_TYPE WHERE account_type = ?"
        return self._fetchone(sql, (account_type,))

    def get_by_name(self, type_name: str) -> Dict:
        """SELECT one record by type_name."""
        sql = "SELECT account_type, type_name FROM ACCOUNT_TYPE WHERE type_name = ?"
        return self._fetchone(sql, (type_name,))

    def update_name(self, account_type: str, new_name: str):
        """UPDATE the type_name for a given account_type."""
        sql = "UPDATE ACCOUNT_TYPE SET type_name = ? WHERE account_type = ?"
        self._run(sql, (new_name, account_type))

    def delete(self, account_type: str):
        """DELETE an account type by account_type key."""
        sql = "DELETE FROM ACCOUNT_TYPE WHERE account_type = ?"
        self._run(sql, (account_type,))

    def list_all(self) -> List[Dict]:
        """RETURN all account types as a list of dicts."""
        sql = "SELECT account_type, type_name FROM ACCOUNT_TYPE"
        return self._fetchall(sql)

    # ─── Private helpers ───────────────────────────────────────────────────

    def _run(self, sql: str, params: tuple = ()):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(sql, params)
            conn.commit()

    def _fetchone(self, sql: str, params: tuple = ()) -> Dict:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return {}
            cols = [col[0] for col in cur.description]
            return dict(zip(cols, row))

    def _fetchall(self, sql: str, params: tuple = ()) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(sql, params)
            rows = cur.fetchall()
            cols = [col[0] for col in cur.description]
            return [dict(zip(cols, r)) for r in rows]