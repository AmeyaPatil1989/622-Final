import sqlite3
from typing import Dict, List

class User:
    """
    Maps to the USER table in 622_final.db, which has columns:
      - user_id (TEXT PRIMARY KEY)
      - username (TEXT)
      - pw       (TEXT)
    """
    def __init__(self, db_path: str = "622_final.db"):
        self.db_path = db_path

    def create(self, user_id: str, username: str, pw: str):
        """INSERT a new user record."""
        sql = "INSERT INTO USER (user_id, username, pw) VALUES (?, ?, ?)"
        self._run(sql, (user_id, username, pw))

    def get_by_id(self, user_id: str) -> Dict:
        """SELECT one user by user_id."""
        sql = "SELECT user_id, username, pw FROM USER WHERE user_id = ?"
        return self._fetchone(sql, (user_id,))

    def get_by_username(self, username: str) -> Dict:
        """SELECT one user by username."""
        sql = "SELECT user_id, username, pw FROM USER WHERE username = ?"
        return self._fetchone(sql, (username,))

    def update_password(self, user_id: str, new_pw: str):
        """UPDATE the pw field for a given user_id."""
        sql = "UPDATE USER SET pw = ? WHERE user_id = ?"
        self._run(sql, (new_pw, user_id))

    def delete(self, user_id: str):
        """DELETE a user by user_id."""
        sql = "DELETE FROM USER WHERE user_id = ?"
        self._run(sql, (user_id,))

    def list_all(self) -> List[Dict]:
        """RETURN all users as a list of dicts."""
        sql = "SELECT user_id, username, pw FROM USER"
        return self._fetchall(sql)

    # ─── Private helpers ───────────────────────────────────────────────────

    def _run(self, sql: str, params: tuple = ()):
        """Execute INSERT/UPDATE/DELETE and commit."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(sql, params)
            conn.commit()

    def _fetchone(self, sql: str, params: tuple = ()) -> Dict:
        """Execute SELECT and return a single row as dict (or empty)."""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return {}
            cols = [col[0] for col in cur.description]
            return dict(zip(cols, row))

    def _fetchall(self, sql: str, params: tuple = ()) -> List[Dict]:
        """Execute SELECT and return all rows as list of dicts."""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(sql, params)
            rows = cur.fetchall()
            cols = [col[0] for col in cur.description]
            return [dict(zip(cols, r)) for r in rows]