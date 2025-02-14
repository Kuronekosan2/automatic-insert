import psycopg2
from psycopg2 import sql
import bcrypt

class SqlFunction:
    def __init__(self, db_name, db_user, db_pass, db_host, db_port):
        """Ini konstructor le."""
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_host = db_host
        self.db_port = db_port
        
    def _connect_db(self):
        """Func internal buat konek db."""
        return psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_pass,
            host=self.db_host,
            port=self.db_port
        )

    def _fetch_as_dict(self, cur, query, params=None) -> dict:
        """Helper buat extract tuple jadi dict dan passing as return."""
        cur.execute(query, params or ())
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in cur.fetchall()]
        return results
    
    def create_data(self, table_name: str, columns: list, values: list) -> dict | bool:
        """Insert data."""
        conn = self._connect_db()
        cur = conn.cursor()

        query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING *;").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() * len(values))
        )

        cur.execute(query, values)
        result = cur.fetchone()
        columns = [desc[0] for desc in cur.description]
        conn.commit()
        
        cur.close()
        conn.close()
        return dict(zip(columns, result)) if result else None

    def reads_data(self, table_name: str, filter_column: str = None, filter_value: str = None, operator: str = '=') -> dict:
        """Reads data."""
        conn = self._connect_db()
        cur = conn.cursor()

        if filter_column and filter_value:
            query = sql.SQL("SELECT * FROM {} WHERE {} {} %s;").format(
                sql.Identifier(table_name),
                sql.Identifier(filter_column),
                sql.SQL(operator)
            )
            results = self._fetch_as_dict(cur, query, (filter_value,))
        else:
            query = sql.SQL("SELECT * FROM {};").format(sql.Identifier(table_name))
            results = self._fetch_as_dict(cur, query)

        conn.commit()
        cur.close()
        conn.close()

        return results
            
    def read_data(self, table_name: str, filter_column: str, filter_value: str, operator: str = '=') -> dict | bool:
        """Read data."""
        conn = self._connect_db()
        cur = conn.cursor()

        query = sql.SQL("SELECT * FROM {} WHERE {} {} %s LIMIT 1;").format(
            sql.Identifier(table_name),
            sql.Identifier(filter_column),
            sql.SQL(operator)
        )
        cur.execute(query, (filter_value,))

        result = cur.fetchone()
        columns = [desc[0] for desc in cur.description]
        
        conn.commit()
        cur.close()
        conn.close()
        
        return dict(zip(columns, result)) if result else None

    def update_data(self, table_name: str, update_columns: list, update_values: list, filter_column: str, filter_value: str, operator: str = '=') -> dict | bool:
        """Update data."""
        conn = self._connect_db()
        cur = conn.cursor()

        update_columns.append("updated_at")
        
        set_clause = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(col)) for col in update_columns[:-1]
        )
        
        set_clause = set_clause + sql.SQL(", updated_at = timezone('Asia/Bangkok', NOW())")

        query = sql.SQL("UPDATE {} SET {} WHERE {} {} %s RETURNING *;").format(
            sql.Identifier(table_name),
            set_clause,
            sql.Identifier(filter_column),
            sql.SQL(operator)
        )

        if not update_values:
            raise ValueError("update_values cannot be empty!")

        cur.execute(query, (*update_values, filter_value))

        result = cur.fetchone()
        columns = [desc[0] for desc in cur.description]

        conn.commit()
        cur.close()
        conn.close()

        return dict(zip(columns, result)) if result else None

    def delete_data(self, table_name: str, filter_column: str, filter_value: str, operator: str = '=') -> dict | bool:
        """Delete data."""
        conn = self._connect_db()
        cur = conn.cursor()

        query = sql.SQL("DELETE FROM {} WHERE {} {} %s RETURNING *;").format(
            sql.Identifier(table_name),
            sql.Identifier(filter_column),
            sql.SQL(operator)
        )

        cur.execute(query, (filter_value,))
        result = cur.fetchone()
        columns = [desc[0] for desc in cur.description]
        
        conn.commit()
        cur.close()
        conn.close()

        return dict(zip(columns, result)) if result else None
            
    def hash_password(self, password: str) -> str:
        """Hash password pake bcrypt."""
        return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

# EYYO NIGGA, THIS SCRIPT CREATED BY KURONEKOSAN, DONT TOUCH IT!, UNLESS YOU KNOW WHAT YOU ARE DOING.