
class QueryExecutor:
    def __init__(self, cursor):
        self.cursor = cursor
        
    def _execute_query(self, query: str) -> list:
        self.cursor.execute(query)
        
        columns = [desc[0] for desc in self.cursor.description]
        
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
    
    def get_students_per_room(self) -> list:
        query = """
            SELECT r.name, COUNT(s.id) AS students_count
            FROM rooms r
            LEFT JOIN students s ON r.id = s.room_id
            GROUP BY r.name
            ORDER BY students_count DESC;
        """
        return self._execute_query(query)
    
    def get_top5_rooms_by_min_avg_age(self) -> list:
        query = """
            SELECT 
                r.name,
                AVG(EXTRACT(YEAR FROM AGE(NOW(), s.birthday))) AS avg_age
            FROM rooms r
            JOIN students s ON r.id = s.room_id
            GROUP BY r.name
            ORDER BY avg_age ASC
            LIMIT 5
        """
        return self._execute_query(query)
    
    def get_top5_rooms_by_max_age_diff(self) ->list:
        query = """
            SELECT 
                r.name,
                MAX(s.birthday) - MIN(s.birthday) AS age_diff_days
            FROM rooms r
            JOIN students s ON r.id = s.room_id
            GROUP BY r.name
            HAVING COUNT(s.id) > 1
            ORDER BY age_diff_days DESC
            LIMIT 5
        """
        return self._execute_query(query)
    
    def get_rooms_with_mixed_genders(self) -> list:
        query = """
            SELECT r.name
            FROM rooms r
            JOIN students s ON r.id = s.room_id
            GROUP BY r.name
            HAVING COUNT(DISTINCT s.sex) > 1
        """
        return self._execute_query(query)