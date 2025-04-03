import sqlite3

# Connect to your SQLite database
conn = sqlite3.connect("rosteriq.db")
cursor = conn.cursor()

# Get column names
table_name = "Player_Seasons"
cursor.execute(f"PRAGMA table_info({table_name})")
columns = [column[1] for column in cursor.fetchall()]

# Print column names
print(columns)

table_name = "Team_Seasons"
cursor.execute(f"PRAGMA table_info({table_name})")
columns = [column[1] for column in cursor.fetchall()]

print(columns)
# Close connection
conn.close()
