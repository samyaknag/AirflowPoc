from flask import Flask, request, redirect, url_for, render_template
import sqlite3

app = Flask(__name__)

DATABASE = '/home/ec2-user/airflow/airflow.db'

# Initialize SQLite database
def init_db():
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS complaints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                description TEXT NOT NULL,
                status TEXT DEFAULT 'New',
                category TEXT,
                assignee TEXT
            )
        ''')
    print("Database initialized.")

@app.route('/')
def index():
    return render_template('complaint_form.html')

@app.route('/submit', methods=['GET', 'POST'])
def submit_complaint():
    if request.method == 'POST':
        name = request.form['name']
        email = request.form['email']
        description = request.form['description']

        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO complaints (name, email, description)
                VALUES (?, ?, ?)
            ''', (name, email, description))
            conn.commit()

        return redirect(url_for('view_complaints'))

    return render_template('submit_complaint.html')

@app.route('/view')
def view_complaints():
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, email, description, status FROM complaints")
        complaints = cursor.fetchall()
    return render_template('view_complaints.html', complaints=complaints)

@app.route('/search', methods=['GET', 'POST'])
def search_complaints():
    complaints = []
    if request.method == 'POST':
        search_keyword = request.form['search_keyword']
        search_status = request.form['search_status']

        with sqlite3.connect(DATABASE) as conn:
            cursor = conn.cursor()

            query = '''
                SELECT id, name, email, description, status 
                FROM complaints 
                WHERE (description LIKE ? OR name LIKE ? OR email LIKE ?)
            '''
            params = [f'%{search_keyword}%', f'%{search_keyword}%', f'%{search_keyword}%']
            
            if search_status:
                query += " AND status = ?"
                params.append(search_status)

            cursor.execute(query, params)
            complaints = cursor.fetchall()

    return render_template('search_complaints.html', complaints=complaints)

if __name__ == '__main__':
    init_db()
    app.run(debug=True)
