import os
from app import create_app, db

app = create_app(os.getenv("FLASK_ENV", "development"))

# Auto-create all tables on startup if they don't exist.
# This replaces the need to run `flask db upgrade` manually in development.
with app.app_context():
    db.create_all()

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
