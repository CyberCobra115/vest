from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

db = SQLAlchemy()
migrate = Migrate()


def create_app(config_name: str = "development") -> Flask:
    app = Flask(__name__)

    from app.config import config_map
    app.config.from_object(config_map[config_name])

    db.init_app(app)
    migrate.init_app(app, db)

    from app.routes.ingest import bp as ingest_bp
    from app.routes.positions import bp as positions_bp
    from app.routes.compliance import bp as compliance_bp
    from app.routes.reconciliation import bp as reconciliation_bp

    app.register_blueprint(ingest_bp)
    app.register_blueprint(positions_bp)
    app.register_blueprint(compliance_bp)
    app.register_blueprint(reconciliation_bp)

    return app
