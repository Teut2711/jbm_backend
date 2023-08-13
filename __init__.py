import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv

load_dotenv()

db_config = {
    "username": os.getenv("BUCEPHALUS__TIMESCALEDB__USERNAME"),
    "password": os.getenv("BUCEPHALUS__TIMESCALEDB__PASSWORD"),
    "host": os.getenv("BUCEPHALUS__TIMESCALEDB__HOSTNAME"),
    "port": os.getenv("BUCEPHALUS__TIMESCALEDB__PORTNUMBER"),
    "database": os.getenv("BUCEPHALUS__TIMESCALEDB__DATABASE"),
}

DATABASE_URL = f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL

db = SQLAlchemy(app)
with app.app_context():
    db.Model.metadata.reflect(db.engine)
