from .app import db


class TraccerPositions(db.Model):
    __table__ = db.Model.metadata.tables["tc_devices"]
