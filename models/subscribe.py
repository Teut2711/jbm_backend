from pathlib import Path
import cantools
from datetime import datetime, timedelta
import os
import sys
import json
import argparse

# import daemon
import logging
import threading
import redis
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    BigInteger,
    String,
    Float,
    DateTime,
    Numeric,
    Boolean,
    text,
    func,
    Sequence,
    Index,
    PrimaryKeyConstraint,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, TEXT
from sqlalchemy.orm import scoped_session, sessionmaker, declarative_base
import paho.mqtt.client as mqtt
import ssl
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
# Define the SQLAlchemy model for the PostgreSQL tables
Base = declarative_base()


class CANFrame(Base):
    __tablename__ = "CAN_frames"
    __table_args__ = (
        PrimaryKeyConstraint("IMEI", "timestamp"),
        {"schema": "public"},
    )

    IMEI = Column(BigInteger, primary_key=True)
    # timestamp = Column(DateTime(timezone=True), primary_key=True)
    timestamp = Column(DateTime(timezone=False), primary_key=True)
    identifier = Column(TEXT, nullable=False)
    data = Column(TEXT, nullable=False)
    fields = Column(JSONB)
    createdAt = Column(DateTime(timezone=True), server_default=func.now())


class traccar_positions(Base):
    __tablename__ = "tc_positions"
    __table_args__ = (
        Index("position_deviceid_fixtime", "deviceid", "fixtime"),
        {"schema": "traccar"},
    )

    id = Column(
        Integer,
        Sequence("tc_positions_id_seq", schema="traccar"),
        primary_key=True,
    )
    protocol = Column(String(128))
    deviceid = Column(Integer, nullable=False)
    servertime = Column(DateTime, server_default=func.now(), nullable=False)
    devicetime = Column(DateTime, nullable=False)
    fixtime = Column(DateTime, nullable=False)
    valid = Column(Boolean, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    altitude = Column(Float, nullable=False)
    speed = Column(Float, nullable=False)
    course = Column(Float, nullable=False)
    address = Column(String(512))
    attributes = Column(String(4000))
    accuracy = Column(Float, default=0, nullable=False)
    network = Column(String(4000))


class traccar_devices(Base):
    __tablename__ = "tc_devices"
    __table_args__ = (
        Index("idx_devices_uniqueid", "uniqueid"),
        UniqueConstraint("uniqueid", name="tc_devices_uniqueid_key"),
        {"schema": "traccar"},
    )

    id = Column(
        Integer,
        Sequence("tc_devices_id_seq", schema="traccar"),
        primary_key=True,
    )
    name = Column(String(128), nullable=False)
    uniqueid = Column(String(128), nullable=False)
    lastupdate = Column(DateTime)
    positionid = Column(Integer)
    groupid = Column(Integer)
    attributes = Column(String(4000))
    phone = Column(String(128))
    model = Column(String(128))
    contact = Column(String(512))
    category = Column(String(128))
    disabled = Column(Boolean, default=False)
    status = Column(String(8))
    geofenceIds = Column(String(128))
    expirationtime = Column(DateTime)
    motionstate = Column(Boolean, default=False)
    motiontime = Column(DateTime)
    motiondistance = Column(Float, default=0)
    overspeedstate = Column(Boolean, default=False)
    overspeedtime = Column(DateTime)
    overspeedgeofenceid = Column(Integer, default=0)


def run__bucephalus(verbosity):
    # # Elasticsearch configuration
    # elasticsearch_hostname = os.getenv("BUCEPHALUS__ELASTICSEARCH__HOSTNAME", "localhost")
    # elasticsearch_portnumber = int(os.getenv("BUCEPHALUS__ELASTICSEARCH__PORTNUMBER", 9200))
    # elasticsearch_index = os.getenv("BUCEPHALUS__ELASTICSEARCH__INDEX", "bucephalus")

    # # Elasticsearch client
    # elasticsearch_client = Elasticsearch([{"host": elasticsearch_hostname, "port": elasticsearch_portnumber}])

    # Configure logging
    log_level = logging.ERROR
    if verbosity == 1:
        log_level = logging.WARNING
    elif verbosity == 2:
        log_level = logging.INFO
    elif verbosity >= 3:
        log_level = logging.DEBUG

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # # Custom Elasticsearch log handler
    # class ElasticsearchHandler(logging.Handler):
    #     def emit(self, record):
    #         log_entry = {
    #             "timestamp": record.created,
    #             "level": record.levelname,
    #             "message": self.format(record)
    #         }
    #         elasticsearch_client.index(index=elasticsearch_index, body=log_entry)

    # # Add Elasticsearch log handler to the root logger
    # logging.getLogger().addHandler(ElasticsearchHandler())

    # TimescaleDB configuration
    timescaledb_hostname = os.getenv(
        "BUCEPHALUS__TIMESCALEDB__HOSTNAME", "localhost"
    )
    timescaledb_portnumber = int(
        os.getenv("BUCEPHALUS__TIMESCALEDB__PORTNUMBER", 5432)
    )
    timescaledb_database = os.getenv(
        "BUCEPHALUS__TIMESCALEDB__DATABASE", "DATABASE_NAME"
    )
    timescaledb_username = os.getenv(
        "BUCEPHALUS__TIMESCALEDB__USERNAME", "bucephalus"
    )
    timescaledb_password = os.getenv(
        "BUCEPHALUS__TIMESCALEDB__PASSWORD", "bucephalus"
    )
    timescaledb_schema = os.getenv(
        "BUCEPHALUS__TIMESCALEDB__SCHEMA", "bucephalus"
    )

    # Create the TimescaleDB engine
    db_url = f"postgresql://{timescaledb_username}:{timescaledb_password}@{timescaledb_hostname}:{timescaledb_portnumber}/{timescaledb_database}"
    engine = create_engine(
        db_url, pool_size=256, max_overflow=0, pool_timeout=5
    )
    logging.info(f"Connected to database {db_url}")

    # Create the database tables (if not already created)
    Base.metadata.create_all(engine)

    # Create a session factory
    # Session = sessionmaker(bind=engine)
    Session = scoped_session(sessionmaker(bind=engine))

    # Load the DBC file
    filename = os.getenv(
        "BUCEPHALUS__CAN_DATABASE__PATH", "/tmp/bucephalus.dbc"
    )
    db = cantools.database.load_file(filename)
    logging.info(f"Loaded CAN database {filename}")

    # Redis configuration
    redis_hostname = os.getenv("BUCEPHALUS__REDIS__HOSTNAME", "localhost")
    redis_portnumber = int(os.getenv("BUCEPHALUS__REDIS__PORTNUMBER", 6379))
    redis_password = os.getenv("BUCEPHALUS__REDIS__PASSWORD", "PASSWORD")
    redis_cache_duration = int(
        os.getenv("BUCEPHALUS__REDIS__CACHE_DURATION", 3600)
    )

    # Create a Redis client and connect to the Redis server
    redis_client = redis.Redis(
        host=redis_hostname, port=redis_portnumber, password=redis_password
    )

    # Limit the number of threads to be used
    thread_pool = ThreadPoolExecutor(max_workers=256)

    def get__traccar_device(IMEI, session):
        # device._mapping['uniqueid'] == IMEI
        devices_redis = redis_client.get("traccar_devices")
        if devices_redis is None:
            devices_timescaledb = session.query(
                traccar_devices.id, traccar_devices.uniqueid
            ).all()
            devices = {}
            for device in devices_timescaledb:
                devices[device[1]] = device[0]
            # Store the list of devices in the cache with a specific expiration time (in seconds)
            redis_client.setex(
                "traccar_devices", redis_cache_duration, json.dumps(devices)
            )
            return devices[IMEI]
        else:
            devices = json.loads(devices_redis)
            return devices[IMEI]

    def handle__electrical(payload, IMEI, session, db):
        payload = json.loads(payload)

        # Extract start timestamp and calculate the start datetime object
        start_timestamp = int(payload["iat"])
        # start_datetime = datetime.fromtimestamp(start_timestamp / 1000)
        start_datetime = datetime.fromtimestamp(start_timestamp / 1000000)

        # Extract CAN frames information from the JSON data
        can_id = int(payload["id"], 16)
        frames = payload["frames"]

        logging.debug(
            f"Begun processing CAN frames of {can_id} that arrived in {IMEI} at {start_timestamp}"
        )

        for frame in frames:
            # Extract offset and CAN frame data
            offset = int(frame[0], 16)
            bytebody = bytearray.fromhex("".join(frame[1:]))
            is_extended_frame = True  # Assuming extended frame by default

            # Calculate timestamp for the current message
            # message_timestamp = start_datetime + timedelta(milliseconds=offset)
            message_timestamp = start_datetime + timedelta(microseconds=offset)

            # Decode the CAN frame
            try:
                message_body = db.decode_message(can_id, bytebody)
                message_body["timestamp"] = str(message_timestamp)
                message_body["IMEI"] = IMEI
            except KeyError as e:
                logging.error(
                    f'KeyError "{e}" while decoding {can_id}, {frame[1:]}'
                )
                # Save the CAN frame to TimescaleDB
                try:
                    # can_frame = CANFrame(IMEI=IMEI, timestamp=str(message_timestamp), identifier=payload['id'], data=frame[1:], fields=str(message_body))
                    can_frame = CANFrame(
                        IMEI=IMEI,
                        timestamp=str(message_timestamp),
                        identifier=payload["id"],
                        data=frame[1:],
                    )
                except TypeError as e:
                    logging.error(
                        f'TypeError "{e}" while attempting to save frame {can_id}, {frame[1:]} to database'
                    )
                else:
                    session.add(can_frame)
                    try:
                        # Commit the changes to the database
                        session.commit()
                        # Flush the changes to the database
                        session.flush()
                    except Exception as e:
                        logging.error(
                            f'Error "{e}" while inserting frame {can_id}, {frame[1:]} into the database'
                        )
                        # Rollback the current transaction
                        session.rollback()
            else:
                # Save the message to TimescaleDB
                try:
                    instance = globals()[
                        db.get_message_by_frame_id(can_id).name
                    ]()
                    for key, value in message_body.items():
                        if type(value) == float:
                            value = round(value, 3)
                        elif type(value) == int:
                            pass
                        elif type(value) == str:
                            pass
                        else:
                            value = str(value)
                        setattr(instance, key, value)
                except Exception as e:
                    logging.error(
                        f'Error "{e}" while attempting to save message {can_id}, {message_body} to database'
                    )
                else:
                    session.add(instance)
                    try:
                        # Commit the changes to the database
                        session.commit()
                        # Flush the changes to the database
                        session.flush()
                    except Exception as e:
                        logging.error(
                            f'Error "{e}" while inserting message {can_id}, {message_body} into the database'
                        )
                        # Rollback the current transaction
                        session.rollback()
                # Save the CAN frame to TimescaleDB
                try:
                    can_frame = CANFrame(
                        IMEI=IMEI,
                        timestamp=str(message_timestamp),
                        identifier=payload["id"],
                        data=frame[1:],
                        fields=str(message_body),
                    )
                    # can_frame = CANFrame(IMEI=IMEI, timestamp=str(message_timestamp), identifier=payload['id'], data=frame[1:])
                except TypeError as e:
                    logging.error(
                        f'TypeError "{e}" while attempting to save frame {can_id}, {message_body} to database'
                    )
                else:
                    logging.debug(f"Decoded message is {message_body}")
                    session.add(can_frame)
                    try:
                        # Commit the changes to the database
                        session.commit()
                        # Flush the changes to the database
                        session.flush()
                    except Exception as e:
                        logging.error(
                            f'Error "{e}" while inserting frame into the database'
                        )
                        # Rollback the current transaction
                        session.rollback()

    def handle__geographical(payload, IMEI, session, db):
        payload = json.loads(payload)["location"]

        # Extract timestamp and calculate the datetime object
        timestamp = int(payload["iat"])
        record_datetime = datetime.fromtimestamp(timestamp / 1000)

        logging.debug(
            f"Begun processing packet that was measured by {IMEI} at {record_datetime}"
        )

        try:
            # device = session.query(traccar_devices.id).filter(traccar_devices.uniqueid==IMEI).scalar()
            device = get__traccar_device(IMEI, session)
            position = traccar_positions(
                deviceid=device,
                devicetime=record_datetime,
                fixtime=record_datetime,
                valid=bool(payload["fixed"]),
                latitude=float(payload["latitude"]),
                longitude=float(payload["longitude"]),
                altitude=float(payload["altitude"]),
                speed=float(payload["speed"]),
                course=float(payload["heading"]),
                accuracy=int(payload["satellitecount"]),
            )
        except Exception as e:
            logging.error(f'Error "{e}" while decoding {payload}')
        else:
            session.add(position)
            try:
                # Commit the changes to the database
                session.commit()
                # Flush the changes to the database
                session.flush()
            except Exception as e:
                logging.error(
                    f'Error "{e}" while inserting position into the database'
                )
                # Rollback the current transaction
                session.rollback()

    def handle__other(payload, IMEI, session, db):
        pass

    def handle__alert(payload, IMEI, session, db):
        pass

    def handle__heartbeat(payload, IMEI, session, db):
        pass

    def handle__boot(payload, IMEI, session, db):
        pass

    # MQTT broker details
    broker_hostname = os.getenv("BUCEPHALUS__MQTT_BROKER__HOSTNAME")
    broker_portnumber = int(
        os.getenv("BUCEPHALUS__MQTT_BROKER__PORTNUMBER", 8883)
    )

    # Parse MQTT topics, QoS levels, and retain flags from environment variable JSON
    topics_data = os.getenv("BUCEPHALUS__MQTT__TOPICS")
    if topics_data:
        try:
            topics_config = json.loads(topics_data)
            logging.info("Parsed MQTT topics configuration")
            topics = [
                (entry["topic"], entry["qos"], entry["retain"])
                for entry in topics_config
            ]
        except json.JSONDecodeError:
            logging.error("Error parsing MQTT_TOPICS as JSON")
            topics = []
    else:
        logging.error("No MQTT topics determined from environment")
        topics = []

    # Client ID
    client_id = os.getenv("BUCEPHALUS__MQTT__CLIENT_ID", "bucephalus")

    # MQTT clean session flag
    clean_session = bool(os.getenv("BUCEPHALUS__MQTT__CLEAN_SESSION", False))

    # TLS/SSL Configuration
    tls_enabled = bool(os.getenv("BUCEPHALUS__MQTT__TLS_ENABLED", False))
    ca_certificate = os.getenv("BUCEPHALUS__MQTT__CA_CERTIFICATE")

    def process_message(payload, topic):
        # Process the received message
        # Electrical Data
        if "/e/" in topic:
            IMEI = topic.split("/")[2]
            session = Session()
            handle__electrical(payload, IMEI, session, db)
            session.close()
        # Geographical Data
        elif "/g/" in topic:
            IMEI = topic.split("/")[2]
            session = Session()
            handle__geographical(payload, IMEI, session, db)
            session.close()
        elif "/other/" in topic:
            IMEI = topic.split("/")[2]
            session = Session()
            handle__other(payload, IMEI, session, db)
            session.close()
        elif "/alert/" in topic:
            IMEI = topic.split("/")[2]
            session = Session()
            handle__alert(payload, IMEI, session, db)
            session.close()
        elif "/heartbeat/" in topic:
            IMEI = topic.split("/")[2]
            session = Session()
            handle__heartbeat(payload, IMEI, session, db)
            session.close()
        elif "/boot/" in topic:
            IMEI = topic.split("/")[2]
            session = Session()
            handle__boot(payload, IMEI, session, db)
            session.close()
        else:
            pass

    # Callback function when connection is established
    def on_connect(client, userdata, flags, rc):
        logging.info("Connected to MQTT broker")
        for topic, qos, retain in topics:
            client.subscribe(topic, qos=qos)
            logging.info(f"Subscribed to topic: {topic} with QoS: {qos}")

    # Callback function when a new message is received
    def on_message(client, userdata, msg):
        logging.debug(
            f"Received message: {msg.payload.decode()} on topic {msg.topic}"
        )

        # Process the received message in a separate thread taken from the thread pool
        # thread = threading.Thread(target=process_message, args=(msg.payload.decode(), msg.topic))
        # thread.start()
        thread_pool.submit(process_message, msg.payload.decode(), msg.topic)

    # Create MQTT client instance
    client = mqtt.Client(client_id=client_id, clean_session=clean_session)

    # Set username and password if required
    username = os.getenv("BUCEPHALUS__MQTT_BROKER__USERNAME")
    password = os.getenv("BUCEPHALUS__MQTT_BROKER__PASSWORD")
    if username and password:
        logging.info(f"Applied {username} to authenticate with MQTT broker")
        client.username_pw_set(username, password)

    # Configure TLS/SSL
    if tls_enabled:
        logging.info(
            "Applied TLS certificate to encrypt communication with MQTT broker"
        )
        client.tls_set(
            ca_certs=ca_certificate,
            certfile=None,
            keyfile=None,
            tls_version=ssl.PROTOCOL_TLSv1_2,
        )

    # Assign callback functions
    client.on_connect = on_connect
    client.on_message = on_message

    # Connect to the MQTT broker
    client.connect(broker_hostname, broker_portnumber)

    # Start the MQTT network loop (blocking)
    logging.info("Entering MQTT network loop")
    client.loop_forever()


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Bucephalus")
    parser.add_argument(
        "-v",
        "--verbosity",
        type=int,
        default=2,
        help="Set the verbosity level of logging (0-3)",
    )
    args = parser.parse_args()

    # # Create a context for the daemon
    # daemon_context = daemon.DaemonContext(
    #     working_directory=os.getcwd(),
    #     umask=0o002,
    #     pidfile=lockfile.FileLock("bucephalus.pid"),
    #     detach_process=True,
    #     signal_map={
    #         signal.SIGTERM: lambda signum, frame: sys.exit(0)
    #     }
    # )

    # # Run the script as a daemon
    # with daemon_context:
    #     run__bucephalus(args.verbosity)

    run__bucephalus(args.verbosity)
