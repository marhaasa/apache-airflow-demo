import os
import json
import uuid
import time
import random
import psycopg2
import argparse
from faker import Faker
from faker_vehicle import VehicleProvider
import logging as logger
from datetime import datetime, timezone, timedelta

# Faker and Logger
faker = Faker(["no_NO"])
faker.add_provider(VehicleProvider)

logger.basicConfig(
    format="%(asctime)s (%(levelname)s) %(message)s",
    datefmt="[%Y-%m-%d %H:%M:%S]",
    level=logger.INFO,
)

# Parser config
parser = argparse.ArgumentParser(
    epilog="example: python fake-data-generator.py --qtd 100 --debug true --interval 0",
    description="Automatic fictitious data generator for testing, sending to a specific PostgreSQL topic.",
)
parser.add_argument(
    "--qtd",
    type=int,
    help="How many fictitious records do you want to generate?",
    default=100,
)
parser.add_argument(
    "--interval",
    type=float,
    nargs="?",
    help="Is there an interval between record insertions? [FLOAT]",
)
parser.add_argument(
    "--debug", type=bool, nargs="?", help="Show records inserted on screen?"
)
args = parser.parse_args()

# DbConfig

POSTGRES_DATABASE_USER = os.environ.get("POSTGRES_DATABASE_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_DATABASE = os.environ.get("POSTGRES_DATABASE")
POSTGRES_TABLE = os.environ.get("POSTGRES_TABLE")

conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DATABASE,
    user=POSTGRES_DATABASE_USER,
    password=POSTGRES_PASSWORD,
)

# Fake Data
logger.info("Starting insertion of fake data...")
logger.debug(
    "Arguments: "
    + f"\n       TABLE: {POSTGRES_TABLE}"
    + "\n        Qtd: "
    + str(args.qtd)
    + "\n        Interval: "
    + str(args.interval)
)

counter = 0
while counter < args.qtd:
    data = {}
    # data["id"] = random.getrandbits(32)
    data["uuid"] = str(uuid.uuid4())
    data["radar_id"] = random.randint(1, 50000)
    data["license_plate"] = faker.license_plate()
    data["vehicle_make"] = faker.vehicle_make().upper()
    data["vehicle_model"] = faker.vehicle_model().upper()
    data["vehicle_color"] = faker.safe_color_name().upper()
    data["velocity"] = random.randint(20, 160)
    data["velocity_limit"] = random.choice(
        random.choices(
            [40, 50, 60, 70, 80, 90, 100, 120],
            weights=(40, 10, 10, 10, 10, 15, 2.5, 2.5),
            k=1,
        )
    )
    data["country_code"] = "NO"
    data["state_name"] = faker.city_suffix().upper()
    # data["created_at"] = str(faker.date_time_this_year())
    # data["updated_at"] = str(faker.date_time_this_year())

    counter += 1

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO source.biler (
                uuid,
                radar_id,
                license_plate,
                vehicle_make,
                vehicle_model,
                vehicle_color,
                velocity,
                velocity_limit,
                country_code,
                state_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                data["uuid"],
                data["radar_id"],
                data["license_plate"],
                data["vehicle_make"],
                data["vehicle_model"],
                data["vehicle_color"],
                data["velocity"],
                data["velocity_limit"],
                data["country_code"],
                data["state_name"],
            ),
        )
        conn.commit()

    except Exception as error:
        logger.error(f"{error}")
        conn.close()
        exit()
    time.sleep(args.interval if args.interval is not None else 0)
logger.info(f"Record {counter} inserted successfully into table {POSTGRES_TABLE}.")
