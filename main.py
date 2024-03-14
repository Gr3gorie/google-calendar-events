from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import BaseModel, EmailStr, field_validator
from datetime import datetime
import psycopg2
import os

SERVICE_ACCOUNT_FILE = 'calendar-api-service-account.json'
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('calendar', 'v3', credentials=credentials)


# Define a time range


class Event(BaseModel):
    id: str
    title: str
    starttime: datetime
    endtime: datetime
    updated: datetime
    status: str
    organizer: str


class Attendee(BaseModel):
    event_id: str
    email: EmailStr

    @field_validator('email')
    def strip_email(cls, email):
        return email.strip()


def parse_datetime(dt_str):
    return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S%z')


def fetch_events(calendar_id='primary', time_min=None, time_max=None, max_results=2500):
    events_result = service.events().list(
        calendarId=calendar_id,
        timeMin=time_min,
        timeMax=time_max,
        maxResults=max_results,
        singleEvents=True,
        orderBy='startTime'
    ).execute()
    return events_result.get('items', [])


def process_events(raw_events):
    processed_events = []
    for event in raw_events:
        # Extract and transform event data here
        # Validate event data against your Pydantic models (Event and Attendee)
        event_data = {
            "id": event["id"],
            "title": event.get("summary", "No Title"),
            "starttime": event["start"].get("dateTime") or event["start"].get("date"),
            "endtime": event["end"].get("dateTime") or event["end"].get("date"),
            "updated": event["updated"],
            "status": event.get("status", "No Status"),
            "organizer": event["organizer"].get("email", "No Organizer"),
        }
        event_obj = Event(**event_data)

        attendees = []
        for attendee_data in event.get("attendees", []):
            attendee_obj = Attendee(event_id=event_obj.id, email=attendee_data.get("email", "No Email"))
            attendees.append(attendee_obj)

        processed_events.append((event_obj, attendees))
    return processed_events


def make_migrations(conn):
    cur = conn.cursor()

    cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id VARCHAR PRIMARY KEY,
                title VARCHAR,
                start_time TIMESTAMP WITH TIME ZONE,
                end_time TIMESTAMP WITH TIME ZONE,
                updated TIMESTAMP WITH TIME ZONE,
                status VARCHAR,
                organizer VARCHAR
            );
        """)

    cur.execute("""
            CREATE TABLE IF NOT EXISTS attendees (
                attendee_id SERIAL PRIMARY KEY,
                event_id VARCHAR,
                email VARCHAR,
                FOREIGN KEY (event_id) REFERENCES events(event_id)
            );
        """)

    conn.commit()
    cur.close()


def insert_into_postgres(conn, processed_events):
    cur = conn.cursor()
    for event_obj, attendees in processed_events:
        cur.execute("""
            INSERT INTO events (event_id, title, start_time, end_time, updated, status, organizer)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
            title = EXCLUDED.title,
            start_time = EXCLUDED.start_time,
            end_time = EXCLUDED.end_time,
            updated = EXCLUDED.updated,
            status = EXCLUDED.status,
            organizer = EXCLUDED.organizer;
            """,
                    (event_obj.id, event_obj.title, event_obj.starttime, event_obj.endtime, event_obj.updated,
                     event_obj.status, event_obj.organizer)
                    )

        for attendee_obj in attendees:
            cur.execute("""
                        INSERT INTO attendees (event_id, email)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                        """,
                        (attendee_obj.event_id, attendee_obj.email)
                        )

    # Commit transactions and close cursor
    conn.commit()
    cur.close()


def main():
    print("Connecting to Postgres...")
    conn = psycopg2.connect(user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
                            database=os.environ["POSTGRES_DATABASE"], host=os.environ["POSTGRES_HOST"],
                            port=os.environ["POSTGRES_PORT"])

    print("Running migrations...")
    make_migrations(conn)

    print("Fetching data from Google Calendar...")
    time_min = datetime(2022, 3, 1).isoformat() + 'Z'
    time_max = datetime(2025, 1, 1).isoformat() + 'Z'
    raw_events = fetch_events('galina.skripka@dataacquisition.ru', time_min, time_max)
    processed_events = process_events(raw_events)

    print("Inserting Google Calendar data to Postgres...")
    insert_into_postgres(conn, processed_events)

    print("Done")


if __name__ == '__main__':
    main()