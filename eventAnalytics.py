import psycopg2
import psycopg2.extras
import json
import os
from datetime import datetime, timezone

# --- Database Connection Details ---
DB_HOST = os.getenv("DB_HOST", "beatbnk-db.cdgq4essi2q1.ap-southeast-2.rds.amazonaws.com")
DB_NAME = os.getenv("DB_NAME", "beatbnk_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "X1SOrzeSrk")
DB_PORT = os.getenv("DB_PORT", "5432")


# --- Helper Functions for Single-Event Report ---

def get_event_id_by_name(event_name: str) -> int | None:
    """Finds an event's unique ID by its name."""
    conn = None
    event_id = None
    print(f"\n🔎 Searching for Event ID for: '{event_name}'...")
    sql = """
        SELECT id FROM events WHERE "eventName" = %(event_name)s ORDER BY "createdAt" DESC LIMIT 1;
    """
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql, {'event_name': event_name})
        result = cur.fetchone()
        if result:
            event_id = result['id']
            print(f"  ✅ Found Event ID: {event_id}")
        else:
            print(f"  ⚠️  Warning: No event found with the name '{event_name}'.")
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"  ❌ An unexpected database error occurred: {error}")
        return None
    finally:
        if conn is not None:
            conn.close()
    return event_id


def get_event_details(cur, event_id: int) -> dict:
    """Fetches the core details of the event, including venue and performer."""
    sql = """
        SELECT
            e."eventName",
            e."startTime",
            e."eventStatus",
            v."venueName",
            u.name AS performer_name
        FROM events e
        LEFT JOIN venues v ON e."venueId" = v.id
        LEFT JOIN performers p ON e."performerId" = p.id
        LEFT JOIN users u ON p."userId" = u.id
        WHERE e.id = %(event_id)s;
    """
    cur.execute(sql, {'event_id': event_id})
    return dict(cur.fetchone() or {})


def get_ticket_sales_summary(cur, event_id: int) -> dict:
    """Calculates ticket sales, revenue, and attendance for the event."""
    # FIXED: Added double quotes around "paymentStatus".
    sql = """
        SELECT
            (SELECT COALESCE(SUM("totalTickets"), 0) FROM event_tickets WHERE "eventId" = %(event_id)s) AS tickets_available,
            (SELECT COUNT(*) FROM tickets WHERE "eventId" = %(event_id)s AND UPPER("paymentStatus"::text) = 'PAID') AS tickets_sold,
            (SELECT COALESCE(SUM(price), 0) FROM tickets WHERE "eventId" = %(event_id)s AND UPPER("paymentStatus"::text) = 'PAID') AS total_revenue,
            (SELECT COUNT(*) FROM attendees WHERE "eventId" = %(event_id)s) AS total_check_ins;
    """
    cur.execute(sql, {'event_id': event_id})
    return dict(cur.fetchone() or {})


def get_ticket_type_breakdown(cur, event_id: int) -> list:
    """Provides a breakdown of sales per ticket type."""
    # FIXED: Added double quotes around "paymentStatus".
    sql = """
        SELECT
            "ticketType",
            COUNT(id) AS sold_count,
            SUM(price) AS revenue
        FROM tickets
        WHERE "eventId" = %(event_id)s AND UPPER("paymentStatus"::text) = 'PAID'
        GROUP BY "ticketType"
        ORDER BY sold_count DESC;
    """
    cur.execute(sql, {'event_id': event_id})
    return [dict(row) for row in cur.fetchall()]


def get_attendee_demographics(cur, event_id: int) -> list:
    """Calculates the gender breakdown of users who purchased tickets."""
    # FIXED: Added double quotes around "paymentStatus".
    sql = """
        SELECT
            u.gender,
            COUNT(DISTINCT u.id) AS unique_ticket_buyers
        FROM tickets t
        JOIN users u ON t."userId" = u.id
        WHERE t."eventId" = %(event_id)s AND UPPER(t."paymentStatus"::text) = 'PAID' AND u.gender IS NOT NULL
        GROUP BY u.gender;
    """
    cur.execute(sql, {'event_id': event_id})
    return [dict(row) for row in cur.fetchall()]


def get_event_engagement_stats(cur, event_id: int) -> dict:
    """Calculates in-event engagement like song requests and tips."""
    sql = """
        SELECT
            (SELECT COUNT(*) FROM song_requests WHERE "eventId" = %(event_id)s) AS total_song_requests,
            (SELECT COALESCE(SUM("tipAmount"), 0) FROM performer_tips WHERE "eventId" = %(event_id)s) AS total_tips_from_event;
    """
    cur.execute(sql, {'event_id': event_id})
    return dict(cur.fetchone() or {})


# --- Main Orchestration Function for a Single Event ---

def generate_single_event_analytics_report(event_id: int) -> dict:
    """Connects to the database and builds a complete analytics report for a single event."""
    conn = None
    report = {}
    print("🚀 Starting single-event analytics report generation...")

    try:
        print(f"  - Attempting to connect to database '{DB_NAME}'...")
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("  ✅ Database connection successful.\n")

        print("--- Fetching Report Components for Event ID:", event_id, "---")

        print("  ℹ️  Fetching event details...")
        event_details = get_event_details(cur, event_id)
        if not event_details:
            return {"error": f"Event with ID {event_id} not found."}
        print("  ...done.")

        print("  🎟️  Fetching ticket sales summary...")
        ticket_summary = get_ticket_sales_summary(cur, event_id)
        print("  ...done.")

        print("  📊 Fetching ticket type breakdown...")
        ticket_breakdown = get_ticket_type_breakdown(cur, event_id)
        print("  ...done.")

        print("  👫 Fetching attendee demographics...")
        attendee_demographics = get_attendee_demographics(cur, event_id)
        print("  ...done.")

        print("  🎶 Fetching in-event engagement...")
        event_engagement = get_event_engagement_stats(cur, event_id)
        print("  ...done.\n")

        print("--- Assembling Final Report ---")
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "report_type": "Single-Event Analytics",
            "event_details": event_details,
            "ticket_summary": ticket_summary,
            "ticket_sales_by_type": ticket_breakdown,
            "attendee_demographics": attendee_demographics,
            "in_event_engagement": event_engagement
        }
        print("  ✅ Final report dictionary assembled successfully.\n")
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ An unexpected error occurred: {error}")
        return {"error": str(error)}
    finally:
        if conn is not None:
            conn.close()
            print("🚪 Database connection closed.")

    return report


# --- Example Usage ---

if __name__ == '__main__':
    target_event_name = "Aloha"

    print("=====================================================")
    print(f"   Starting Full Analytics Run for Event: {target_event_name}")
    print("=====================================================")

    event_id = get_event_id_by_name(target_event_name)

    if event_id:
        analytics_data = generate_single_event_analytics_report(event_id)
        print("\n=====================================================")
        if "error" in analytics_data:
            print("   ❌ Report generation failed.")
        else:
            print("   ✅ Report generation complete. Final JSON output:")
            print("=====================================================")
            json_output = json.dumps(analytics_data, indent=4, default=str)
            print(json_output)
    else:
        print(f"\nCould not proceed with report generation as event '{target_event_name}' was not found.")