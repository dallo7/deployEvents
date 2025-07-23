import requests
import json

BASE_URL = "http://127.0.0.1:5059"


def get_event_report(event_name):
    """
    Calls the Flask API to get the event report for a given event name.
    """
    endpoint = f"/event_report/{event_name}"
    url = f"{BASE_URL}{endpoint}"

    print(f"Attempting to fetch report for event: '{event_name}' from {url}")

    try:
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        response.raise_for_status()

        # Parse the JSON response
        data = response.json()

        if data.get("status") == "success":
            print(f"\n✅ Successfully retrieved report for '{event_name}':")
            print(json.dumps(data.get("data"), indent=4))
        else:
            print(f"\n❌ Failed to retrieve report for '{event_name}':")
            print(json.dumps(data, indent=4))

    except requests.exceptions.ConnectionError as e:
        print(f"\nConnection Error: Could not connect to the Flask API.")
        print(f"Please ensure your Flask app is running at {BASE_URL}.")
        print(f"Error details: {e}")
    except requests.exceptions.HTTPError as e:
        print(f"\nHTTP Error: Received a bad response from the server.")
        print(f"Status Code: {e.response.status_code}")
        print(f"Response: {e.response.text}")
    except json.JSONDecodeError:
        print(f"\nError: Could not decode JSON response from the server.")
        print(f"Raw Response: {response.text}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")


if __name__ == "__main__":
    # --- Example Usage ---
    get_event_report("Aloha")

    print("\n" + "=" * 50 + "\n")

    # Test with an event that does not exist
    get_event_report("NonExistentEvent")

    print("\n" + "=" * 50 + "\n")

    # Test with another existing event
    get_event_report("TechFest")
