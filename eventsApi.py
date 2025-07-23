from flask import Flask, jsonify
import eventAnalytics


app = Flask(__name__)


@app.route('/event_report/<string:event_name>', methods=['GET'])
def get_event_report(event_name):
    """
    API endpoint to get an event report by event name.
    Expects the event name as a path parameter.
    """
    print(f"\n=====================================================")
    print(f"   API Call Received for Event: {event_name}")
    print(f"=====================================================")

    # Use the first function to get the event ID
    event_id = eventAnalytics.get_event_id_by_name(event_name)

    if event_id:
        # Use the second function to generate the analytics report
        analytics_data = eventAnalytics.generate_single_event_analytics_report(event_id)

        if "error" in analytics_data:
            print(f"   ❌ Report generation failed for event '{event_name}'.")
            return jsonify({"status": "error", "message": analytics_data["error"]}), 500
        else:
            print(f"   ✅ Report generation complete for event '{event_name}'.")
            print("=====================================================")
            return jsonify({"status": "success", "data": analytics_data}), 200
    else:
        print(f"\nCould not proceed with report generation as event '{event_name}' was not found.")
        return jsonify({"status": "error", "message": f"Event '{event_name}' not found."}), 404


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5080, debug=False, use_reloader=False)
