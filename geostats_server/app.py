from flask import Flask, request, jsonify
from services.stats_service import calculate_stats, clear_cached_results, create_stats_table, load_stats_queries
import logging

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Setup database and load stats queries
create_stats_table()
load_stats_queries()

@app.route('/stats', methods=['GET'])
def stats():
    try:
        lat = request.args.get('lat', type=float)
        lng = request.args.get('lng', type=float)

        if lat is None or lng is None:
            return jsonify({'error': 'Latitude and longitude are required'}), 400

        result = calculate_stats(lat, lng)
        return jsonify(result)

    except Exception as e:
        logging.exception(f"Error occurred: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/stats/cache', methods=['DELETE'])
def truncate_cache():
    """
    Endpoint to truncate the cached stats table.
    """
    try:
        clear_cached_results()
        return jsonify({'message': 'Cache successfully truncated.'}), 200
    except Exception as e:
        logging.exception(f"Error truncating cache: {e}")
        return jsonify({'error': 'Failed to truncate cache.'}), 500



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
