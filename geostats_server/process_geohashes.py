import csv
import logging
from services.stats_service import calculate_stats_geohash
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Function to read CSV file in batches
def read_csv_in_batches(file_path, batch_size):
    """
    Reads a CSV file in batches and yields lists of geohashes.
    """
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        if 'geohash' not in reader.fieldnames:
            raise ValueError("The input CSV file must contain a 'geohash' column.")
        
        batch = []
        for row in reader:
            batch.append(row['geohash'])
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:  # Yield the last batch if it's not empty
            yield batch

# Process the CSV file and calculate stats
def process_geohashes(input_file, output_file, batch_size=100):
    """
    Reads geohashes from the input CSV in batches, calculates stats for each, and writes results to the output CSV.
    """
    headers_written = False

    try:
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = None  # CSV writer to be initialized after headers are determined

            for batch in read_csv_in_batches(input_file, batch_size):
                batch_results = []
                stat_names = set()
                start = datetime.now()
                for geohash in batch:
                    try:
                        logging.debug(f"Processing geohash: {geohash}")
                        stats = calculate_stats_geohash(geohash)
                        flat_stats = {'geohash': geohash}
                        flat_stats.update(stats['stats'])
                        batch_results.append(flat_stats)
                        stat_names.update(stats['stats'].keys())
                    except Exception as e:
                        logging.error(f"Error processing geohash {geohash}: {e}", exc_info=True)

                if batch_results:
                    # Write headers once using the first result's keys
                    if not headers_written:
                        first_result = batch_results[0]
                        fieldnames = list(first_result.keys())  # Preserve key order
                        writer = csv.DictWriter(file, fieldnames=fieldnames)
                        writer.writeheader()
                        headers_written = True

                    # Write batch results
                    writer.writerows(batch_results)
                time_taken = datetime.now() - start
                logging.info(f"Batch of {len(batch_results)} geohashes processed in {time_taken}ms.")

    except Exception as e:
        logging.critical(f"Critical error during processing: {e}", exc_info=True)
        raise

# Main execution
if __name__ == "__main__":
    input_file = "./data/input.csv"  # Input file path
    output_file = "./data/output.csv"  # Output file path
    batch_size = 100  # Adjust based on memory capacity

    try:
        logging.info("Starting geohash processing...")
        process_geohashes(input_file, output_file, batch_size)
        logging.info("Geohash processing completed successfully.")
    except Exception as e:
        logging.critical("An unrecoverable error occurred.", exc_info=True)
