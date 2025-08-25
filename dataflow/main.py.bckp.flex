import argparse
import json
import logging
from datetime import datetime
import os

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import TaggedOutput

# Import the schemas and the new mapping configurations
from schema import SCHEMAS
from mappings import MAPPINGS, Literal


def flatten_dict(d, parent_key='', sep='.'):
    """
    Flattens a nested dictionary.

    Args:
        d (dict): The dictionary to flatten.
        parent_key (str): The base key for the current level.
        sep (str): The separator to use between keys.

    Returns:
        dict: A flattened dictionary.
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


class TransformAndFlattenDoFn(beam.DoFn):
    """
    A generic DoFn to transform and flatten raw JSON records based on a
    declarative mapping configuration.
    """

    def __init__(self, schemas, mappings):
        self._schemas = schemas
        self._mappings = mappings

    def process(self, element):
        """
        Processes a single JSON string element.

        Args:
            element (str): A JSON string representing a single record.

        Yields:
            tuple: A (collection_name, record_dict) tuple for the main output.
            str: A JSON string for the dead-letter output on failure.
        """
        try:
            data = json.loads(element)

            # The upstream ingestor standardizes the payload.
            # We get the collection name to select the right schema and processing logic.
            collection_name = data.get('collection')
            if not collection_name:
                logging.warning("Skipping record without 'collection' key: %s", data)
                return

            target_schema = self._schemas.get(collection_name)
            mapping = self._mappings.get(collection_name)

            if not target_schema or not mapping:
                logging.warning(
                    "No schema or mapping found for collection '%s'. Skipping record.",
                    collection_name
                )
                return

            # The original document is placed inside the 'document' key.
            record = data.get('document')
            if not record:
                logging.warning("Skipping record without 'document' key: %s", data)
                return

            flat_record = flatten_dict(record)
            output_record = {}

            # Iterate through the fields defined in the target schema
            for field in target_schema:
                target_name = field.name
                rule = mapping.get(target_name)

                if not rule:
                    # If no mapping rule is found, fill with None to maintain schema
                    output_record[target_name] = None
                    continue

                source_spec, transform_fn = rule
                raw_value = None

                if isinstance(source_spec, Literal):
                    raw_value = source_spec.value
                elif callable(source_spec):
                    raw_value = source_spec()
                elif isinstance(source_spec, str):
                    raw_value = flat_record.get(source_spec)

                # Apply the transformation function if one is provided
                final_value = transform_fn(raw_value) if transform_fn else raw_value
                output_record[target_name] = final_value

            yield (collection_name, output_record)

        except Exception as e:
            # Any exception during processing will route the original element to the DLQ
            logging.error(
                "Record failed transformation and is being sent to dead-letter queue. Error: %s, Record: %s",
                e, element
            )
            error_payload = {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
                "original_record": element
            }
            yield TaggedOutput('dead_letter', json.dumps(error_payload))


def run():
    """Main entry point; defines and runs the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input GCS path for the JSON files (e.g., gs://bucket/path/*).')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output GCS path to write the Parquet files.')
    parser.add_argument(
        '--dead_letter_output',
        dest='dead_letter_output',
        required=True,
        help='GCS path to write failed records for the dead-letter queue.')

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        raw_records = (p
         # The input reading logic is correct for single-JSON-per-file.
         # No changes are needed here.
         | 'MatchFiles' >> beam.io.fileio.MatchFiles(known_args.input)
         | 'ReadMatches' >> beam.io.fileio.ReadMatches()
         | 'ReadFileContent' >> beam.Map(lambda file: file.read_utf8())
        )

        # The transformation step now produces multiple outputs. We use with_outputs
        # to capture the main output and the 'dead_letter' tagged output.
        transform_results = (raw_records
            | 'TransformAndTag' >> beam.ParDo(
                TransformAndFlattenDoFn(schemas=SCHEMAS, mappings=MAPPINGS)
            ).with_outputs('dead_letter', main='main_output')
        )

        # Separate the PCollections for successful and failed records.
        tagged_records = transform_results.main_output
        dead_letter_records = transform_results.dead_letter

        # --- Main Success Path ---
        # Branch the pipeline for each collection to write to a separate output folder.
        # This fulfills the requirement from roadmap.md to partition output.
        for collection_name, schema in SCHEMAS.items():
            (tagged_records
             | f'Filter_{collection_name}' >> beam.Filter(lambda item: item[0] == collection_name)
             | f'GetRecord_{collection_name}' >> beam.Map(lambda item: item[1])
             # TODO: Add step for complex discount calculation (GroupByKey + ParDo)
             | f'Write_{collection_name}' >> beam.io.WriteToParquet(
                 os.path.join(known_args.output, collection_name),
                 schema=schema,
                 file_name_suffix='.parquet',
                 codec='snappy'
             )
         )

        # --- Dead-Letter Path ---
        # Write the failed records to the specified dead-letter GCS location.
        (dead_letter_records
            | 'WriteDeadLetter' >> beam.io.WriteToText(
                os.path.join(known_args.dead_letter_output, 'failed_records'),
                file_name_suffix='.jsonl'
            ))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()