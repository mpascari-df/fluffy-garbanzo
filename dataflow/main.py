import argparse
import json
import logging
from datetime import datetime
import os

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
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
            tuple: A (collection_name, record_dict) tuple for the main output,
                   or a string for the dead-letter queue.
        """
        try:
            # Parse the JSON record
            record = json.loads(element)
            collection_name = record.get('collection_name')
            
            # Use the correct mapping for the collection
            if collection_name not in self._mappings:
                raise ValueError(f"No mapping found for collection: {collection_name}")
            
            mapping = self._mappings[collection_name]
            
            # Flatten the nested dictionary
            flattened_record = flatten_dict(record)
            
            # Apply transformations and create the final structured record
            structured_record = {}
            for target_field, (source_path, transform_func) in mapping.items():
                if isinstance(source_path, Literal):
                    value = source_path.value
                elif callable(source_path):
                    value = source_path()
                else:
                    value = flattened_record.get(source_path)

                if transform_func:
                    value = transform_func(value)
                
                structured_record[target_field] = value

            # Yield the processed record with a tag for the main output
            yield (collection_name, structured_record)

        except Exception as e:
            logging.error('Failed to process element: %s, error: %s', element, e)
            # Yield the failed element to the dead-letter queue
            yield TaggedOutput('dead_letter', element)


def run():
    """Main entry point for the pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    parser.add_argument(
        '--dead-letter-output',
        dest='dead_letter_output',
        required=True,
        help='Output path for records that fail transformation.')
    
    known_args, pipeline_args = parser.parse_known_args()

    # Pass the pipeline args to the PipelineOptions object
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        
        # Read the raw JSON data.
        raw_data = (p
            | 'ReadFromGCS' >> beam.io.ReadFromText(known_args.input))

        # Apply the transformation logic to the raw data.
        # This will produce two PCollections: one for successful records and one for failed records.
        transform_results = (raw_data
            | 'TransformAndFlatten' >> beam.ParDo(
                TransformAndFlattenDoFn(SCHEMAS, MAPPINGS)
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
