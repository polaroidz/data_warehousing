# pytype: skip-file

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def run(config, pipeline_args, argv=None, save_main_session=True):
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        print(">> Starting job...")

        ## YOUR CODE GOES HERE

        result = p.run()
        result.wait_until_finish()
