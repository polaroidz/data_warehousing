# pytype: skip-file

from datetime import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

from apache_beam.io.parquetio import WriteToParquet

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from pysql_beam.sql_io.sql import ReadFromSQL
from pysql_beam.sql_io.wrapper import MySQLWrapper

_timestamp = datetime.today().strftime('%Y-%m-%d')

def add_timestamp(element):
    element["_timestamp"] = _timestamp
    return element


def run(config, pipeline_args, argv=None, save_main_session=True):
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        print(">> Starting job...")
        # 1. Carregar dados da fonte de dados externa 
        df = p | ReadFromSQL(host="***", 
                             port="3306",
                             username="root", 
                             password="****",
                             database="empresa_xpto",
                             query="select * from Countries",
                             wrapper=MySQLWrapper,
                             batch=100000)

        df = df | 'AddTimestamp' >> beam.Map(add_timestamp)

        # 2. Adicionar timestamp
        # 3. Salvar dataframe em uma tabela Delta (se possivel)
        df | WriteToText("./output.txt")
        #df | WriteToParquet("gs://diego-misc2/hello/output.txt", schema=)
        #df | WriteToText("gs://diego-misc2/hello/output.txt")

        result = p.run()
        result.wait_until_finish()

