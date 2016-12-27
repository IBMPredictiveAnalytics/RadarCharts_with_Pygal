import pandas as pd
import pygal
import sys
import os

if len(sys.argv) > 1 and sys.argv[1] == "-test":
    df = pd.read_csv("../example/baskets.csv")
    cat_field = "pmethod"
    value_fields = "fruit,veg,freshmeat,dairy,cannedveg,cannedmeat,frozenmeal,beer,wine,softdrink,fish,confecionary".split(",")
    output_option = 'output_to_screen'
    output_path = ''
    output_width = 1024
    output_height = 1024
    viewer_command = "firefox"
    title = "Test"
    fill_areas = True
else:
    from pyspark.context import SparkContext
    from pyspark.sql.context import SQLContext
    import spss.pyspark.runtime
    ascontext = spss.pyspark.runtime.getContext()
    sc = ascontext.getSparkContext()
    sqlCtx = ascontext.getSparkSQLContext()
    df = ascontext.getSparkInputData().toPandas()
    cat_field = '%%category_field%%'
    value_fields = map(lambda x: x.strip(),"%%value_fields%%".split(","))
    output_option = '%%output_option%%'
    output_path = '%%output_path%%'
    output_width = int('%%output_width%%')
    output_height = int('%%output_height%%')
    viewer_command = '%%viewer_command%%'
    title = '%%title%%'
    fill_areas = ('%%fill_areas%%' == 'Y')


df = df[[cat_field]+value_fields].groupby([cat_field], as_index=False).mean()

from pygal import Config

config = Config()
config.width = output_width
config.height = output_height
config.fill = fill_areas
radar_chart = pygal.Radar(config)
radar_chart.title = title
radar_chart.x_labels = value_fields

for i in df.index:
    cat = df.ix[i][cat_field]
    vals = [df.ix[i][value_field] for value_field in value_fields]
    radar_chart.add(cat, vals)

if output_option == 'output_to_file':
    if not output_path:
        raise Exception("No output path specified")
else:
    output_path = os.tempnam()+".svg"

radar_chart.render_to_file(output_path)

if output_option == 'output_to_screen':
    os.system(viewer_command+" "+output_path)
    print("Output should open in a browser window")
else:
    print("Output should be saved on the server to path: "+output_path)

