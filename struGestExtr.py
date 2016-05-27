from pig_util import outputSchema

@outputSchema('output_field_name:chararray')
def struGestExtr_udf(strn, n):
    return strn[:n]