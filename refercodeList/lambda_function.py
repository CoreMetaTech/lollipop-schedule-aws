import json

def lambda_handler(event, context):
    ref_codes = [
        "PNU01", "PNU02", "PNU03", "PNU04", "PNU05", "PNU06", "FTU01"
    ]
    return {
        'statusCode': 200,
        'body': ref_codes
    }
