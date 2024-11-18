import boto3
import uuid
import os

def lambda_handler(event, context):
    # Entrada (json)
    print(event) # Log json en CloudWatch
    try:
        tenant_id = event['body']['tenant_id']
    except Exception as exception:
        error = {
            "tipo": "ERROR",
            "log_datos": {
                "message": "Hubo un error al leer el tenant_id",
                "error": str(exception)
            }
        }
        print(error)
        return {
            'statusCode': 500,
            "message": "Hubo un error al leer el tenant_id",
            "error": str(exception)
        }
    else:
        info = {
            "tipo": "INFO",
            "log_datos": {
                "message": "El tenant_id se leyo correctamente",
                "info": tenant_id
            }
        }
        print(info)

    try:
        pelicula_datos = event['body']['pelicula_datos']
    except Exception as exception:
        error = {
            "tipo": "ERROR",
            "log_datos": {
                "message": "Error al leer pelicula_datos",
                "error": str(exception)
            }
        }
        print(error)
        return {
            'statusCode': 500,
            "message": "Error al leer pelicula_datos",
            "error": str(exception)
        }
    else:
        info = {
            "tipo": "INFO",
            "log_datos": {
                "message": "Se leyó pelicula_datos correctamente",
                "info": pelicula_datos
            }
        }
        print(info)

    nombre_tabla = os.environ["TABLE_NAME"]
    # Proceso
    uuidv4 = str(uuid.uuid4())
    pelicula = {
        'tenant_id': tenant_id,
        'uuid': uuidv4,
        'pelicula_datos': pelicula_datos
    }

    info = {
            "tipo": "INFO",
            "log_datos": {
                "message": "Se inserto la pelicula",
                "info": pelicula
            }
        }
    print(info)

    dynamodb = boto3.resource('dynamodb')

    try:
        table = dynamodb.Table(nombre_tabla)
    except Exception as exception:
        error = {
            "tipo": "ERROR",
            "log_datos": {
                "message": "Hubo un error al conectarse a la tabla",
                "error": str(exception)
            }
        }
        print(error)
        return {
            'statusCode': 500,
            'message': "Hubo un error al conectarse a la tabla",
            'error': str(exception)
        }
    else:
        info = {
            "tipo": "INFO",
            "log_datos": {
                "message": "Conexion a la tabla correctamente",
            }
        }
        print(info)
    try:
        response = table.put_item(Item=pelicula)
    except Exception as exception:
        error = {
            "tipo": "ERROR",
            "log_datos": {
                "message": "Error al insertar en la tabla",
                "error": str(exception)
            }
        }
        print(error)
        return {
            'statusCode': 500,
            'message': "Hubo un error al insertar en la tabla",
            'error': str(exception)
        }

    # Salida (json)

    info = {
        "tipo": "INFO",
        "log_datos": {
            "message": "Se realizó la insercion correctamente",
            "info": response
        }
    }

    print(info) # Log json en CloudWatch

    return {
        'statusCode': 200,
        'pelicula': pelicula,
        'response': response
    }
