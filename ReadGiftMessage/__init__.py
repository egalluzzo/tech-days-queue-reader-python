import json
import logging
import os
import pyodbc
import uuid

import azure.functions as func

def main(msg: func.ServiceBusMessage):
    message_text = msg.get_body().decode('utf-8')
    logging.info('Read gift message: %s', message_text)
    gift_msg = json.loads(message_text)

    conn = pyodbc.connect(os.getenv("GIFTS_DB_CONNECTION_STRING"))
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO gifts (id, length, width, height, production_line, recipient, creation_date)
        VALUES (?, ?, ?, ?, 'eric-galluzzo', ?, CURRENT_TIMESTAMP)
        """,
        str(uuid.uuid4()),
        gift_msg['giftBoundingBox']['length'],
        gift_msg['giftBoundingBox']['width'],
        gift_msg['giftBoundingBox']['height'],
        gift_msg['recipient'])
    cursor.commit()

    logging.info('Wrote gift message to database')
