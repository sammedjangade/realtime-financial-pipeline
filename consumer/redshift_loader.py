def load_to_redshift(records, conn):
    cursor = conn.cursor()
    inserted = 0
    duplicates = 0

    for record in records:
        data = json.loads(record['Data'])

        # Check for duplicate trade_id
        cursor.execute("""
            SELECT COUNT(*) FROM trades.raw_events 
            WHERE trade_id = %s AND product_id = %s
        """, (data['trade_id'], data['product_id']))

        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO trades.raw_events (
                    trade_id, product_id, price, size, side,
                    trade_timestamp, trade_value_usd, size_category,
                    ingested_at, source_system
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['trade_id'],
                data['product_id'],
                data['price'],
                data['size'],
                data['side'],
                data['trade_timestamp'],
                data['trade_value_usd'],
                data['size_category'],
                data['ingested_at'],
                'COINBASE_WEBSOCKET'
            ))
            inserted += 1
        else:
            duplicates += 1

    conn.commit()
    cursor.close()
    print(f"✅ Inserted: {inserted} | Duplicates skipped: {duplicates}")