INSERT INTO DIM_DELIVERY_ADDRESSES(
    DELIVERY_ADDRESS_SURR_ID,
    SOURCE_SYSTEM,
    SOURCE_TABLE,
    SOURCE_ID,
    DELIVERY_STREET_ADDRESS,
    DELIVERY_POSTAL_CODE,
    DELIVERY_STATE_ID,
    DELIVERY_STATE,
    DELIVERY_COUNTRY,
    insert_dt,
    update_dt)
VALUES(
    -99,
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    -99,
    -99,
    'N/A',
    'N/A',
    SYSDATE,
    SYSDATE);

COMMIT;
