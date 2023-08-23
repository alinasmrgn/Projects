DROP TABLE wrk_sales CASCADE CONSTRAINT;
CREATE TABLE wrk_sales(
    DATE_ID                DATE,
    TRANSACTION_ID               NUMBER,
    CUSTOMER_SURR_ID                NUMBER,
    DELIVERY_ADDRESS_SURR_ID             NUMBER,
    EMPLOYEE_SURR_ID              NUMBER,
    PRODUCT_SURR_ID  NUMBER,
    CHANNEL_SURR_ID       NUMBER,
    PAYMENT_TYPE_SURR_ID     VARCHAR(60),
    UNIT_PRICE                NUMBER,
    UNIT_COST            NUMBER,
    QUANTITY             NUMBER,
    srcsystem          VARCHAR(60),
    srctable           VARCHAR(60),
    UPDATE_DT               DATE);
    
CREATE OR REPLACE PUBLIC SYNONYM wrk_sales FOR bl_cl.wrk_sales;