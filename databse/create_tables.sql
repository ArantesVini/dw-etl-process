-- database name is: logistic_data_mart
-- schema: dbs
-- I use postgres with pgAdmin as DBMS, all running in Docker containers

CREATE TABLE dbs."D_CUSTOMER"
(
    customer_id int NOT NULL,
    customer_name text,
    customer_surname text,
    PRIMARY KEY (customer_id)
);


CREATE TABLE dbs."D_SHIPP_COMPANY"
(
    shippcom_id integer NOT NULL,
    shippcom_name text,
    PRIMARY KEY (shippcom_id)
);


CREATE TABLE dbs."D_WAREHOUSE"
(
    warehouse_id bigint NOT NULL,
    warehouse_name text,
    PRIMARY KEY (warehouse_id)
);


CREATE TABLE dbs."D_DELIVERY"
(
    delivery_id bigint NOT NULL,
    delivery_adress text,
    delivery_country text,
    delivery_type text,
    PRIMARY KEY (delivery_id)
);


CREATE TABLE dbs."D_PAYMENT"
(
    payment_id bigint NOT NULL,
    payment_type text,
    PRIMARY KEY (payment_id)
);


CREATE TABLE dbs."D_DATE"
(
    date_id bigint NOT NULL,
    date_full text,
    date_day integer,
    date_month integer,
    date_year integer,
    PRIMARY KEY (date_id)
);


CREATE TABLE dbs."F_LOGISTIC"
(
    customer_id integer,
    shippcom_id integer,
    warehouse_id integer,
    delivery_id integer,
    payment_id integer,
    date_id integer,
    shipping_value double precision,
    PRIMARY KEY (customer_id, shippcom_id, warehouse_id, delivery_id, payment_id, date_id)
);


ALTER TABLE IF EXISTS dbs."F_LOGISTIC"
    ADD CONSTRAINT "FK_CUSTOMER" FOREIGN KEY (customer_id)
    REFERENCES dbs."D_CUSTOMER" (customer_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS dbs."F_LOGISTIC"
    ADD CONSTRAINT "FK_SHIPPCOM" FOREIGN KEY (shippcom_id)
    REFERENCES dbs."D_SHIPP_COMPANY" (shippcom_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS dbs."F_LOGISTIC"
    ADD CONSTRAINT "FK_WAREHOUSE" FOREIGN KEY (warehouse_id)
    REFERENCES dbs."D_WAREHOUSE" (warehouse_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS dbs."F_LOGISTIC"
    ADD CONSTRAINT "FK_DELIVERY" FOREIGN KEY (delivery_id)
    REFERENCES dbs."D_DELIVERY" (delivery_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS dbs."F_LOGISTIC"
    ADD CONSTRAINT "FK_PAYMENT" FOREIGN KEY (payment_id)
    REFERENCES dbs."D_PAYMENT" (payment_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS dbs."F_LOGISTIC"
    ADD CONSTRAINT "FK_DATE" FOREIGN KEY (date_id)
    REFERENCES dbs."D_DATE" (date_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;