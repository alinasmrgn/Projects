DROP TABLE mta_log_table;
Create TABLE mta_log_table(
	log_id 		    NUMBER(20) GENERATED ALWAYS AS IDENTITY,	
	msg_type	    VARCHAR2(20) NOT NULL, 
	obj_name 	    VARCHAR2(200) DEFAULT NULL,
	insert_dt	    DATE NOT NULL,
	msg_code 	    VARCHAR2(200) DEFAULT NULL,
	msg_text 	    VARCHAR2(200) DEFAULT NULL,
    session_user    VARCHAR2(100) NOT NULL,
    session_id      NUMBER(20) NOT NULL,
	CONSTRAINT mta_log_table_log_pk PRIMARY KEY(log_id)
);
CREATE OR REPLACE PUBLIC SYNONYM mta_log_table FOR bl_cl.mta_log_table;


                       