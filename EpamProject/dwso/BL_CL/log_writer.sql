CREATE OR REPLACE PROCEDURE log_writer ( p_msgtype    	IN VARCHAR2,
                                        p_objname    	IN VARCHAR2,
                                        p_insert_dt 	IN DATE,
                                        p_msgcode    	IN VARCHAR2 DEFAULT NULL,
                                        p_msgtext    	IN VARCHAR2 DEFAULT NULL)
                         		
IS
	PRAGMA autonomous_transaction;
BEGIN
	INSERT INTO mta_log_table(msg_type,	
                            obj_name, 
                            insert_dt,
                            msg_code, 
                            msg_text,
                            session_user,
                            session_id)
				
	VALUES(p_msgtype,
            p_objname,
            p_insert_dt,
            p_msgcode,
            p_msgtext,
            (Select sys_context('USERENV', 'SESSION_USER') from dual),
            (Select sys_context('USERENV', 'SID') from dual));
	COMMIT;            

END;

