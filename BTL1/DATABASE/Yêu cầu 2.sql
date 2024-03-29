--Nh�p v�o m� kh�ch h�ng cho bi?t,  cho bi?t th�ng tin s?n ph?m m�  kh�ch h�ng n�y mua nhi?u nh?t ? t?ng chi nh�nh

CREATE OR REPLACE PROCEDURE PROCEDURE1(CUSID CUSTOMER.CUS_ID%TYPE)
AS
BEGIN
    FOR item IN (
                (SELECT BR1.BRANCH_NAME as BRANCHNAME, P1.PRO_ID AS PROID, P1.PRODUCT_NAME AS PRONAME, SUM(AMOUNT) AS SOLD_AMOUNT
                FROM CN1.PRODUCT P1
                JOIN CN1.BILL_DETAILS BD1 ON P1.PRO_ID = BD1.PRO_ID
                JOIN CN1.BILL B1 ON B1.BILL_ID = BD1.BILL_ID
                JOIN CN1.EMPLOYEE E1 ON B1.EMP_ID  = E1.EMP_ID
                JOIN CN1.BRANCH BR1 ON E1.BRANCH_ID =  BR1.BRANCH_ID
                WHERE B1.CUS_ID = CUSID
                GROUP BY BR1.BRANCH_NAME, P1.PRO_ID, P1.PRODUCT_NAME
                ORDER BY SOLD_AMOUNT DESC
                FETCH FIRST 1 ROW WITH TIES) 
                UNION
                (SELECT BR2.BRANCH_NAME as BRANCHNAME, P2.PRO_ID AS PROID, P2.PRODUCT_NAME AS PRONAME, SUM(AMOUNT) AS SOLD_AMOUNT
                FROM CN2.PRODUCT@cn2_link P2
                JOIN CN2.BILL_DETAILS@cn2_link BD2 ON P2.PRO_ID = BD2.PRO_ID
                JOIN CN2.BILL@cn2_link B2 ON B2.BILL_ID = BD2.BILL_ID
                JOIN CN2.EMPLOYEE@cn2_link E2 ON B2.EMP_ID  = E2.EMP_ID
                JOIN CN2.BRANCH@cn2_link BR2 ON E2.BRANCH_ID =  BR2.BRANCH_ID
                WHERE B2.CUS_ID = CUSID
                GROUP BY BR2.BRANCH_NAME, P2.PRO_ID, P2.PRODUCT_NAME
                ORDER BY SOLD_AMOUNT DESC
                FETCH FIRST 1 ROW WITH TIES)
            )
        LOOP
            DBMS_OUTPUT.PUT_LINE(
            'Ten chi nh�nh = ' || item.BRANCHNAME 
            || ', Ma san pham = ' || item.PROID
            ||', Ten san pham ='||item.PRONAME 
            ||',So luong ='||item.SOLD_AMOUNT);
        END LOOP;
END;
SET SERVEROUTPUT ON
BEGIN
PROCEDURE1(300009);
END;

--FUNCTION
--Nh?p v�o m� kh�ch h�ng, t�nh t?ng s? ti?n m� kh�ch h�ng n�y ?� CHI mua trong c? h? th?ng mini mart
CREATE OR REPLACE FUNCTION FUNCTION1(CUSID CUSTOMER.CUS_ID%TYPE)
RETURN NUMBER
AS
 V_TONGTIEN NUMBER;
BEGIN
    SELECT SUM(TOTAL) INTO V_TONGTIEN
    FROM(
            SELECT SUM(B1.TOTAL_MONEY) AS TOTAL 
            FROM CN1.BILL B1
            WHERE B1.CUS_ID = CUSID
            UNION ALL
            SELECT SUM(B2.TOTAL_MONEY) AS TOTAL 
            FROM CN2.BILL@cn2_link B2
            WHERE B2.CUS_ID=CUSID
        );
    RETURN V_TONGTIEN;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
END;
SET SERVEROUTPUT ON
DECLARE
    CUSID CUSTOMER.CUS_ID%TYPE := 300001;
BEGIN
    DBMS_OUTPUT.PUT_LINE( 'Tong tien chi mua: '||FUNCTION1(CUSID));
END;

--Ng�y mua h�ng (BILL_DATE) c?a m?t kh�ch h�ng th�nh vi�n s? l?n h?n ho?c b?ng ng�y kh�ch h�ng ?� 
--??ng k� th�nh vi�n (REG_DATE)
CREATE OR REPLACE TRIGGER TRIGGER_INSERT_UPDATE_BILL
AFTER INSERT OR UPDATE OF BILL_DATE ON BILL
FOR EACH ROW
DECLARE
    B_BILL_DATE BILL.BILL_DATE%TYPE;
    C_REG_DATE CUSTOMER.REG_DATE%TYPE;
    C_CUSID CUSTOMER.CUS_ID%TYPE;
BEGIN
	SELECT REG_DATE INTO C_REG_DATE
	FROM CUSTOMER
	WHERE CUS_ID = :NEW.CUS_ID;

	IF(:NEW.BILL_DATE < C_REG_DATE) THEN
		BEGIN
			RAISE_APPLICATION_ERROR(-20100,  'ERROR: NGAY MUA HANG CUA KHACH HANG KHONG HOP LE');     
		END;
    END IF;
END;

INSERT INTO CN1.BILL
VALUES(BILL_ID_SEQUENCE.nextval, 200002, 300002, TO_DATE('01/01/2022', 'dd/mm/yyyy'), 53295);
update bill
set BILL_DATE = TO_DATE('01/05/2022', 'dd/mm/yyyy') 
where bill_id = 500024;

CREATE OR REPLACE TRIGGER TRIGGER_UPDATE_CUSTOMER
AFTER INSERT OR UPDATE OF REG_DATE ON CUSTOMER
FOR EACH ROW
DECLARE 
    B_BILL_ID BILL.BILL_ID%TYPE;
    B_BILL_DATE BILL.BILL_DATE%TYPE;
	CURSOR CURSOR_BILL_DATE IS
		SELECT BILL_ID
		FROM BILL
		WHERE CUS_ID = :NEW.CUS_ID;
BEGIN
	OPEN CURSOR_BILL_DATE;
    LOOP
		FETCH CURSOR_BILL_DATE INTO B_BILL_ID;
		EXIT WHEN CURSOR_BILL_DATE%NOTFOUND;
        
		SELECT BILL_DATE INTO B_BILL_DATE
        FROM BILL
        WHERE BILL_ID = B_BILL_ID;
        
        IF(B_BILL_DATE < :NEW.REG_DATE) THEN
                RAISE_APPLICATION_ERROR(-20100,  'ERROR: NGAY DANG KY KHONG HOP LE');
        END IF;
	END LOOP;
    CLOSE CURSOR_BILL_DATE;
    DBMS_OUTPUT.PUT_LINE('THANH CONG');
END;

update customer
set reg_date = TO_DATE('01/01/2020', 'dd/mm/yyyy') 
where cus_id = 300002;















