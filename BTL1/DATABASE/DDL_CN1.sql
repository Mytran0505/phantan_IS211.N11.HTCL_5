--CREATE TABLE

CREATE TABLE CN1.BRANCH(
    BRANCH_ID VARCHAR2(5) CONSTRAINT BRANCH_PK PRIMARY KEY, 
    BRANCH_NAME VARCHAR2(200), 
    ADDRESS VARCHAR2(200)
);

CREATE TABLE CN1.EMPLOYEE(
    EMP_ID NUMBER CONSTRAINT EMPLOYEE_PK PRIMARY KEY,
    FIRST_NAME VARCHAR2(200),
    LAST_NAME VARCHAR2(200),
    GENDER VARCHAR(10),
    BIRTHDAY DATE,
    PHONE VARCHAR2(200),
    ADDRESS VARCHAR2(200),
    START_DATE DATE,
    SALARY NUMBER,
    ROLE VARCHAR2(200),
    BRANCH_ID VARCHAR2(5),
    CONSTRAINT FK_EMP_BRANCH FOREIGN KEY(BRANCH_ID) REFERENCES BRANCH(BRANCH_ID)
);

CREATE TABLE CN1.CUSTOMER
(
    CUS_ID NUMBER CONSTRAINT PK_STUDENT PRIMARY KEY,
    FIRST_NAME VARCHAR(20),
    LAST_NAME VARCHAR(20),
    GENDER VARCHAR(10),
    ADDRESS VARCHAR(200),
    PHONE VARCHAR(20),
    BIRTHDAY DATE,
    REG_DATE DATE,
    SPENT_MONEY NUMBER,
    POINT INT
);

CREATE TABLE CN1.PRODUCT
(
    PRO_ID NUMBER CONSTRAINT PK_PRO PRIMARY KEY,
    PRODUCT_NAME VARCHAR(200),
    COUNTRY VARCHAR(50),
    ORIGINAL_PRICE NUMBER,
    SALE_PRICE NUMBER,
    MFG DATE,
    EXP DATE,
    PRODUCT_TYPE VARCHAR(200),
    VAT NUMBER,
    REMAINING_QUANTITY NUMBER
);

CREATE TABLE CN1.WAREHOUSE_MANAGEMENT 
(
    BRANCH_ID VARCHAR2(5),  
    PRO_ID NUMBER, 
    IMPORTED_DATE DATE, 
    IMPORTED_QUANTITY NUMBER,
    CONSTRAINT PK_WM PRIMARY KEY(BRANCH_ID, PRO_ID),
    CONSTRAINT FK_WM_BRANCH FOREIGN KEY (BRANCH_ID) REFERENCES BRANCH(BRANCH_ID),
    CONSTRAINT FK_WM_PRO FOREIGN KEY (PRO_ID) REFERENCES PRODUCT(PRO_ID)
);

CREATE TABLE CN1.WAREHOUSE_SALES 
(
    BRANCH_ID VARCHAR2(5), 
    PRO_ID NUMBER, 
    STATUS VARCHAR2(200),
    CONSTRAINT WS_PK PRIMARY KEY(BRANCH_ID, PRO_ID),
    CONSTRAINT FK_WS_BRANCH FOREIGN KEY (BRANCH_ID) REFERENCES BRANCH(BRANCH_ID),
    CONSTRAINT FK_WS_PRO FOREIGN KEY (PRO_ID) REFERENCES PRODUCT(PRO_ID)
);

CREATE TABLE CN1.BILL
(
    BILL_ID NUMBER CONSTRAINT PK_BILL PRIMARY KEY,
    EMP_ID NUMBER,
    CUS_ID NUMBER,
    BILL_DATE DATE,
    TOTAL_MONEY NUMBER,
    CONSTRAINT FK_BILL_EMP FOREIGN KEY(EMP_ID) REFERENCES EMPLOYEE(EMP_ID),
    CONSTRAINT FK_BILL_CUS FOREIGN KEY(CUS_ID) REFERENCES CUSTOMER(CUS_ID)
);

CREATE TABLE CN1.BILL_DETAILS
(
    BILL_ID NUMBER,
    PRO_ID NUMBER,
    AMOUNT INT,
    CONSTRAINT PK_B_DETAILS PRIMARY KEY(BILL_ID, PRO_ID),
    CONSTRAINT FK_B_DETAILS_BILL FOREIGN KEY(BILL_ID) REFERENCES BILL(BILL_ID),
    CONSTRAINT FK_B_DETAILS_PRO FOREIGN KEY(PRO_ID) REFERENCES PRODUCT(PRO_ID)
);
--DROP TABLE CN1.BILL_DETAILS;
--DROP TABLE CN1.BILL;
--DROP TABLE CN1.WAREHOUSE_SALES;
--DROP TABLE CN1.WAREHOUSE_MANAGEMENT;
--DROP TABLE CN1.PRODUCT;
--DROP TABLE CN1.CUSTOMER;
--DROP TABLE CN1.EMPLOYEE;
--DROP TABLE CN1.BRANCH;


---CREATE SEQUENCE

CREATE SEQUENCE CN1.EMP_ID_SEQUENCE
      INCREMENT BY 1
      START WITH 200001
      MAXVALUE 299999
      NOCYCLE;

CREATE SEQUENCE CN1.CUS_ID_SEQUENCE
    INCREMENT BY 1
    START WITH 300001
    MAXVALUE 399999
    NOCYCLE;

CREATE SEQUENCE CN1.PRO_ID_SEQUENCE
    INCREMENT BY 1
    START WITH 400001
    MAXVALUE 500999
    NOCYCLE;

CREATE SEQUENCE CN1.BILL_ID_SEQUENCE
    INCREMENT BY 1
    START WITH 500001
    MAXVALUE 599999
    NOCYCLE;
--DROP SEQUENCE CN1.BILL_ID_SEQUENCE;
--DROP SEQUENCE CN1.CUS_ID_SEQUENCE;
--DROP SEQUENCE CN1.PRO_ID_SEQUENCE;
--DROP SEQUENCE CN1.EMP_ID_SEQUENCE;
    















