# In[1]:
#import các thư viện sử dụng để phân tán
from py2neo import Graph
import pandas as pd
import numpy as np

#Kết nối tới database của chi nhánh 1 sử dụng VPN ảo của Radmin
cn1 = Graph(uri="bolt://26.116.246.130:7687", auth=("neo4j","123456"))

#Kết nối tới database của chi nhánh 2 sử dụng VPN ảo của Radmin
cn2 = Graph(uri="bolt://26.198.144.133:7687", auth=("neo4j","123456"))

"""ở đây 'neo4j' là tên user, '123456' là mật khẩu"""

#Các câu truy vấn hoặc dml đều sử dụng trên các biến cn1 và cn2 ở trên

# Lấy dữ liệu qua lại giữa hai máy bằng NoSQL
# In[2]:
# 1. In ra mã hóa đơn, trị giá các hóa đơn và họ tên nhân viên đã thanh toán những hóa đơn này
#do khách hàng có tên là "Nguyen Huu Tho"  mua
from py2neo import Graph
import pandas as pd
import numpy as np

cn1 = Graph(uri="bolt://26.116.246.130:7687", auth=("neo4j","123456"))
cn2 = Graph(uri="bolt://26.198.144.133:7687", auth=("neo4j","123456"))
data1 = cn1.run("MATCH   (B) -[:FK_BILL_CUS]->(C), (B)-[R:FK_BILL_EMP]->(E) \
                WHERE  C.FIRST_NAME +' ' +C.LAST_NAME = 'Nguyen Huu Tho' \
                RETURN B.BILL_ID, B.TOTAL_MONEY, E.FIRST_NAME +' ' +E.LAST_NAME AS EMP_FULLNAME").data()
data2 = cn2.run("MATCH   (B) -[:FK_BILL_CUS]->(C), (B)-[R:FK_BILL_EMP]->(E) \
                WHERE  C.FIRST_NAME +' ' +C.LAST_NAME = 'Nguyen Huu Tho' \
                RETURN B.BILL_ID, B.TOTAL_MONEY, E.FIRST_NAME +' ' +E.LAST_NAME AS EMP_FULLNAME").data()

d1 = pd.DataFrame(data1)
d2 = pd.DataFrame(data2)
df = [d1,d2]
result = pd.concat(df, ignore_index=True)
print(result)


# In[3]:
#2. In ra danh sách các sản phẩm (PRO_ID, PRODUCT_NAME) không bán được của nước "USA"
from py2neo import Graph
import pandas as pd
import numpy as np

cn1 = Graph(uri="bolt://26.116.246.130:7687", auth=("neo4j","123456"))
cn2 = Graph(uri="bolt://26.198.144.133:7687", auth=("neo4j","123456"))
data1 = cn1.run("MATCH (P:PRODUCT) \
                WHERE NOT (()-[:FK_B_DETAILS_PRO]->(P)) AND P.COUNTRY = 'USA' \
                RETURN P.PRO_ID, P.PRODUCT_NAME").data()
data2 = cn2.run("MATCH (P:PRODUCT) \
                WHERE NOT (()-[:FK_B_DETAILS_PRO]->(P)) AND P.COUNTRY = 'USA' \
                RETURN P.PRO_ID, P.PRODUCT_NAME").data()

d1 = pd.DataFrame(data1)
d2 = pd.DataFrame(data2)
df = [d1,d2]
result = pd.concat(df, ignore_index=True)
print(result.drop_duplicates())


# In[4]:
#3. TÌM SẢN PHẨM ĐƯỢC MUA NHIỀU NHẤT
from py2neo import Graph
import pandas as pd
import numpy as np

cn1 = Graph(uri="bolt://26.116.246.130:7687", auth=("neo4j","123456"))
cn2 = Graph(uri="bolt://26.198.144.133:7687", auth=("neo4j","123456"))
data1 = cn1.run("MATCH (BD)-[:FK_B_DETAILS_PRO]->(P) \
                WITH  P, SUM(BD.AMOUNT) AS SLMUA \
                WITH MAX(SLMUA) AS MAX \
                MATCH (BD)-[:FK_B_DETAILS_PRO]->(P) \
                WITH  P, MAX, SUM(BD.AMOUNT) AS SL \
                WHERE SL=MAX \
                RETURN P.PRO_ID, P.PRODUCT_NAME, SL").data()
data2 = cn2.run("MATCH (BD)-[:FK_B_DETAILS_PRO]->(P) \
                WITH  P, SUM(BD.AMOUNT) AS SLMUA \
                WITH MAX(SLMUA) AS MAX \
                MATCH (BD)-[:FK_B_DETAILS_PRO]->(P) \
                WITH  P, MAX, SUM(BD.AMOUNT) AS SL \
                WHERE SL=MAX \
                RETURN P.PRO_ID, P.PRODUCT_NAME, SL").data()

d1 = pd.DataFrame(data1)
d2 = pd.DataFrame(data2)
df = pd.concat([d1,d2], ignore_index=True)
dsort = df.sort_values('SL', ascending = False)
result = dsort[dsort["SL"] == dsort['SL'].max()]
print(result)


# In[5]:
#4. Top 5 SẢN PHẨM ĐƯỢC BÁN NHIỀU NHẤT
from py2neo import Graph
import pandas as pd
import numpy as np

cn1 = Graph(uri="bolt://26.116.246.130:7687", auth=("neo4j","123456"))
cn2 = Graph(uri="bolt://26.198.144.133:7687", auth=("neo4j","123456"))
data1 = cn1.run("MATCH (BD)-[:FK_B_DETAILS_PRO]->(P) \
                RETURN P.PRO_ID,P.PRODUCT_NAME, SUM(BD.AMOUNT) AS SL \
                ORDER BY SL DESC").data()
data2 = cn2.run("MATCH (BD)-[:FK_B_DETAILS_PRO]->(P) \
                RETURN P.PRO_ID,P.PRODUCT_NAME, SUM(BD.AMOUNT) AS SL \
                ORDER BY SL DESC").data()

d1 = pd.DataFrame(data1)
d2 = pd.DataFrame(data2)
df = [d1,d2]
fulldata = pd.concat(df, ignore_index=True)

result = fulldata.sort_values('SL', ascending = False).head(5)

print(result)


# In[6]:
#5. TÌM TẤT CẢ KHÁCH HÀNG ĐÃ MUA có ít nhất 3 lần và được ít nhất 2 nhân viên khác nhau thanh toán
from py2neo import Graph
import pandas as pd
import numpy as np

cn1 = Graph(uri="bolt://26.116.246.130:7687", auth=("neo4j","123456"))
cn2 = Graph(uri="bolt://26.198.144.133:7687", auth=("neo4j","123456"))
data1 = cn1.run("MATCH (B:BILL)-[:FK_BILL_CUS]->(C:CUSTOMER) \
WITH C, count(B.BILL_ID) AS NUM_BILL,COUNT(B.EMP_ID) AS NUM_EMP \
WITH C, NUM_BILL, NUM_EMP \
RETURN C.CUS_ID, C.LAST_NAME, NUM_BILL, NUM_EMP \
ORDER BY NUM_EMP DESC, NUM_BILL DESC").data()
data2 = cn2.run("MATCH (B:BILL)-[:FK_BILL_CUS]->(C:CUSTOMER) \
WITH C, count(B.BILL_ID) AS NUM_BILL,COUNT(B.EMP_ID) AS NUM_EMP \
WITH C, NUM_BILL, NUM_EMP \
RETURN C.CUS_ID, C.LAST_NAME, NUM_BILL, NUM_EMP \
ORDER BY NUM_EMP DESC, NUM_BILL DESC").data()

d1 = pd.DataFrame(data1)
d2 = pd.DataFrame(data2)
df = [d1,d2]

fulldata = pd.concat(df, ignore_index=True).groupby(['C.CUS_ID', 'C.LAST_NAME']).sum()
result = fulldata[(fulldata["NUM_BILL"] >=3) & (fulldata["NUM_EMP"] >=2)]
print(result)


# In[7]:
#6. Tìm sản phẩm bán được ở cả 2 chi nhánh
from py2neo import Graph
import pandas as pd
import numpy as np

cn1 = Graph(uri="bolt://26.116.246.130:7687", auth=("neo4j","123456"))
cn2 = Graph(uri="bolt://26.198.144.133:7687", auth=("neo4j","123456"))
data1 = cn1.run("MATCH (BD:BILL_DETAILS)-[:FK_B_DETAILS_PRO]->(P:PRODUCT) \
                RETURN P.PRO_ID, P.PRODUCT_NAME").data()
data2 = cn2.run("MATCH (BD:BILL_DETAILS)-[:FK_B_DETAILS_PRO]->(P:PRODUCT) \
                RETURN P.PRO_ID, P.PRODUCT_NAME").data()

d1 = pd.DataFrame(data1)
d2 = pd.DataFrame(data2)
df = [d1,d2]
result = pd.merge(d1, d2 , on=["P.PRO_ID", "P.PRODUCT_NAME"], how="inner")
print(result.drop_duplicates())


# In[8]:
#7. Nhập vào mã nhân viên, cho biết nhân viên đó làm việc tại chi nhánh nào
from py2neo import Graph
import pandas as pd
import numpy as np

cn1 = Graph(uri="bolt://26.116.246.130:7687", auth=("neo4j","123456"))
cn2 = Graph(uri="bolt://26.198.144.133:7687", auth=("neo4j","123456"))

cypher_text = "MATCH(E: EMPLOYEE) WHERE E.EMP_ID = maNV RETURN E"
print("Nhập mã nhân viên cần tìm:")
eID = input()
cypher_text = cypher_text.replace("maNV",str(eID))
employee1 = cn1.run(cypher_text).data()
employee2 = cn2.run(cypher_text).data()

if(len(employee1) > 0):
    print("Nhân viên có mã " + str(eID) + " làm việc tại CN1")
else:
    if(len(employee2) > 0):
        print("Nhân viên có mã " + str(eID) + " làm việc tại CN2")
    else:
        print("Không tìm thấy nhân viên có mã " + str(eID))



# Thêm, xóa, sửa qua lại giữa hai máy
# In[9]:
#Tại máy CN2, thêm customer vào CN1
from py2neo import Graph
import pandas as pd
import numpy as np

cn1 = Graph(uri="bolt://26.116.246.130:7687", auth=("neo4j","123456"))
cn1.run("CREATE(C:CUSTOMER{ \
        CUS_ID:300021, \
        FIRST_NAME: 'Nguyen Huu', \
        LAST_NAME: 'Han', \
        PHONE: '883644231', \
        ADDRESS: '117/2 Nguyen Trai, Q5, TpHCM', \
        REG_DATE: '2022-09-01', \
        GENDER: 'Male', \
        POINT: 0, \
        BIRTHDAY: '2001-06-18', \
        SPENT_MONEY: 38760})").stats()


# In[10]:
#Tại máy CN2, XÓA CUSTOMER CÓ MÃ 300021 tại CN1
from py2neo import Graph
import pandas as pd
import numpy as np

cn1 = Graph(uri="bolt://26.116.246.130:7687", auth=("neo4j","123456"))
cn1.run("MATCH (C:CUSTOMER {CUS_ID: 300021}) DELETE C").stats()


# In[11]:
#Tại máy CN2, sửa phone của CUSTOMER CÓ mã 300021 thành 0921231741 tại CN1
from py2neo import Graph
import pandas as pd
import numpy as np

cn1 = Graph(uri="bolt://26.116.246.130:7687", auth=("neo4j","123456"))
cn1.run("MATCH (C:CUSTOMER {CUS_ID: 300021}) SET C.PHONE = '0921231741'").stats()


# In[12]:
#Tại máy CN1, thêm product vào CN2
from py2neo import Graph
import pandas as pd
import numpy as np
cn2 = Graph(uri="bolt://26.198.144.133:7687", auth=("neo4j","123456"))
cn2.run("CREATE(P:PRODUCT{ \
    PRO_ID: 400021, \
    PRODUCT_NAME: 'Soad 2',\
    SALE_PRICE: 8000, \
    PRODUCT_TYPE: 'Requisite', \
    ORIGINAL_PRICE: 5000, \
    COUNTRY: 'China', \
    VAT: 2, \
    REMAINING_QUANTITY: 30, \
    MFG: '2022-01-01', \
    EXP: '2023-01-01'})").stats()


# In[13]:
#Tại máy CN1 sửa nước sản xuất của product có mã 400021 ở CN2 thành VN
from py2neo import Graph
import pandas as pd
import numpy as np
cn2 = Graph(uri="bolt://26.198.144.133:7687", auth=("neo4j","123456"))
cn2.run(
    "MATCH (P:PRODUCT {PRO_ID: 400021}) \
    SET P.COUNTRY = 'VN'").stats()


# In[17]:
#Tại máy CN1 xóa product có mã 400021 ở CN2
from py2neo import Graph
import pandas as pd
import numpy as np
cn2 = Graph(uri="bolt://26.198.144.133:7687", auth=("neo4j","123456"))
cn2.run("MATCH (P:PRODUCT {PRO_ID: 400021}) DELETE P").stats()