Answer A

select Order_desc , item_Desc , date_due_order , QTY ,Price_Item , sales_persons.Sales_person_name
from Orders JOIN sales_persons ON Orders.Sales_Person = sales_persons.Sales_Person ;


Answer B

select SUM(QTY * Price_Item) from Orders where date_due_order >= DATE('2016-01-01') AND date_due_order < DATE('2017-01-01') ;