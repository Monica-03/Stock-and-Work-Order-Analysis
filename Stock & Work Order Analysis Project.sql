#Stock & Work Order Analysis Project

create database project;
use project;
create table project.Date_wise_report(
Sale_Date varchar(225),
Qty int,
Item_Type varchar(255),
Job_Status varchar(255),
Planner int,
Buyer_Name varchar(255),
Sale_id int,
Preferred_Supplier varchar(255),
Safety varchar(225),
Pre_PLT int,
Post_PLT int,
LT int,
Run_Total int,
Late int,
Safety_RT int,
PO_Note varchar(225),
Net_Neg varchar(225),
Last_Neg varchar(225),
Item_Category varchar(225),
Created_On_Date varchar(225)
);

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Date_wise_report.csv'
into table project.date_wise_report
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

create table project.Order_Status(
Trans varchar(225),
Negative varchar(225),
Order_Type varchar(225),
Assembly_Supplier varchar(225),
Ref	varchar(225),
Order_id varchar(225),	
Sale_id	int,
Description text
);

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Order_Status.csv'
into table project.order_status
fields terminated by ','
enclosed by '"'
escaped by '\\'
lines terminated by '\n'
ignore 1 rows;

-- 1.  We need to  calculate the Stock count & work order count based on order_id 
       -- Hint (you find the stock & work order in Order type field)
       
select Order_id, 
count(case when Order_Type="Stock" then 1 end) as Stock_Count, 
count(case when Order_Type="Work_Order" then 1 end) as Work_Order_Count 
from project.Order_Status 
group by Order_id;

-- 2.  next you calculate Work_order_pending Status
       -- Hint(Work_order_Pending_status = stock_count – Work_order count) 
      
select Order_id, 
count(case when Order_Type="Stock" then 1 end) as Stock_Count, 
count(case when Order_Type="Work_Order" then 1 end) as Work_Order_Count,
(count(case when Order_Type="Stock" then 1 end) - count(case when Order_Type="Work_Order" then 1 end)) as Work_order_Pending_status 
from project.Order_Status 
group by Order_id;

-- 3. finally you close the work_order
      -- Conditions
-- (i) creat a new field (Field name work_order_closed_or_not
          
alter table project.Order_Status
add column work_order_closed_or_not varchar(100);

-- (ii)  Work_order_pending status < 0 Then update order_closed other wise Order_pending (apply logical function)
                   
update project.Order_Status as os
join (
select 
Order_id,
count(case when Order_Type = 'Stock' then 1 end) as Stock_Count,
count(case when Order_Type = 'Work_Order' then 1 end) as Work_Order_Count
from project.Order_Status
group by Order_id
) as counts on os.Order_id = counts.Order_id
set os.Work_Order_closed_or_not = 
case 
	when (counts.Stock_Count - counts.Work_Order_Count) < 0 then 'order_closed' 
	else 'order_pending' 
end;

-- 4. you need to create a new table after completing pending status (table name: Order_pending_status)

create table project.Order_pending_status (
    Order_id varchar(100),
    Stock_Count int,
    Work_Order_Count int,
    Work_Order_Pending_Status int,
    Work_Order_Closed_or_not varchar(20)
);

insert into project.Order_pending_status 
(Order_id, Stock_Count, Work_Order_Count, Work_Order_Pending_Status, Work_Order_Closed_or_not)
select os.Order_id,
count(case when os.Order_Type = 'Stock' then 1 end) as Stock_Count,
count(case when os.Order_Type = 'Work_Order' then 1 end) as Work_Order_Count,
(count(case when os.Order_Type = 'Stock' then 1 end) - count(case when os.Order_Type = 'Work_Order' then 1 end)) as Work_Order_Pending_status,
case 
	when (count(case when os.order_type = 'Stock' then 1 end) - count(case when os.order_type = 'Work Order' then 1 end)) < 0 then 'order_closed' 
	else 'order_pending' 
    end as work_order_closed_or_not
from project.order_status os
group by os.order_id;

select * from project.Order_pending_status;

-- 5. We need to create a second table while using join     
      -- (table name : order_supplier_report)  Joining tables
                  #Table 1 – order_status
				  #Table 2 -  Date_wise _supplier
                  
create table project.order_supplier_report as
(select a.*,b.Trans,b.Negative,b.Order_Type,b.Assembly_Supplier,b.Ref,b.Order_id,b.Description
from project.date_wise_report a
join project.order_status b
on a.sale_id = b.sale_id
);

select * from project.order_supplier_report;

-- 6. After creating second table find out the reports
-- (I)  Date_wise Quantity & Order_id count

select Sale_Date,
count(distinct Order_id) as Order_id_Count,
sum(Qty) as Date_wise_Quantity
from project.order_supplier_report 
group by Sale_Date;

-- (II)   you can split the supplier_name while using comma delimiter 
		-- For ex   Kumar N, Mr.Vinay will be Kumar N(last_name), Mr.vinay (first_name).

select Buyer_Name,
trim(substring_index(Buyer_Name, ',', -1)) First_Name,
trim(substring_index(Buyer_Name, ',', 1)) Last_Name
from project.order_supplier_report;

-- 7. Finally you stored the all reports and tables while using stored procedure.

delimiter //

create procedure stock_work_order_analysis()
begin
    -- Step 1: Calculate Stock Count & Work Order Count
    create temporary table temp_order_counts as
    select order_id,
	count(case when order_type = 'Stock' then 1 end) as stock_count,
	count(case when order_type = 'Work_Order' then 1 end) as work_order_count
    from project.order_status
    group by order_id;

    -- Step 2: Calculate Work Order Pending Status
    create temporary table temp_pending_status as
    select oc.order_id,oc.stock_count,oc.work_order_count,
	(oc.stock_count - oc.work_order_count) as work_order_pending_status
    from temp_order_counts oc;

    -- Step 3: Update the 'work_order_closed_or_not' column based on Work_Order_Pending_Status
    update project.order_status os
    join temp_pending_status ps
    on os.order_id = ps.order_id
    set os.work_order_closed_or_not = 
	case 
		when ps.work_order_pending_status < 0 then 'order_closed'
		else 'order_pending'
	end;

    -- Step 4: Create the order_pending_status table
    create table if not exists project.order_pending_status as
    select ps.order_id,ps.stock_count,ps.work_order_count,ps.work_order_pending_status,
	case 
        when ps.work_order_pending_status < 0 then 'order_closed'
	    else 'order_pending'
	end as work_order_closed_or_not
    from temp_pending_status ps;

    -- Step 5: Create the order_supplier_report table by joining date_wise_report and order_status
    create table if not exists project.order_supplier_report as
    select dwr.*, os.trans, os.negative, os.order_type, os.assembly_supplier, os.ref, os.order_id, os.description
    from project.date_wise_report dwr
    join project.order_status os
    on dwr.sale_id = os.sale_id;

    -- Step 6: Generate Date-wise Quantity & Order_id count
    create temporary table temp_date_wise_quantity as
    select dwr.sale_date,
    count(distinct dwr.order_id) as order_id_count,
	sum(dwr.qty) as date_wise_quantity
    from project.order_supplier_report dwr
    group by dwr.sale_date;

    -- Split Supplier Name using comma delimiter
    create temporary table temp_supplier_name_split as
    select os.buyer_name,
	trim(substring_index(os.buyer_name, ',', -1)) as first_name,
	trim(substring_index(os.buyer_name, ',', 1)) as last_name
    from project.order_supplier_report os;

    -- Store the reports into respective tables
    create table if not exists project.date_wise_quantity_report as
    select * from temp_date_wise_quantity;

    create table if not exists project.supplier_name_split as
    select * from temp_supplier_name_split;

end//

delimiter ;

call stock_work_order_analysis();

-- 8. Export the All reports for our reference.

select * into outfile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Reports/order_pending_status.csv'
fields terminated by ',' 
optionally enclosed by '"' 
lines terminated by '\n'
from project.order_pending_status;

select * into outfile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Reports/order_supplier_report.csv'
fields terminated by ',' 
optionally enclosed by '"' 
lines terminated by '\n'
from project.order_supplier_report;

delimiter //

create procedure export_reports()
begin
    -- create a temporary table for date wise quantity report
    create temporary table if not exists date_wise_quantity_report as
    select sale_date,
	count(distinct order_id) as order_id_count,
	sum(qty) as date_wise_quantity
    from project.order_supplier_report 
    group by sale_date;

    -- export date wise quantity report to CSV
    select * 
    into outfile 'c:/programdata/mysql/mysql server 8.0/uploads/reports/date_wise_quantity_report.csv'
    fields terminated by ',' 
    optionally enclosed by '"' 
    lines terminated by '\n'
    from date_wise_quantity_report;

    -- create a temporary table for supplier name split report
    create temporary table if not exists supplier_name_split_report as
    select buyer_name,
	trim(substring_index(buyer_name, ',', -1)) as first_name,
	trim(substring_index(buyer_name, ',', 1)) as last_name
    from project.order_supplier_report;

    -- export supplier name split report to CSV
    select * 
    into outfile 'c:/programdata/mysql/mysql server 8.0/uploads/reports/supplier_name_split_report.csv'
    fields terminated by ',' 
    optionally enclosed by '"' 
    lines terminated by '\n'
    from supplier_name_split_report;

end //

delimiter ;

call export_reports();
