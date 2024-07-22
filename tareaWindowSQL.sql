
--1 : Obtener el promedio de precios por cada categoría de producto. 
--La cláusula OVER(PARTITION BY CategoryID) específica que 
--se debe calcular el promedio de 
--precios por cada valor único de CategoryID en la tabla.

select c.category_name,
p.product_name, 
od.unit_price,
AVG(od.unit_price) over (partition by c.category_id) 
as avgpricecategory
from categories c 
inner join products p  
on c.category_id = p.category_id 
inner join order_details od 
on p.product_id = od.product_id;


--2: Obtenerelpromediodeventadecadacliente

select 
avg(od.quantity*od.unit_price) over (partition by c.customer_id),
od.order_id,
c.customer_id,
e.employee_id,
o.order_date,
o.required_date,
o.shipped_date
from order_details od 
inner join orders o 
on od.order_id = o.order_id 
inner join customers c 
on o.customer_id = c.customer_id 
inner join employees e 
on c.customer_id = o.customer_id ;

--3 Obtener el promedio de cantidad de productos vendidos por categoría 
--(product_name, quantity_per_unit, unit_price, quantity, avgquantity) 
--y ordenarlo por nombre de la categoría y nombre del producto

select 
p.product_name,
c.category_name,
p.quantity_per_unit,
p.unit_price,
od.quantity,
avg(od.quantity) over (partition by c.category_name)
from products p 
inner join  order_details od 
on p.product_id = od.product_id 
inner join categories c 
on c.category_id = p.category_id 
order by 2, 1;

--4 : SeleccionaelIDdelcliente,
--lafechadelaordenylafechamásantiguadela 
--orden para cada cliente de la tabla 'Orders'.

select
c.customer_id,
o.order_date,
min(o.order_date) over (partition by c.customer_id)
from customers c 
inner join orders o 
on c.customer_id = o.customer_id ;

--5: Seleccioneeliddeproducto,
--elnombredeproducto,elpreciounitario,
--elidde categoría y el precio unitario máximo 
--para cada categoría de la tabla Products.

select 
p.product_id,
p.product_name,
p.unit_price,
p.category_id,
max(p.unit_price) over (partition by p.category_id) as maxunitprice
from products p ;

--6 : Obtener el ranking de los productos más vendidos

select 
rank () over (partition by p.product_name order by od.quantity desc) as ranking,
p.product_name,
od.quantity as totalquantity
from products p 
inner join order_details od 
on p.product_id = od.product_id ;


--7 : Asignarnumerosdefilaparacadacliente,
--ordenadosporcustomer_id

select 
row_number () over ( order by c.customer_id) as rownumber,
c.customer_id,
c.company_name,
c.contact_name,
c.contact_title,
c.address
from customers c ;

--8 : Obtenerelrankingdelosempleadosmásjóvenes()ranking,
--nombreyapellidodel 
--empleado, fecha de nacimiento)

select 
rank() over ( order by e.birth_date desc),
e.first_name ,
e.birth_date
from employees e;


--9 : Obtenerlasumadeventadecadacliente

select sum(od.quantity*od.unit_price) over (partition by c.customer_id ) as sumorderamount,
o.order_id,
c.customer_id, 
e.employee_id,
o.order_date,
o.required_date
from order_details od 
inner join orders o 
on od.order_id= o.order_id
inner join customers c 
on o.customer_id = c.customer_id 
inner join employees e 
on e.employee_id = o.employee_id; 

--10 : Obtener la suma total de ventas por categoría de producto

select c.category_name,
p.product_name,
od.unit_price,
od.quantity,
sum(od.quantity*od.unit_price) over (partition by c.category_name) as totalsales
from categories c 
inner join products p 
on c.category_id = p.category_id 
inner join order_details od 
on p.product_id = od.product_id ;



--11: Calcular la suma total de gastos de envío 
--por país de destino, luego ordenarlo por país 
--y por orden de manera ascendente


select 
o.ship_country,
o.order_id,
o.shipped_date,
o.freight,
sum(o.freight) over (partition by o.ship_country order by o.ship_country,2 asc) as totalshippingcost
from orders o ;

--12: Ranking de ventas por cliente

select 
c.customer_id,
c.company_name,
sum(od.quantity*od.unit_price) as TotalSales,
rank () over (partition by c.company_name order by 3 desc) as rank
from customers c 
inner join orders o 
on c.customer_id = o.customer_id 
inner join order_details od 
on o.order_id = od.order_id 
group by c.customer_id;

--13: Ranking de empleados por fecha de contratacion

select 
e.employee_id,
e.first_name,
e.last_name,
e.hire_date, 
rank () over (partition by e.hire_date)
from employees e ;

--14: Ranking de productos por precio unitario

select 
p.product_id,
p.product_name,
od.unit_price,
rank() over (partition by p.product_id order by od.unit_price asc) as rank_
from products p 
inner join order_details od 
on p.product_id = od.product_id ;

--15: Mostrar por cada producto de una orden, 
--la cantidad vendida y 
--la cantidad vendida del producto previo.

select 
od.order_id, 
od.product_id,
od.quantity,
lag(od.quantity) over (partition by od.order_id order by od.order_id asc)
from order_details od ;

--16: Obtener un listado de ordenes mostrando el id de la orden, 
--fecha de orden, 
--id del cliente y última fecha de orden.

select 
o.order_id,
o.order_date,
o.customer_id,
lag(o.order_date) over (partition by o.customer_id order by o.order_date asc)
from orders o ;

--17: Obtener un listado de productos que contengan: id de producto, nombre del producto, 
--precio unitario, precio del producto anterior, diferencia entre el precio 
--del producto y precio del producto anterior.

select 
p.product_id,
p.product_name,
p.unit_price,
lag(p.unit_price) 
over (partition by p.product_id order by p.product_id asc) 
as lastunitprice
from products p ;

--18: Obtener un listado que muestra el precio
-- de un producto junto con el precio del producto siguiente:

select 
p.product_name,
p.unit_price, 
lead (p.unit_price) 
over (partition by p.product_name order by p.unit_price desc) as nextprice
from products p ;


--19: Obtener un listado que muestra el total de ventas por categoría de producto junto 
--con el total de ventas de la categoría siguiente

select 
c.category_name,
(od.quantity*od.unit_price) as totalsales,
lead((od.quantity*od.unit_price)) 
over (partition by c.category_name order by 1 asc)
from categories c 
inner join products p 
on c.category_id = p.category_id 
inner join order_details od 
on p.product_id = od.product_id ;












