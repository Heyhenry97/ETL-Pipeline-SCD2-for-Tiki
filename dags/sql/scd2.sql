merge into dim_product_scd2
using (
  -- The base staging data.
  select
    dim_product.tiki_pid as join_key,
    dim_product.* from dim_product

  union all

  -- Generate an extra row for changed records.
  -- The null join_key means it will be inserted.
  select
	null,
	dim_product.*
from
	dim_product
join dim_product_scd2 on
	dim_product.tiki_pid = dim_product_scd2.tiki_pid
where
	(( dim_product.name <> dim_product_scd2.name )
		or ( dim_product.origin <> dim_product_scd2.origin )
			or ( dim_product.brand_name <> dim_product_scd2.brand_name ) )
	and dim_product_scd2.valid_to is null
	and dim_product_scd2.is_current = true
) sub
on
	sub.join_key = dim_product_scd2.tiki_pid
	when matched
	and (sub.name <> dim_product_scd2.name
		or sub.origin <> dim_product_scd2.origin
		or sub.brand_name <> dim_product_scd2.brand_name )  
  
  then
update
set
	valid_to = sub.ingestion_dt_unix,
	is_current = false
	
	-- change to ingestion time
	when not matched
  then
insert
values (default,
sub.tiki_pid,
sub.name,
sub.brand_name,
sub.origin,
sub.ingestion_dt_unix,
sub.ingestion_dt_unix,
null,
true);