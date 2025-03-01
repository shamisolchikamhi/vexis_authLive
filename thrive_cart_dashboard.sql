delete from `thrive_cart.dashboard_data`
where date = current_date();

insert into `thrive_cart.dashboard_data`
select distinct  date, time(time) as time, transaction_id, transaction_type,item_type, item_name, item_pricing_option_name, t2.type as option_type,
t2.price as option_price, subscription_price, frequency, payments,
processor, currency, amount, invoice_id, order_id, internal_subscription_id, subscription_id,
customer.email as customer_email, customer.name as customer_name,
affiliate_commissions.balance_delay as commissions_balance_delay, affiliate_commissions.bump_percentage as commissions_bump_percentage,
affiliate_commissions.type as commissions_type,affiliate_commissions.custom as commissions_custom,
affiliate_commissions.payout_schedule as commission_payout_schedule,
affiliate_commissions.recurring_percentage as commissions_recurring_percentage,
affiliate_commissions.upfront_amount as commissions_upfront_amount,
affiliate_commissions.upfront_percentage as commissions_upfront_percentage,
t3.label, t3.url,  t3.statusString as status
from
(SELECT * FROM `arboreal-cat-451816-n0.thrive_cart.transactions`
WHERE date = current_date()
and item_type = 'product') t1
left join
(select distinct * from `thrive_cart.product_price_details` )t2
on t1.item_pricing_option_id = t2.id
and t1.item_pricing_option_name = t2.name
left join
(select distinct * from `thrive_cart.products`) t3
on t1.item_id = t3.product_id

UNION ALL

select distinct  date, time(time) as time, transaction_id, transaction_type,item_type, item_name, item_pricing_option_name, t2.type as option_type,
t2.price as option_price, subscription_price, frequency, payments,
processor, currency, amount, invoice_id, order_id, internal_subscription_id, subscription_id,
customer.email as customer_email, customer.name as customer_name,
null  as commissions_balance_delay, null as commissions_bump_percentage,
affiliate_commissions.type as commissions_type,affiliate_commissions.custom as commissions_custom,
''  as commission_payout_schedule,
affiliate_commissions.recurring_percentage as commissions_recurring_percentage,
null  as commissions_upfront_amount,
affiliate_commissions.upfront_percentage as commissions_upfront_percentage,
t3.label, '' as url,  '' as status
from
(SELECT * FROM `arboreal-cat-451816-n0.thrive_cart.transactions`
WHERE date = current_date()
and item_type = 'downsell') t1
left join
(select distinct * from `thrive_cart.downsells_price_details` )t2
on t1.item_pricing_option_id = t2.id
and t1.item_pricing_option_name = t2.name
left join
(select distinct * from `thrive_cart.downsells`) t3
on t1.item_id = t3.downsell_id

UNION ALL

select distinct  date, time(time) as time, transaction_id, transaction_type,item_type, item_name, item_pricing_option_name, t2.type as option_type,
t2.price as option_price, subscription_price, frequency, payments,
processor, currency, amount, invoice_id, order_id, internal_subscription_id, subscription_id,
customer.email as customer_email, customer.name as customer_name,
null as commissions_balance_delay, null as commissions_bump_percentage,
affiliate_commissions.type as commissions_type,affiliate_commissions.custom as commissions_custom,
'' as commission_payout_schedule,
affiliate_commissions.recurring_percentage as commissions_recurring_percentage,
null as commissions_upfront_amount,
affiliate_commissions.upfront_percentage as commissions_upfront_percentage,
t3.label, '' as url,  '' as status
from
(SELECT * FROM `arboreal-cat-451816-n0.thrive_cart.transactions`
WHERE date = current_date()
and item_type = 'upsell') t1
left join
(select distinct * from `thrive_cart.upsells_price_details` )t2
on t1.item_pricing_option_id = t2.id
and t1.item_pricing_option_name = t2.name
left join
(select distinct * from `thrive_cart.upsells`) t3
on t1.item_id = t3.upsell_id

UNION ALL

select distinct  date, time(time) as time, transaction_id, transaction_type,item_type, item_name, item_pricing_option_name, t2.type as option_type,
t2.price as option_price, null as subscription_price, '' as frequency, null as payments,
processor, currency, amount, invoice_id, order_id, internal_subscription_id, subscription_id,
customer.email as customer_email, customer.name as customer_name,
null as commissions_balance_delay, null as commissions_bump_percentage,
'' as commissions_type, false as commissions_custom,
'' as commission_payout_schedule,
null  commissions_recurring_percentage,
null as commissions_upfront_amount,
null as commissions_upfront_percentage,
'' as label, '' as url,  '' as status
from
(SELECT * FROM `arboreal-cat-451816-n0.thrive_cart.transactions`
WHERE date = current_date()
and item_type = 'bump') t1
left join
(select distinct * from `thrive_cart.bump_price_details` )t2
on t1.item_pricing_option_id = t2.id
and t1.item_pricing_option_name = t2.name
left join
(select distinct * from `thrive_cart.bumps`) t3
on t1.item_id = t3.bump_id;
