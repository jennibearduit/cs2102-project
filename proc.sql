-- Constraint 1:
CREATE OR REPLACE FUNCTION shop_sell_func() RETURNS TRIGGER
AS $$
DECLARE
  products INT = 0;
BEGIN
  SELECT COUNT(*) INTO products
  FROM sells
  WHERE NEW.id = sells.shop_id;

  IF (products = 0) THEN
    RAISE EXCEPTION 'SHOP % SHOULD HAVE AT LEAST ONE PRODUCT!', NEW.id;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS shop_sell_trigger ON shop;

CREATE CONSTRAINT TRIGGER shop_sell_trigger
AFTER INSERT ON shop
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION shop_sell_func();

-- Constraint 2:
CREATE OR REPLACE FUNCTION order_prod_func() RETURNS TRIGGER
AS $$
DECLARE
  items INT = 0;
BEGIN
  SELECT COUNT(*) INTO items
  FROM orderline
  WHERE order_id = NEW.id;

  IF (items = 0) THEN
    RAISE EXCEPTION 'ORDER % HAS NO ASSOCIATED PRODUCTS!', NEW.id;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS order_prod_trigger ON orders;

CREATE CONSTRAINT TRIGGER order_prod_trigger
AFTER INSERT ON orders
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION order_prod_func();

-- Constraint 3:
CREATE OR REPLACE FUNCTION coupon_min_func() RETURNS TRIGGER
AS $$
DECLARE
  reward NUMERIC;
  min NUMERIC;
BEGIN
  SELECT reward_amount, min_order_amount
  INTO reward, min
  FROM coupon_batch
  WHERE id = NEW.coupon_id;
  
  IF (min > reward + NEW.payment_amount) THEN
    RAISE NOTICE 'ORDER % DOES NOT MEET MIN COUPON AMOUNT $%', NEW.id, min; 
    RETURN NULL;
  ELSE
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER coupon_min_trigger
BEFORE INSERT ON orders
FOR EACH ROW WHEN (NEW.coupon_id IS NOT NULL)
EXECUTE FUNCTION coupon_min_func();

-- Constraint 4:
CREATE OR REPLACE FUNCTION refund_qty_func() RETURNS TRIGGER
AS $$
DECLARE
  order_qty INT := 0;
  refund_qty INT := 0;
BEGIN
  SELECT quantity INTO order_qty
  FROM orderline
  WHERE order_id = NEW.order_id
    AND shop_id = NEW.shop_id
    AND product_id = NEW.product_id
    AND sell_timestamp = NEW.sell_timestamp;

  SELECT COALESCE(SUM(quantity),0) INTO refund_qty
  FROM refund_request
  WHERE order_id = NEW.order_id
    AND shop_id = NEW.shop_id
    AND product_id = NEW.product_id
    AND sell_timestamp = NEW.sell_timestamp
    AND status <> 'rejected';

  refund_qty := refund_qty + NEW.quantity; 
  IF (order_qty < refund_qty) THEN
    RAISE NOTICE 'ATTEMPTED TO REFUND % OUT OF % ORDERED ITEMS', refund_qty, order_qty;
    RETURN NULL;
  ELSE
    RETURN NEW;
  END IF;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER refund_qty_trigger
BEFORE INSERT ON refund_request
FOR EACH ROW
EXECUTE FUNCTION refund_qty_func();

-- Constraint 5: 
CREATE OR REPLACE FUNCTION refund_date_func() RETURNS TRIGGER
AS $$
DECLARE
  delivery_dt DATE;
  days INT;
BEGIN
  SELECT delivery_date INTO delivery_dt
  FROM orderline
  WHERE order_id = NEW.order_id
    AND shop_id = NEW.shop_id
    AND product_id = NEW.product_id
    AND sell_timestamp = NEW.sell_timestamp;

  days := NEW.request_date - delivery_dt;
  IF (days <= 30 AND days >= 0) THEN
    RETURN NEW; 
  ELSE
    RAISE NOTICE 'PRODUCT DELIVERED % DAYS AGO', days;
    RETURN NULL;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER refund_request_date
BEFORE INSERT ON refund_request
FOR EACH ROW
EXECUTE FUNCTION refund_date_func();

-- Constraint 6:
CREATE OR REPLACE FUNCTION refund_status_func() RETURNS TRIGGER
AS $$
DECLARE
  status TEXT;
BEGIN
  SELECT orderline.status INTO status
  FROM orderline
  WHERE order_id = NEW.order_id
  AND shop_id = NEW.shop_id
  AND product_id = NEW.product_id
  AND sell_timestamp = NEW.sell_timestamp;

  if (status = 'delivered') THEN
    RETURN NEW;
  ELSE
    RAISE NOTICE 'PRODUCT HAS NOT BEEN DELIVERED';
    RETURN NULL;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER refund_pdt_delivered
BEFORE INSERT ON refund_request
FOR EACH ROW
EXECUTE FUNCTION refund_status_func();

DROP TRIGGER IF EXISTS review_on_purchased_product_only ON public.review;
DROP TRIGGER IF EXISTS comment_type ON public.comment;
DROP TRIGGER IF EXISTS reply_has_version ON public.reply;
DROP TRIGGER IF EXISTS review_has_version ON public.review;
DROP TRIGGER IF EXISTS complaint_on_delivered_products_only ON public.delivery_complaint;
DROP TRIGGER IF EXISTS complaint_type ON public.complaint;

/* constraint 7 */
CREATE OR REPLACE FUNCTION check_review_on_purchased_product_only()
RETURNS TRIGGER AS $$
BEGIN 
    IF NOT EXISTS (
        SELECT DISTINCT 1
        FROM orderline OL join orders O on OL.order_id = O.id
        WHERE NEW.order_id = OL.order_id 
        AND NEW.product_id = OL.product_id
        AND NEW.shop_id = OL.shop_id
        AND NEW.sell_timestamp = OL.sell_timestamp
        AND O.user_id IN (
            SELECT C.user_id
            FROM comment C
            WHERE C.id = NEW.id
        )  
    ) THEN RAISE EXCEPTION 'Can only review on products purchased by the user!';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER review_on_purchased_product_only
BEFORE INSERT ON review
FOR EACH ROW 
EXECUTE FUNCTION check_review_on_purchased_product_only();

/* constraint 8 */
CREATE OR REPLACE FUNCTION check_comment_type() 
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.id IN (select id from review) AND NEW.id IN (select id from reply)) OR
        (NEW.id NOT IN (select id from review) AND NEW.id NOT IN (select id from reply)) THEN
    RAISE EXCEPTION 'Comment must be either a review or a reply, and not both!';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER comment_type
AFTER INSERT ON comment
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION check_comment_type();

/* constraint 9 */
CREATE OR REPLACE FUNCTION check_reply_has_version() 
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.id NOT IN (select distinct V.reply_id from reply_version V)) THEN
    RAISE EXCEPTION 'Reply must have at least one existing version!';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- So that can add first then add version
CREATE CONSTRAINT TRIGGER reply_has_version
AFTER INSERT ON reply
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW 
EXECUTE FUNCTION check_reply_has_version();

/* constraint 10 */
CREATE OR REPLACE FUNCTION check_review_has_version() 
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.id NOT IN (select distinct V.review_id from review_version V)) THEN
    RAISE EXCEPTION 'Review must have at least one existing version!';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER review_has_version
AFTER INSERT ON review
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW 
EXECUTE FUNCTION check_review_has_version();

/* constraint 11 */
CREATE OR REPLACE FUNCTION check_complaint_on_delivered_products_only() 
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM orderline OL join orders O on OL.order_id = O.id
        WHERE row(OL.order_id, OL.shop_id, OL.product_id, OL.sell_timestamp)
        = row(NEW.order_id, NEW.shop_id, NEW.product_id, NEW.sell_timestamp)
        AND OL.status = 'delivered'
        AND O.user_id IN (
            SELECT C.user_id
            FROM complaint C
            WHERE C.id = NEW.id            
        )
    ) THEN RAISE EXCEPTION 'Can only complain on delivered products purchased by the user!';
    ELSE RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER complaint_on_delivered_products_only
BEFORE INSERT ON delivery_complaint
FOR EACH ROW 
EXECUTE FUNCTION check_complaint_on_delivered_products_only();

/* constraint 12 */
CREATE OR REPLACE FUNCTION check_complaint_type() 
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.id IN (select id from delivery_complaint) AND (NEW.id IN (select id from shop_complaint) OR NEW.id IN (select id from comment_complaint))) OR
        (NEW.id IN (select id from shop_complaint) AND (NEW.id IN (select id from delivery_complaint) OR NEW.id IN (select id from comment_complaint))) OR
        (NEW.id IN (select id from comment_complaint) AND (NEW.id IN (select id from shop_complaint) OR NEW.id IN (select id from delivery_complaint))) OR
        (NEW.id NOT IN (select id from comment_complaint) AND NEW.id NOT IN (select id from shop_complaint) AND NEW.id NOT IN (select id from delivery_complaint)) THEN
    RAISE EXCEPTION 'Complaint must be either one of: delivery_complaint, shop_complaint, or comment_complaint';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER complaint_type
AFTER INSERT ON complaint
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW 
EXECUTE FUNCTION check_complaint_type();


-- Procedure 1: Place order
create or replace procedure place_order 
    (user_id integer, coupon_id integer, shipping_address text, shop_ids integer[], product_ids integer[], 
    sell_timestamps timestamp[], quantities integer[], shipping_costs numeric[])
as $$

DECLARE
    total_cost numeric := 0;
    product_cost numeric;
    item_count integer;
    new_order_id integer;
    coupon_value numeric;
    min_order numeric;

BEGIN
    item_count := coalesce(array_length(shop_ids, 1), 0);
    for idx in 1..item_count loop
        select price into product_cost
        from Sells
        where shop_id = shop_ids[idx]
        and product_id = product_ids[idx]
        and sell_timestamp = sell_timestamps[idx];

        total_cost := total_cost + product_cost * quantities[idx] + shipping_costs[idx];

    end loop;
    
    select reward_amount into coupon_value
    from Coupon_Batch 
    where id = coupon_id;

    -- Allow potentially negative payment amount so that trigger 3 can be applied
    if (coupon_value is not null) 
      then total_cost := total_cost - coupon_value;
    end if;
    
    insert into Orders (user_id, coupon_id, shipping_address, payment_amount)
    values (user_id, coupon_id, shipping_address, total_cost)
    returning id into new_order_id;

    for idx in 1..item_count loop
        insert into OrderLine 
        values (new_order_id, shop_ids[idx], product_ids[idx], sell_timestamps[idx], quantities[idx], shipping_costs[idx], 'being_processed', null);

        update Sells
        set quantity = quantity - quantities[idx]
        where shop_id = shop_ids[idx]
        and product_id = product_ids[idx]
        and sell_timestamp = sell_timestamps[idx];
        
    end loop;

END;
$$ language plpgsql;

-- Procedure 2: Review
create or replace procedure review
    (user_id integer, order_id integer, shop_id integer, product_id integer, sell_timestamp timestamp, content text, rating integer, comment_timestamp timestamp)
as $$

DECLARE
    new_comment_id integer;

BEGIN

    insert into Comment (user_id)
    values (user_id)
    returning id into new_comment_id;

    insert into Review
    values (new_comment_id, order_id, shop_id, product_id, sell_timestamp);

    insert into Review_Version
    values (new_comment_id, comment_timestamp, content, rating);

END;
$$ language plpgsql;


-- Procedure 3: Reply
create or replace procedure reply 
    (user_id integer, other_comment_id integer, content text, reply_timestamp timestamp)
as $$

DECLARE
    new_comment_id integer;
BEGIN

    insert into Comment (user_id)
    values (user_id)
    returning id into new_comment_id;

    insert into Reply
    values (new_comment_id, other_comment_id);

    insert into Reply_Version
    values (new_comment_id, reply_timestamp, content);
    
END;
$$ language plpgsql;

/* Function 1 */
CREATE OR REPLACE FUNCTION view_comments( in_shop_id INTEGER, in_product_id INTEGER, in_sell_timestamp TIMESTAMP )
RETURNS TABLE ( username TEXT, content TEXT, rating INTEGER, comment_timestamp
TIMESTAMP )
AS $$

/* Query 1 START */
with recursive ReviewReplyPairs(review_id, reply_id, user_id) as (
    select Rev.id as review_id, Rep.id as reply_id, C.user_id
    from (review Rev join (reply Rep natural join comment C) on Rev.id = Rep.other_comment_id)
    union
    select Rrp.review_id as root_id, Rep.id as reply_id, C.user_id
    from (ReviewReplyPairs Rrp join (reply Rep natural join comment C) on Rrp.reply_id = Rep.other_comment_id)
),
ProductReviewPairs(shop_id, product_id, sell_timestamp, review_id, user_id) as (
    select S.shop_id, S.product_id, S.sell_timestamp, R.id as review_id, R.user_id
    from (sells S join (review natural join comment) R 
    on S.shop_id = R.shop_id
    and S.product_id = R.product_id
    and S.sell_timestamp = R.sell_timestamp)
)
select username, content, rating, comment_timestamp
from (
    select 
        (
            select case 
                when U.account_closed then 'A Deleted User'
                else name
            end
            from users U where U.id = Prp.user_id
        ) as username,
        V.content as content,
        V.rating as rating,
        V.review_timestamp as comment_timestamp,
        V.review_id as comment_id
    from ProductReviewPairs Prp join review_version V on Prp.review_id = V.review_id
    where Prp.product_id = in_product_id /* param */
    and Prp.shop_id = in_shop_id /* param */
    and Prp.sell_timestamp = in_sell_timestamp /* param */
    and not exists (
        select 1
        from review_version V2
        where V2.review_timestamp > V.review_timestamp
        and V2.review_id = Prp.review_id
    )

    union all

    select 
        (
            select case 
                when U.account_closed then 'A Deleted User'
                else name
            end
            from users U where U.id = Rrp.user_id
        ) as username,
        V.content as content,
        null as rating,
        V.reply_timestamp as comment_timestamp,
        V.reply_id as comment_id
    from (
        ProductReviewPairs Prp join 
            (ReviewReplyPairs Rrp join reply_version V on Rrp.reply_id = V.reply_id)
        on Prp.review_id = Rrp.review_id
    )
    where Prp.product_id = in_product_id /* param */
    and Prp.shop_id = in_shop_id /* param */
    and Prp.sell_timestamp = in_sell_timestamp /* param */
    and not exists (
        select 1
        from reply_version V2
        where V2.reply_timestamp > V.reply_timestamp
        and V2.reply_id = Rrp.reply_id
    )

    order by comment_timestamp, comment_id
) as T
/* Query 1 END */

$$ LANGUAGE sql;

/* Function 2 */
CREATE OR REPLACE FUNCTION get_most_returned_products_from_manufacturer( in_manufacturer_id INTEGER, n INTEGER)
RETURNS TABLE ( product_id INTEGER, product_name TEXT, return_rate NUMERIC(3, 2) )
AS $$

with ProductDeliveredQuantity as (
    select product_id, coalesce(sum(quantity), 0) as delivered_quantity
    from orderline O
    group by (product_id, status)
    having (status = 'delivered')
),
ProductRefundedQuantity as (
    select product_id, coalesce(sum(quantity), 0) as refunded_quantity
    from refund_request R
    group by (product_id, status)
    having (status = 'accepted')
)
select P.id as product_id, P.name as product_name, case
        when delivered_quantity is null then 0.00
        when refunded_quantity is null then 0.00
        when delivered_quantity = 0 then 0.00
        else round(refunded_quantity * 1.00 / delivered_quantity, 2)
    end as return_rate
from (product P left join ProductDeliveredQuantity D on P.id = D.product_id) 
    left join ProductRefundedQuantity R on P.id = R.product_id
where P.manufacturer = in_manufacturer_id /* param */
order by return_rate desc, product_id
limit n /* params */

$$ LANGUAGE sql;

/* Function 3 */
CREATE OR REPLACE FUNCTION get_worst_shops( n INTEGER )
RETURNS TABLE( shop_id INTEGER, shop_name TEXT, num_negative_indicators INTEGER )
AS $$

with ShopComplaintCount as (
    select shop_id, count(*) as negative_score
    from shop_complaint
    group by (shop_id)
),
DeliveryComplaintCount as (
    select shop_id, count(*) as negative_score
    from delivery_complaint C
    where not exists (
        select 1
        from delivery_complaint C2
        where C.product_id = C2.product_id
        and C.shop_id = C2.shop_id
        and C.sell_timestamp = C2.sell_timestamp
        and C.order_id = C2.order_id
        and C.id < C2.id
    )
    group by (shop_id)
),
RefundRequestCount as (
    select shop_id, count(*) as negative_score
    from refund_request R
    where not exists (
        select 1
        from refund_request R2
        where R.product_id = R2.product_id
        and R.shop_id = R2.shop_id
        and R.sell_timestamp = R2.sell_timestamp
        and R.order_id = R2.order_id
        and R.id < R2.id
    )
    group by (shop_id)
),
OneStarReviewCount as (
    select shop_id, count(*) as negative_score
    from review R join review_version V on R.id = V.review_id
    where not exists (
        select 1
        from review R2 join review_version V2 on R2.id = V2.review_id
        where R2.id = R.id
        and V2.review_timestamp > V.review_timestamp
    ) and V.rating = 1
    group by (shop_id)
)
select S.id, S.name as shop_name, coalesce(sum(negative_score), 0) as num_negative_indicators
from (shop S left join (
    select * from RefundRequestCount
    union all
    select * from ShopComplaintCount
    union all
    select * from DeliveryComplaintCount
    union all
    select * from OneStarReviewCount
) as U on S.id = U.shop_id)
group by (S.id)
order by num_negative_indicators desc, S.id
limit n /* param */

$$ LANGUAGE sql;
