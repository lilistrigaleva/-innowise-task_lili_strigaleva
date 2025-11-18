--Выведите количество фильмов в каждой категории, отсортированное по убыванию.
SELECT 
    c.name AS category_name,
    COUNT(*) AS film_count
FROM 
    film_category fc
JOIN 
    category c ON fc.category_id = c.category_id
GROUP BY 
    c.name
ORDER BY 
    film_count DESC;


--Выведите 10 актеров, фильмы которых были в прокате чаще всего, отсортированных по убыванию.
SELECT 
    a.actor_id, a.first_name, a.last_name,
    COUNT(*) AS rental_count
FROM 
    rental r
JOIN 
    inventory i ON r.inventory_id = i.inventory_id
JOIN 
    film f ON i.film_id = f.film_id
JOIN 
    film_actor fa ON f.film_id = fa.film_id
JOIN 
    actor a ON fa.actor_id = a.actor_id
GROUP BY 
    a.actor_id, a.first_name, a.last_name
ORDER BY 
    rental_count DESC
LIMIT 10;


--Выведите категорию фильмов, на которые было потрачено больше всего денег.
SELECT 
	c.name AS category_name,
    SUM(f.replacement_cost) AS total_cost
FROM 
	category c 
JOIN 
	film_category fc ON c.category_id = fc.category_id
JOIN 
	film f ON fc.film_id = f.film_id
GROUP BY 
    c.name
ORDER BY 
    total_cost DESC
LIMIT 1;
	

--Выведите названия фильмов, которых нет в инвентаре. Составьте запрос без оператора IN.
SELECT 
    f.film_id, f.title
FROM 
    film f
WHERE NOT EXISTS (
    SELECT 1
    FROM inventory i
    WHERE i.film_id = f.film_id
);


--Выведите тройку актёров, которые чаще всего снимались в фильмах категории «Дети». 
--Если у нескольких актёров одинаковое количество фильмов, выведите их всех.
WITH actor_counts AS (
    SELECT 
        a.actor_id,
        a.first_name,
        a.last_name,
        COUNT(*) AS film_count
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film_category fc ON fa.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    WHERE c.name = 'Children'
    GROUP BY a.actor_id, a.first_name, a.last_name
)
SELECT actor_id, first_name, last_name, film_count
FROM actor_counts
WHERE film_count >= (
    SELECT MIN(film_count)
    FROM (
        SELECT film_count
        FROM actor_counts
        ORDER BY film_count DESC
        LIMIT 3
    ) top3 
)
ORDER BY film_count DESC;


--Вывести города с количеством активных и неактивных клиентов (active - customer.active = 1). 
--Сортировать по количеству неактивных клиентов в порядке убывания.
SELECT 
    ci.city,
    SUM(CASE WHEN cu.active = 1 THEN 1 ELSE 0 END) AS active_customers,
    SUM(CASE WHEN cu.active = 0 THEN 1 ELSE 0 END) AS inactive_customers
FROM customer cu
JOIN address ad ON cu.address_id = ad.address_id
JOIN city ci ON ad.city_id = ci.city_id
GROUP BY ci.city
ORDER BY inactive_customers DESC;


--Выведите категорию фильмов с наибольшим общим количеством часов проката в городе (customer.address_id в этом городе), 
--начинающихся на букву «a». 
--Сделайте то же самое для городов, в названиях которых есть «-». Запишите все в один запрос.
SELECT 
    c.name AS category_name,
    SUM(f.rental_duration) AS total_duration_hours 
FROM 
	category c
JOIN 
	film_category fc ON c.category_id = fc.category_id
JOIN 
	film f ON fc.film_id = f.film_id
JOIN 
	inventory i ON f.film_id = i.film_id         
JOIN 
	rental r ON i.inventory_id = r.inventory_id  
JOIN 
	customer cu ON r.customer_id = cu.customer_id 
JOIN 
	address a ON cu.address_id = a.address_id   
JOIN 
	city ci ON a.city_id = ci.city_id           
WHERE ci.city LIKE 'A%' OR ci.city LIKE '%-%'
GROUP BY c.name
ORDER BY total_duration_hours DESC;







