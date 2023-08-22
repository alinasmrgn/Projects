--1-- popular apps with bad realization
SELECT a.app_name ,c.cat_name , a.rating , i.installs , i.release_date , i.last_update 
FROM installs i
INNER JOIN apps a
ON i.app_id =a.app_id 
INNER JOIN category c 
ON a.cat_id =c.cat_id 
WHERE a.rating < 3 AND i.installs > 100000
ORDER BY i.installs desc;

--2--the most popular category
SELECT category, sum(install) AS sum_of_installs
FROM 
    (SELECT c.cat_name AS category ,i.installs AS install
    FROM category c
    INNER JOIN apps a 
    ON c.cat_id=a.cat_id  
    INNER JOIN installs i 
    ON a.app_id =i.app_id 
    
   ) qwe
GROUP BY category
ORDER BY sum_of_installs DESC
    
--3--correlation between rating and installs
SELECT rating, sum(installs) AS sum_of_installs
FROM 
    (SELECT a.rating  ,i.installs 
    FROM apps a  
    INNER JOIN installs i 
    ON a.app_id =i.app_id 
    
   ) qwe
GROUP BY rating
ORDER BY sum_of_installs DESC
 
 
--4--correlation between rating and the percentage of rating_count to installs
SELECT rating, concat(to_char((SUM(rating_count)*100 /  sum(sum(installs))
                              OVER (PARTITION BY rating)), '99.999'), ' %') as percentage
FROM 
    (SELECT a.rating ,a.rating_count, i.installs 
    FROM apps a  
    INNER JOIN installs i 
    ON a.app_id =i.app_id
    
   ) qwe
WHERE rating > 0
GROUP BY rating
ORDER BY percentage DESC






