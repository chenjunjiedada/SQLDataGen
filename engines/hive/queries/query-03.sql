DROP TABLE IF EXISTS q3_result;
CREATE TABLE q3_result stored as textfile as SELECT count(*) from tbl_data_event_1d WHERE ring_time > '3/1/2017' AND ring_time <= DATEADD(day,1,'3/31/2017') 
                 --make it inclusive for a datetime type
   -- AND DATEPART(hh,ring_time) >= 6 AND DATEPART(hh,ring_time) <= 22 
                 -- gets the hour of the day from the datetime
   -- AND DATEPART(dw,ring_time) >= 3 AND DATEPART(dw,ring_time) <= 5 
                 -- gets the day of the week from the datetime
;
