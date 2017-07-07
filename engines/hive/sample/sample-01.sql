DROP TABLE IF EXISTS sample01;
CREATE TABLE sample01 stored as textfile as
select did, cnt from
    ( select did, cnt, row_number() over (partition by cnt order by cnt) rn from
        ( select device_id as did, count(device_id) as cnt from tbl_data_event_1d group by device_id) tbl
    ) tbl2
where rn=1
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
