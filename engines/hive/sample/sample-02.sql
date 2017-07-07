DROP TABLE IF EXISTS sample-02;
CREATE TABLE sample-02 stored as textfile as
"select did, cnt from
    ( select un, cnt, row_number() over (partition by cnt order by cnt) rn from
        ( select user_num as un, count(user_num) as cnt from tbl_data_event_1d group by user_num) tbl
    ) tbl2
where rn=1 order by cnt"
