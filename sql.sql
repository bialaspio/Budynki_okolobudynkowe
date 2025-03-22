select *from Okolobudynkowe_bloki_budynkow where kodobiektu like 'EGBC03' -- blok budybku 
select *from Okolobudynkowe_bloki_budynkow where kodobiektu like 'EGBC03' -- blok budybku 

select *from Okolobudynkowe_bloki_budynkow where kodobiektu like 'EGBC04' -- blok budynku 
select *from obiekty_wokolbud where kodobiektu like 'EGBL05' -- lacznik napowietrzny 
select *from obiekty_wokolbud where kodobiektu like 'EGBN11' -- nawis
select *from obiekty_wokolbud where kodobiektu like 'EGBP16' -- inny ob bud
select *from obiekty_wokolbud where kodobiektu like 'EGBI06' -- 

select  kodobiektu, count (kodobiektu) from obiekty_wokolbud group by kodobiektu
"EGBS10";648
"EGBR13";2
"EGBC03";272
"EGBT07";202
"EGBP12";26
"EGBG08";97
"EGBN11";26


"EGBP15";2
"EGBW09";1
	"EGBC04";2
"EGBC03";616
"EGBG08";326
"EGBP12";90
"EGBS10";1738
"EGBL05";1
"EGBW14";22
"EGBT07";830
"EGBR13";4
"EGBN11";180
"EGBI17";7




select kodobiektu, count (kodobiektu) from taras group by kodobiektu -- "EGBT07"
select  kodobiektu, count (kodobiektu) from rampa group by kodobiektu --"EGBI17";7 "EGBW09";1  "EGBL05";2 "EGBC03";13 "EGBP15";3 "EGBR13";3 "EGBW14";23 "EGBN11";271
select  kodobiektu, count (kodobiektu) from schody group by kodobiektu -- "EGBS10"
select  kodobiektu, count (kodobiektu) from wer_gan group by kodobiektu -- "EGBG08"
select  kodobiektu, count (kodobiektu) from podpory group by kodobiektu -- "EGBP12"

drop table if exists taras;
drop table if exists rampa;
drop table if exists schody; 
drop table if exists wer_gan;
drop table if exists podpory;

create table taras as select *from obiekty_wokolbud where kodobiektu like 'EGBT04'; -- blok budynku
create table taras as select *from obiekty_wokolbud where kodobiektu like 'EGBT07'; -- tarasy 
create table rampa as select *from obiekty_wokolbud where kodobiektu in ('EGBI17','EGBW09','EGBL05','EGBC03','EGBP15','EGBR13','EGBW14','EGBN11','EGBP16','EGBC04'); -- tarasy 
create table schody as select *from obiekty_wokolbud where kodobiektu like 'EGBS10'; -- schody
create table wer_gan as select *from obiekty_wokolbud where kodobiektu like 'EGBG08'; -- schody
create table podpory as select *from obiekty_wokolbud where kodobiektu like 'EGBP12'; -- schody
--drop table budynki
--create table budynki as select *from Budynki_EGIB_zostaja_w_bazie;
drop table if exists budynki;
create table budynki as 
select ogc_fid,id , wkb_geometry from budynki_in;
-- !!!!!!!!!
   ERR
   Wywalone z okolobudynkowych te co mialy bledna gemetrie 
-- !!!!!!!!!

--*************************************************************************************************************************
-- PODPORY_PUNKT
--*************************************************************************************************************************

alter table elementy_okolobudynkowe_podpory_punkt drop id_budynku_fb ;
alter table elementy_okolobudynkowe_podpory_punkt add id_budynku_fb varchar;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT id from budynki B where ST_Contains(B.wkb_geometry,A.wkb_geometry));
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT id_budynku_fb from taras B where ST_Contains(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT id_budynku_fb from rampa B where ST_Contains(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT id_budynku_fb from schody B where ST_Contains(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT id_budynku_fb from wer_gan B where ST_Contains(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;


select ogc_fid, count (ogc_fid) as liczik from  
	(
	select AA.ogc_fid, BB.id from elementy_okolobudynkowe_podpory_punkt AA, budynki BB where ST_Intersects(BB.wkb_geometry,AA.wkb_geometry_buf)
	) as foo group by ogc_fid order by  count (ogc_fid)    


alter table elementy_okolobudynkowe_podpory_punkt drop wkb_geometry_buf;
alter table elementy_okolobudynkowe_podpory_punkt add wkb_geometry_buf  geometry;
update elementy_okolobudynkowe_podpory_punkt set wkb_geometry_buf = st_buffer (wkb_geometry, 0.01);

update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select min(id) from budynki B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from taras B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from rampa B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from schody B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from wer_gan B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;


alter table elementy_okolobudynkowe_podpory_punkt drop wkb_geometry_buf;
alter table elementy_okolobudynkowe_podpory_punkt add wkb_geometry_buf  geometry;
update elementy_okolobudynkowe_podpory_punkt set wkb_geometry_buf = st_buffer (wkb_geometry, 0.10);

update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select min(id) from budynki B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from taras B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from rampa B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from schody B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from wer_gan B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;


alter table elementy_okolobudynkowe_podpory_punkt drop wkb_geometry_buf;
alter table elementy_okolobudynkowe_podpory_punkt add wkb_geometry_buf  geometry;
update elementy_okolobudynkowe_podpory_punkt set wkb_geometry_buf = st_buffer (wkb_geometry, 0.20);

update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select min(id) from budynki B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from taras B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from rampa B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from schody B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from wer_gan B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;



alter table elementy_okolobudynkowe_podpory_punkt drop wkb_geometry_buf;
alter table elementy_okolobudynkowe_podpory_punkt add wkb_geometry_buf  geometry;
update elementy_okolobudynkowe_podpory_punkt set wkb_geometry_buf = st_buffer (wkb_geometry, 0.50);

update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select min(id) from budynki B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from taras B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from rampa B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from schody B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update elementy_okolobudynkowe_podpory_punkt A set id_budynku_fb = (select DISTINCT B.id_bud_fb from wer_gan B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;

select *from elementy_okolobudynkowe_podpory_punkt where id_budynku_fb is null;
alter table elementy_okolobudynkowe_podpory_punkt drop wkb_geometry_buf;
/*alter table elementy_okolobudynkowe_podpory_punkt drop id_bud varchar;
alter table elementy_okolobudynkowe_podpory_punkt drop jedn_ter varchar; 
alter table elementy_okolobudynkowe_podpory_punkt add ident varchar;
update elementy_okolobudynkowe_podpory_punkt set ident = id_budynku_fb;
alter table elementy_okolobudynkowe_podpory_punkt drop ident ;

alter table elementy_okolobudynkowe_podpory_punkt add id_bud varchar;
alter table elementy_okolobudynkowe_podpory_punkt add jedn_ter varchar; 
alter table elementy_okolobudynkowe_podpory_punkt drop id_bud ;
alter table elementy_okolobudynkowe_podpory_punkt drop jedn_ter ; 
*/

--update elementy_okolobudynkowe_podpory_punkt set jedn_ter = split_part(id_budynku_fb,'_',1)||'_'||split_part(id_budynku_fb,'_',2);
--update elementy_okolobudynkowe_podpory_punkt A set id_bud = (select ogc_fid from budynki_in B where A.id_budynku_fb = B.id_bud_fb)

alter table elementy_okolobudynkowe_podpory_punkt add teryt varchar ;
update elementy_okolobudynkowe_podpory_punkt set teryt ='240206_2';

--*************************************************************************************************************************
-- podpory poligony 
--*************************************************************************************************************************

alter table podpory drop id_budynku_fb ;
alter table podpory add id_budynku_fb varchar;
update podpory A set id_budynku_fb = (select DISTINCT id from budynki B where ST_Contains(B.wkb_geometry,A.wkb_geometry));
update podpory A set id_budynku_fb = (select DISTINCT id_bud_fb from taras B where ST_Contains(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;
update podpory A set id_budynku_fb = (select DISTINCT id_bud_fb from rampa B where ST_Contains(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;
update podpory A set id_budynku_fb = (select DISTINCT id_bud_fb from schody B where ST_Contains(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;
update podpory A set id_budynku_fb = (select DISTINCT id_bud_fb from wer_gan B where ST_Contains(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;
	
update podpory  A set id_budynku_fb = (select min (B.id) from budynki B where ST_Intersects(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;
update podpory  A set id_budynku_fb = (select DISTINCT B.id_bud_fb from taras B where ST_Intersects(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;
update podpory  A set id_budynku_fb = (select DISTINCT B.id_bud_fb from rampa B where ST_Intersects(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;
update podpory  A set id_budynku_fb = (select DISTINCT B.id_bud_fb from schody B where ST_Intersects(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;
update podpory  A set id_budynku_fb = (select DISTINCT B.id_bud_fb from wer_gan B where ST_Intersects(B.wkb_geometry,A.wkb_geometry)) where id_budynku_fb is null;

alter table podpory drop wkb_geometry_buf;
alter table podpory add wkb_geometry_buf  geometry;
update podpory set wkb_geometry_buf = st_buffer (wkb_geometry, 0.10);

update podpory A set id_budynku_fb = (select min(id) from budynki B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update podpory A set id_budynku_fb = (select DISTINCT B.id_bud_fb from taras B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update podpory A set id_budynku_fb = (select DISTINCT B.id_bud_fb from rampa B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update podpory A set id_budynku_fb = (select DISTINCT B.id_bud_fb from schody B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update podpory A set id_budynku_fb = (select DISTINCT B.id_bud_fb from wer_gan B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;

alter table podpory drop wkb_geometry_buf;
alter table podpory add wkb_geometry_buf  geometry;
update podpory set wkb_geometry_buf = st_buffer (wkb_geometry, 0.50);

update podpory A set id_budynku_fb = (select min(id) from budynki B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update podpory A set id_budynku_fb = (select DISTINCT B.id_bud_fb from taras B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update podpory A set id_budynku_fb = (select DISTINCT B.id_bud_fb from rampa B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update podpory A set id_budynku_fb = (select DISTINCT B.id_bud_fb from schody B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;
update podpory A set id_budynku_fb = (select DISTINCT B.id_bud_fb from wer_gan B where ST_Intersects(B.wkb_geometry,A.wkb_geometry_buf)) where id_budynku_fb is null;

alter table podpory drop wkb_geometry_buf;
alter table podpory add id_bud varchar;
alter table podpory add jedn_ter varchar; 
update podpory set id_bud = split_part(id_budynku_fb,'_',1)||'_'||split_part(id_budynku_fb,'_',2);
update podpory set jedn_ter =split_part(id_budynku_fb,'_',3);  

---- zebranie danych do jednej tabeli 

drop table tmp_okolobu_z_id_bud 
create table tmp_okolobu_z_id_bud as 
select *from taras union all 
select *from rampa union all 
select *from schody union all 
select *from wer_gan; 

obiekty_wokolbud_z_id_bud

drop table if exists obiekty_wokolbud_z_id_bud;
create table obiekty_wokolbud_z_id_bud as 
select *from tmp_okolobu_z_id_bud ;

alter table obiekty_wokolbud_z_id_bud add teryt varchar ;
update obiekty_wokolbud_z_id_bud set teryt ='240206_2';


alter table Okolobudynkowe_bloki_budynkow add id_bud varchar;

alter table Okolobudynkowe_bloki_budynkow add tmp varchar; 
alter table Okolobudynkowe_bloki_budynkow_rob add tmp varchar; 

/*
update Okolobudynkowe_bloki_budynkow set tmp = id::varchar||wkb_geometry::varchar; 
update Okolobudynkowe_bloki_budynkow_rob set tmp = id::varchar||wkb_geometry::varchar; 
update Okolobudynkowe_bloki_budynkow A set ident = (select id_budynku_fb from Okolobudynkowe_bloki_budynkow_rob B where A.tmp like B.tmp )
*/
update Okolobudynkowe_bloki_budynkow set id_bud = split_part(ident,'_',1)||'_'||split_part(ident,'_',2);
update Okolobudynkowe_bloki_budynkow set jedn_ter =split_part(ident,'_',3);  


alter table Okolobudynkowe_bloki_budynkow add id_bud varchar;
alter table Okolobudynkowe_bloki_budynkow add jedn_ter varchar; 
update Okolobudynkowe_bloki_budynkow set id_bud = split_part(ident,'_',1)||'_'||split_part(ident,'_',2);
update Okolobudynkowe_bloki_budynkow set jedn_ter =split_part(ident,'_',3);  



alter table obiekty_wokolbud_z_id_bud add licz_blok varchar;  