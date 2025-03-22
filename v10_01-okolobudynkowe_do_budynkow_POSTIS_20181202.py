#!/usr/bin/python2.7
# -*- coding: UTF-8 -*-#
#
# Small script to show PostgreSQL and Pyscopg together


import sys,os,glob,psycopg2,shutil,distutils.core,time

#_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*
def exec_query ( str_query ):
	try:
		teraz = time.asctime( time.localtime(time.time()))
		cur.execute(str_query) 
		print "Wykonane zapytanie " + str_query
		naz_log_log.write(teraz + "Q - "+str_query+'\n')
	except Exception, e:
		print '	Nie udalo sie wykonac zapytania:'+str_query
		err_log.write(teraz +' Q ERR:'+str_query+'\n')
		print e
	return cur
#_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*

def exec_query_commit( str_query ):
	try:
		teraz = time.asctime( time.localtime(time.time()))
		cur.execute(str_query) 
		conn.commit()
		print "Wykonane zapytanie "+str_query
		naz_log_log.write(teraz + "QC - "+str_query+'\n')
	except Exception, e:
		print '	Nie udalo sie wykonac zapytania:'+str_query
		print e
		err_log.write(teraz +' QC ERR:'+str_query+'\n')
		os.system("pause")
	return cur 	

#_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*
def przyisz_budynek (naz_obiektu):	#(np schody)
	naz_obiektu_005 = naz_obiektu + '_bud_005_p'
	naz_obiektu_025 = naz_obiektu + '_bud_025_p'
	naz_obiektu_0 = naz_obiektu + '_bud_0_p'
	
	query_select = "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='"+naz_obiektu+"' AND column_name='id_budynku';"
	
	rows_count = cur.execute(query_select)
	rows_list =  cur.fetchall()
	if rows_list:	
		query_alter = "alter table "+naz_obiektu+" drop id_budynku;"
		exec_query_commit(query_alter)
		query_alter = "alter table "+naz_obiektu+" add id_budynku int;"
		exec_query_commit(query_alter)
	else: 	
		query_alter = "alter table "+naz_obiektu+" add id_budynku int;"
		exec_query_commit(query_alter)

#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
#wyszukanie obiektow bez bufora 
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
	query_drop = "drop table if exists " + naz_obiektu_0 + ";"
	exec_query_commit(query_drop)
	
	query_drop = "drop table if exists "+naz_obiektu_0+";"
	exec_query_commit(query_drop)	
	
	query_create = "create table "+naz_obiektu_0 +" as SELECT A.ogc_fid AS ogc_bud, B.ogc_fid AS ogc_obiekt,ST_Area(ST_Intersection (A.wkb_geometry,B.wkb_geometry)) as wkb_intersects from budynki A, "+naz_obiektu+" B where ST_Intersects (A.wkb_geometry,B.wkb_geometry)and B.id_budynku is null;"
	exec_query_commit(query_create)
	query_alter = "alter table "+naz_obiektu_0+" add licznik int;"
	exec_query_commit(query_alter)

	query_update = "update "+naz_obiektu_0+" A set licznik = (select count (ogc_obiekt) from "+naz_obiektu_0+" B where A.ogc_obiekt = B.ogc_obiekt group by A.ogc_obiekt);"
	exec_query_commit(query_update)
	
	query_update ="update "+ naz_obiektu +" A set id_budynku = (select ogc_bud from "+naz_obiektu_0+" B where B.licznik = 1 and A.ogc_fid = B.ogc_obiekt) where id_budynku is null;"
	exec_query_commit(query_update)

#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
#wyszukanie obiektow z buforem okolo 10 cm
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
	tmp_naz_obiektu = 'tmp_'+naz_obiektu+"_005"

	query_drop = "drop table if exists " + naz_obiektu_005 + ";"
	exec_query_commit(query_drop)
	
	query_drop = "drop table if exists " + naz_obiektu_005 + ";"
	exec_query_commit(query_drop)
	
	query_drop = "drop table if exists " + tmp_naz_obiektu + ";"
	exec_query_commit(query_drop)	
	
	query_drop = "drop table if exists tmp_budynki_005;"
	exec_query_commit(query_drop)	
	
	query_create = "create table tmp_budynki_005 as select A.ogc_fid, ST_buffer (A.wkb_geometry, 0.05) as wkb_geometry from budynki A;"
	exec_query_commit(query_create)

	query_create = "create table " +tmp_naz_obiektu+" as select A.ogc_fid, A.id_budynku, ST_buffer (A.wkb_geometry, 0.05) as wkb_geometry from "+naz_obiektu+" A;"
	exec_query_commit(query_create)

	query_create = "create table "+naz_obiektu_005 +" as SELECT A.ogc_fid AS ogc_bud, B.ogc_fid AS ogc_obiekt, ST_Area(ST_Intersection (A.wkb_geometry, B.wkb_geometry)) as wkb_intersects from tmp_budynki_005 A, "+tmp_naz_obiektu+" B where ST_Intersects (A.wkb_geometry,B.wkb_geometry) and B.id_budynku is null;"
	exec_query_commit(query_create)

	print '2'
	query_alter = "alter table "+naz_obiektu_005+" add licznik int;"
	exec_query_commit(query_alter)
	
	print '3'
	query_update = "update "+naz_obiektu_005+" A set licznik = (select count (ogc_obiekt) from "+naz_obiektu_005+" B where A.ogc_obiekt = B.ogc_obiekt group by A.ogc_obiekt);"
	exec_query_commit(query_update)
	
	print '4'	
	query_update ="update "+ naz_obiektu +" A set id_budynku = (select ogc_bud from "+naz_obiektu_005+" B where B.licznik = 1 and A.ogc_fid = B.ogc_obiekt) where id_budynku is null;"
	exec_query_commit(query_update)
	print '5'
	
 	query_drop = "drop table if exists "+tmp_naz_obiektu+";"
	exec_query_commit(query_drop)	
	query_drop = "drop table if exists tmp_budynki_005;"
	exec_query_commit(query_drop)
	
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
#wyszukanie obiektow z buforem okolo 60 cm
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
	
	tmp_naz_obiektu = 'tmp_'+naz_obiektu+"_025"

	query_drop = "drop table if exists " + naz_obiektu_025 + ";"
	exec_query_commit(query_drop)
	
	query_drop = "drop table if exists " + naz_obiektu_025 + ";"
	exec_query_commit(query_drop)
	
	query_drop = "drop table if exists " + tmp_naz_obiektu + ";"
	exec_query_commit(query_drop)	
	
	query_drop = "drop table if exists tmp_budynki_025;"
	exec_query_commit(query_drop)	
	
	query_create = "create table tmp_budynki_025 as select A.ogc_fid, ST_buffer (A.wkb_geometry, 0.25) as wkb_geometry from budynki A;"
	exec_query_commit(query_create)

	query_create = "create table " +tmp_naz_obiektu+" as select A.ogc_fid, A.id_budynku, ST_buffer (A.wkb_geometry, 0.25) as wkb_geometry from "+naz_obiektu+" A;"
	exec_query_commit(query_create)

	query_create = "create table "+naz_obiektu_025 +" as SELECT A.ogc_fid AS ogc_bud, B.ogc_fid AS ogc_obiekt, ST_Area(ST_Intersection (A.wkb_geometry, B.wkb_geometry)) as wkb_intersects from tmp_budynki_025 A, "+tmp_naz_obiektu+" B where ST_Intersects (A.wkb_geometry,B.wkb_geometry) and B.id_budynku is null;"
	exec_query_commit(query_create)

	print '2'
	query_alter = "alter table "+naz_obiektu_025+" add licznik int;"
	exec_query_commit(query_alter)
	
	print '3'
	query_update = "update "+naz_obiektu_025+" A set licznik = (select count (ogc_obiekt) from "+naz_obiektu_025+" B where A.ogc_obiekt = B.ogc_obiekt group by A.ogc_obiekt);"
	exec_query_commit(query_update)
	
	print '4'	
	query_update ="update "+ naz_obiektu +" A set id_budynku = (select ogc_bud from "+naz_obiektu_025+" B where B.licznik = 1 and A.ogc_fid = B.ogc_obiekt) where id_budynku is null;"
	exec_query_commit(query_update)
	print '5'
	
 	query_drop = "drop table if exists "+tmp_naz_obiektu+";"
	exec_query_commit(query_drop)	
	query_drop = "drop table if exists tmp_budynki_025;"
	exec_query_commit(query_drop)


#_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*
def przy_bud_przez_obiekt(posredni,var_schody):
	#ampa_schod_bud 'Taras'
	posredni_schody_bud_000 = posredni+'_schod_bud_000'
	posredni_schody_bud_005 = posredni+'_schod_bud_005'
	posredni_schody_bud_025 = posredni+'_schod_bud_025'

	drop_query = "drop table if exists "+posredni_schody_bud_000+";"
	exec_query_commit(drop_query)
	drop_query = "drop table if exists "+posredni_schody_bud_005+";"
	exec_query_commit(drop_query)
	drop_query = "drop table if exists "+posredni_schody_bud_025+";"
	exec_query_commit(drop_query)
	
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
#wyszukanie obiektow bez bufora
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
	craete_query = "create table "+ posredni_schody_bud_000 +" as SELECT A.id_budynku AS ogc_bud, B.ogc_fid AS ogc_schody,ST_Area(ST_Intersection (A.wkb_geometry,B.wkb_geometry)) as wkb_intersects from "+ posredni+" A, "+ var_schody + " B where ST_Intersects (A.wkb_geometry,B.wkb_geometry)   AND B.id_budynku is null;"
	exec_query_commit(craete_query)
	
	alter_query = "alter table "+posredni_schody_bud_000+" add licznik int;"
	exec_query_commit(alter_query)
	
	update_query = "update "+posredni_schody_bud_000+" A set licznik = (select count (ogc_schody) from "+posredni_schody_bud_000+" B where A.ogc_schody = B.ogc_schody group by ogc_schody);"
	exec_query_commit(update_query)
	
	update_query = "update "+var_schody+" A set id_budynku = (select ogc_bud from "+posredni_schody_bud_000+" B where B.licznik = 1 and A.ogc_fid = B.ogc_schody) where id_budynku is null;"
	exec_query_commit(update_query)

#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
#wyszukanie obiektow bufor okolo 10 cm 
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~

	craete_query = "create table "+ posredni_schody_bud_005 +" as SELECT A.id_budynku AS ogc_bud, B.ogc_fid AS ogc_schody,ST_Area(ST_Intersection(ST_buffer (A.wkb_geometry, 0.05), ST_buffer (B.wkb_geometry, 0.05))) as wkb_intersects from "+ posredni+" A, "+ var_schody + " B where ST_Intersects (ST_buffer (A.wkb_geometry, 0.05), ST_buffer (B.wkb_geometry, 0.05)) AND B.id_budynku is null ;"
	exec_query_commit(craete_query)
	
	alter_query = "alter table "+posredni_schody_bud_005+" add licznik int;"
	exec_query_commit(alter_query)
	
	update_query = "update "+posredni_schody_bud_005+" A set licznik = (select count (ogc_schody) from "+posredni_schody_bud_005+" B where A.ogc_schody = B.ogc_schody group by ogc_schody);"
	exec_query_commit(update_query)
	
	update_query = "update "+var_schody+" A set id_budynku = (select ogc_bud from "+posredni_schody_bud_005+" B where B.licznik = 1 and A.ogc_fid = B.ogc_schody) where id_budynku is null;"
	exec_query_commit(update_query)

#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
#wyszukanie obiektow bufor okolo 50 cm 
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
	craete_query = "create table "+ posredni_schody_bud_025 +" as SELECT A.id_budynku AS ogc_bud, B.ogc_fid AS ogc_schody,ST_Area(ST_Intersection (ST_buffer (A.wkb_geometry, 0.25), ST_buffer (B.wkb_geometry, 0.25))) as wkb_intersects from "+ posredni+" A, "+ var_schody + " B where ST_Intersects (ST_buffer (A.wkb_geometry, 0.25), ST_buffer (B.wkb_geometry, 0.25)) AND B.id_budynku is null;"
	exec_query_commit(craete_query)
	
	alter_query = "alter table "+posredni_schody_bud_025+" add licznik int;"
	exec_query_commit(alter_query)
	
	update_query = "update "+posredni_schody_bud_025+" A set licznik = (select count (ogc_schody) from "+posredni_schody_bud_025+" B where A.ogc_schody = B.ogc_schody group by ogc_schody);"
	exec_query_commit(update_query)
	
	update_query = "update "+var_schody+" A set id_budynku = (select ogc_bud from "+posredni_schody_bud_025+" B where B.licznik = 1 and A.ogc_fid = B.ogc_schody) where id_budynku is null;"
	exec_query_commit(update_query)	
	
#_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*

def podwujne_przypisz_budynek (naz_obiektu):	#(np schody)
	naz_obiektu_005 = naz_obiektu + '_bud_005_p'
	naz_obiektu_025 = naz_obiektu + '_bud_025_p'
	naz_obiektu_000 = naz_obiektu + '_bud_0_p'
	
	qeuery_select = "select distinct ogc_obiekt from "+ naz_obiektu_000+" where licznik > 1"
	exec_query (qeuery_select )
	rows =  cur.fetchall()
	for row in rows:
		update_query = "update "+naz_obiektu+" A set id_budynku = (select ogc_bud from "+naz_obiektu_000+" B where B.licznik > 1 AND B.ogc_obiekt = A.ogc_fid order by wkb_intersects desc limit 1) where id_budynku is null;"
		exec_query_commit(update_query)	

	qeuery_select = "select distinct ogc_obiekt from "+ naz_obiektu_005+" where licznik > 1"
	exec_query (qeuery_select )
	rows =  cur.fetchall()
	for row in rows:
		update_query = "update "+naz_obiektu+" A set id_budynku = (select ogc_bud from "+naz_obiektu_005+" B where B.licznik > 1 AND B.ogc_obiekt = A.ogc_fid order by wkb_intersects desc limit 1) where id_budynku is null;"
		exec_query_commit(update_query)	

	qeuery_select = "select distinct ogc_obiekt from "+ naz_obiektu_025+" where licznik > 1"
	exec_query (qeuery_select )
	rows =  cur.fetchall()
	for row in rows:
		update_query = "update "+naz_obiektu+" A set id_budynku = (select ogc_bud from "+naz_obiektu_025+" B where B.licznik > 1 AND B.ogc_obiekt = A.ogc_fid order by wkb_intersects desc limit 1) where id_budynku is null;"
		exec_query_commit(update_query)	

#_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*

def	przy_bud_obiekt_przez_obiekt(naz_obiektu):	#(np schody)
	naz_obiektu_000 = naz_obiektu + '_'+naz_obiektu+'_0_p'
	naz_obiektu_005 = naz_obiektu + '_'+naz_obiektu+'_005_p'
	naz_obiektu_025 = naz_obiektu + '_'+naz_obiektu+'_025_p'
	
	naz_obiektu_dis_000 = naz_obiektu + '_'+naz_obiektu+'_000_dis'
	naz_obiektu_dis_005 = naz_obiektu + '_'+naz_obiektu+'_005_dis'
	naz_obiektu_dis_025 = naz_obiektu + '_'+naz_obiektu+'_025_dis'
		
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
#wyszukanie obiektow bez bufora
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
	drop_query = "drop table if exists "+ naz_obiektu_000+";"
	exec_query_commit(drop_query)	
	create_query = "create table "+naz_obiektu_000 +" AS SELECT A.id_budynku AS ogc_bud, B.ogc_fid AS ogc_schody from "+ naz_obiektu +" A,"+ naz_obiektu +" B where  ST_Intersects (A.wkb_geometry, B.wkb_geometry) AND B.id_budynku is null AND A.id_budynku is NOT null AND A.ogc_fid <> B.ogc_fid;"
	exec_query_commit(create_query)	
	drop_query = "drop table if exists "+naz_obiektu_dis_000+" ;"
	exec_query_commit(drop_query)	
	create_query = "create table "+naz_obiektu_dis_000 +" as select distinct *from "+naz_obiektu_000+" order by ogc_schody;"
	exec_query_commit(create_query)	
	alter_query = "alter table "+  naz_obiektu_dis_000+" add licznik int;"
	exec_query_commit(alter_query)	
	update_query = "update "+naz_obiektu_dis_000+" A set licznik = (select count (ogc_schody) from "+naz_obiektu_dis_000+" B where A.ogc_schody = B.ogc_schody group by ogc_schody);"
	exec_query_commit(update_query)	
	update_query = "update "+naz_obiektu+" A set id_budynku = (select ogc_bud from "+naz_obiektu_dis_000+" B where B.licznik = 1 and A.ogc_fid = B.ogc_schody) where id_budynku is null ;"
	exec_query_commit(update_query)	

#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
#wyszukanie obiektow bufor okolo 10cm 
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
	drop_query = "drop table if exists "+ naz_obiektu_005+";"
	exec_query_commit(drop_query)	
	create_query = "create table "+naz_obiektu_005 +" AS SELECT A.id_budynku AS ogc_bud, B.ogc_fid AS ogc_schody from "+ naz_obiektu +" A,"+ naz_obiektu +" B where  ST_Intersects (ST_buffer (A.wkb_geometry, 0.05), ST_buffer (B.wkb_geometry, 0.05)) AND B.id_budynku is null AND A.id_budynku is NOT null AND A.ogc_fid <> B.ogc_fid;"
	exec_query_commit(create_query)	
	drop_query = "drop table if exists "+naz_obiektu_dis_005+" ;"
	exec_query_commit(drop_query)	
	create_query = "create table "+naz_obiektu_dis_005 +" as select distinct *from "+naz_obiektu_005+" order by ogc_schody;"
	exec_query_commit(create_query)	
	alter_query = "alter table "+  naz_obiektu_dis_005+" add licznik int;"
	exec_query_commit(alter_query)	
	update_query = "update "+naz_obiektu_dis_005+" A set licznik = (select count (ogc_schody) from "+naz_obiektu_dis_005+" B where A.ogc_schody = B.ogc_schody group by ogc_schody);"
	exec_query_commit(update_query)	
	update_query = "update "+naz_obiektu+" A set id_budynku = (select ogc_bud from "+naz_obiektu_dis_005+" B where B.licznik = 1 and A.ogc_fid = B.ogc_schody) where id_budynku is null ;"
	exec_query_commit(update_query)	

#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
#wyszukanie obiektow bufor okolo 50cm 
#--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~--~~
	drop_query = "drop table if exists "+ naz_obiektu_025+";"
	exec_query_commit(drop_query)	
	create_query = "create table "+naz_obiektu_025 +" AS SELECT A.id_budynku AS ogc_bud, B.ogc_fid AS ogc_schody from "+ naz_obiektu +" A,"+ naz_obiektu +" B where  ST_Intersects (ST_buffer (A.wkb_geometry, 0.25), ST_buffer (B.wkb_geometry, 0.25)) AND B.id_budynku is null AND A.id_budynku is NOT null AND A.ogc_fid <> B.ogc_fid;"
	exec_query_commit(create_query)	
	drop_query = "drop table if exists "+naz_obiektu_dis_025+" ;"
	exec_query_commit(drop_query)	
	create_query = "create table "+naz_obiektu_dis_025 +" as select distinct *from "+naz_obiektu_025+" order by ogc_schody;"
	exec_query_commit(create_query)	
	alter_query = "alter table "+  naz_obiektu_dis_025+" add licznik int;"
	exec_query_commit(alter_query)	
	update_query = "update "+naz_obiektu_dis_025+" A set licznik = (select count (ogc_schody) from "+naz_obiektu_dis_025+" B where A.ogc_schody = B.ogc_schody group by ogc_schody);"
	exec_query_commit(update_query)	
	update_query = "update "+naz_obiektu+" A set id_budynku = (select ogc_bud from "+naz_obiektu_dis_025+" B where B.licznik = 1 and A.ogc_fid = B.ogc_schody) where id_budynku is null ;"
	exec_query_commit(update_query)		

#_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*
	
def przy_petla_bud_obiekt_przez_obiekt(obiekt):
	select_query = "select count (*) from " +obiekt+ " where id_budynku is null"
	result = exec_query(select_query)
	rows_list =  result.fetchall()
	licznik_po = -1 
	if rows_list:	
		for row in rows_list:
			print "JEST "+ str(row[0])
			licznik_wejcie = int(row[0])
			if licznik_wejcie > 0:
				print licznik_wejcie + licznik_po
				while licznik_wejcie > licznik_po:
					print "-------"
					print licznik_wejcie
					print licznik_po
					print "-------"
					if licznik_po <>-1:
						licznik_wejcie = licznik_po
					naz_log_log.write(teraz +"--"+ obiekt +" przez "+ obiekt +" - przypisanie budynkow \n")
					err_log.write(teraz +"--"+ obiekt +" przez "+ obiekt +" - przypisanie budynkow \n")
					przy_bud_obiekt_przez_obiekt (obiekt)
					result_po = exec_query(select_query)
					rows_list_po = result_po.fetchall()
					if rows_list_po:	
						for row_po in rows_list_po:
							print "JEST PO "+ str(row[0])
							licznik_po = int(row_po[0])
					print "++++++++"
					print licznik_wejcie
					print licznik_po
					print "++++++++"

	
			
	else: 	
		print "BRAK "+ str(rows_list[0])

#_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*
def przypisz_id_bud_FB ():
	
	alter_query = "alter table Taras add id_bud_FB varchar;"
	exec_query_commit(alter_query)
	update_query = "update Taras A set id_bud_FB = (SELECT id from budynki B where A.id_budynku = B.ogc_fid );"
	exec_query_commit(update_query)		
	
	alter_query = "alter table Wer_Gan add id_bud_FB varchar;"
	exec_query_commit(alter_query)
	update_query = "update Wer_Gan A set id_bud_FB = (SELECT id from budynki B where A.id_budynku = B.ogc_fid );"
	exec_query_commit(update_query)		
	
	alter_query = "alter table Rampa add id_bud_FB varchar;"
	exec_query_commit(alter_query)
	update_query = "update Rampa A set id_bud_FB = (SELECT id from budynki B where A.id_budynku = B.ogc_fid );"
	exec_query_commit(update_query)		
	
	alter_query = "alter table Schody add id_bud_FB varchar;"
	exec_query_commit(alter_query)
	update_query = "update Schody A set id_bud_FB = (SELECT id from budynki B where A.id_budynku = B.ogc_fid );"
	exec_query_commit(update_query)		
	
	
#_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*
	


		
if not os.path.isdir("__log_copy"):
	os.mkdir("__log_copy")
if not os.path.isdir("__csv"):
	os.mkdir("__csv")
if not os.path.isdir("__log_ERR"):
	os.mkdir("__log_ERR")
	
# Odczytanie pliku
naz_lik_log = time.strftime("%Y%m%d-%H_%M_%S")
naz_log_log = open('__log_copy\\'+naz_lik_log+'.log', 'w')
err_log = open ('__log_ERR\\ERR_'+naz_lik_log+'.csv', 'w')
przekopiowane_file  = open ('__csv\\przekopiowane_'+naz_lik_log+'.csv', 'w')

teraz = time.asctime( time.localtime(time.time()))
try:
	conn = psycopg2.connect("dbname='krakowski_budynki_v02' user='postgres' host='127.0.0.1' password='aaaaaa'")
	naz_log_log.write(teraz + ' [INF] Polaczono z baza danych\n')
except:
	print "I am unable to connect to the database"
	naz_log_log.write(teraz +' - [ERR] Nie udalo sie naiazac polacznia z baz\n')

cur = conn.cursor()

var_schody = 'Schody'	

naz_log_log.write(teraz +"-- SCHODY - przypisanie budynkow \n")
err_log.write(teraz +"-- SCHODY - przypisanie budynkow \n")
przyisz_budynek	(var_schody)

naz_log_log.write(teraz +"-- TARASY - przypisanie budynkow \n")
err_log.write(teraz +"-- TARASY - przypisanie budynkow \n")
przyisz_budynek	('Taras')

naz_log_log.write(teraz +"-- WERANDY - przypisanie budynkow \n")
err_log.write(teraz +"-- WERANDY - przypisanie budynkow \n")
przyisz_budynek	('Wer_Gan')

naz_log_log.write(teraz +"-- RAMPY - przypisanie budynkow \n")
err_log.write(teraz +"-- RAMPY - przypisanie budynkow \n")
przyisz_budynek	('Rampa')

naz_log_log.write(teraz +"-- SCHODY przez tarasy - przypisanie budynow \n")
err_log.write(teraz +"-- SCHODY przez tarasy - przypisanie budynkow \n")
przy_bud_przez_obiekt('Taras',var_schody)

naz_log_log.write(teraz +"-- SCHODY przez werandy - przypisanie budynkow \n")
err_log.write(teraz +"-- SCHODY przez werandy - przypisanie budynkow \n")
przy_bud_przez_obiekt('Wer_Gan',var_schody)

naz_log_log.write(teraz +"-- SCHODY przez rampy - przypisanie budynkow \n")
err_log.write(teraz +"-- SCHODY przez rampy - przypisanie budynkow \n")
przy_bud_przez_obiekt('Rampa',var_schody)

naz_log_log.write(teraz +"-- TARASY przylegle do kilku budynkow \n")
err_log.write(teraz +"-- TARASY przylegle do kilku budynkow \n")
podwujne_przypisz_budynek('Taras')

naz_log_log.write(teraz +"-- WERANDY przylegle do kilku budynkow \n")
err_log.write(teraz +"-- WERANDY przylegle do kilku budynkow \n")
podwujne_przypisz_budynek('Wer_Gan')

naz_log_log.write(teraz +"-- RAMPY przylegle do kilku budynkow \n")
err_log.write(teraz +"-- RAMPY przylegle do kilku budynkow \n")
podwujne_przypisz_budynek('Rampa')

naz_log_log.write(teraz +"-- SCHODY przylegle do kilku budynkow \n")
err_log.write(teraz +"-- SCHODY przylegle do kilku budynkow \n")
podwujne_przypisz_budynek(var_schody)

naz_log_log.write(teraz +"-- TARASY przez schody - przypisanie budynkow \n")
err_log.write(teraz +"-- TARASY przez schody - przypisanie budynkow \n")
przy_bud_przez_obiekt(var_schody,'Taras')

naz_log_log.write(teraz +"-- WEARNDY przez schody - przypisanie budynkow \n")
err_log.write(teraz +"-- WEARNDY przez schody - przypisanie budynkow \n")
przy_bud_przez_obiekt(var_schody,'Wer_Gan')

naz_log_log.write(teraz +"-- RAMPY przez schody - przypisanie budynkow \n")
err_log.write(teraz +"-- RAMPY przez schody - przypisanie budynkow \n")
przy_bud_przez_obiekt(var_schody,'Rampa')

naz_log_log.write(teraz +"-- SCHODY II przez rampy - przypisanie budynkow \n")
err_log.write(teraz +"-- SCHODY II przez rampy - przypisanie budynkow \n")
przy_bud_przez_obiekt('Rampa',var_schody)

naz_log_log.write(teraz +"-- SCHODY II przez tarasy - przypisanie budynkow \n")
err_log.write(teraz +"-- SCHODY II przez tarasy - przypisanie budynkow \n")
przy_bud_przez_obiekt('Taras',var_schody)

naz_log_log.write(teraz +"-- SCHODY II przez werandy - przypisanie budynkow \n")
err_log.write(teraz +"-- SCHODY II przez werandy - przypisanie budynkow \n")
przy_bud_przez_obiekt('Wer_Gan',var_schody)

naz_log_log.write(teraz +"-- TARASY przez TARASY - przypisanie budynkow \n")
err_log.write(teraz +"-- TARASY przez TARASY - przypisanie budynkow \n")
przy_bud_obiekt_przez_obiekt ('Taras')

naz_log_log.write(teraz +"-- WERANDY przez WERANDY - przypisanie budynkow \n")
err_log.write(teraz +"-- WERANDY przez WERANDY - przypisanie budynkow \n")
przy_bud_obiekt_przez_obiekt ('Wer_Gan')

naz_log_log.write(teraz +"-- RAMPY przez RAMPY - przypisanie budynkow \n")
err_log.write(teraz +"-- RAMPY przez RAMPY - przypisanie budynkow \n")
przy_bud_obiekt_przez_obiekt ('Rampa')

naz_log_log.write(teraz +"-- SCHODY przez SCHODY - przypisanie budynkow \n")
err_log.write(teraz +"-- SCHODY przez SCHODY - przypisanie budynkow \n")
przy_bud_obiekt_przez_obiekt (var_schody)

przy_petla_bud_obiekt_przez_obiekt(var_schody)

przypisz_id_bud_FB ()

os.system("pause")