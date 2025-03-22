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
		print "Wykonane zapytanie "+str_query
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

query_select = "select DISTINCT id_bud_fb from obiekty_wokolbud_z_id_bud where kodobiektu in ('EGBC03','EGBC04','EGBL05','EGBN11','EGBP16','EGBI06') and id_bud_fb is not null" 
cur.execute(query_select)
rows_list =  cur.fetchall()
for id_budynku in rows_list:	
	print id_budynku [0]
	#query_select = "select ogc_fid from obiekty_wokolbud_z_id_bud where id_bud like '"+str(id_budynku[0])+"' ;"
	query_select = "select ogc_fid from obiekty_wokolbud_z_id_bud where id_bud_fb like '"+str(id_budynku[0])+"' and kodobiektu in ('EGBC03','EGBC04','EGBL05','EGBN11','EGBP16','EGBI06');"
	print query_select
	cur.execute(query_select)
	rows_list_ogc_fid =  cur.fetchall()	
	licznik = 1
	for ogc_fid in rows_list_ogc_fid:
		query_update = "update obiekty_wokolbud_z_id_bud set licz_blok ="+str (licznik)+" where ogc_fid = "+str(ogc_fid[0])+" ;"
		exec_query_commit (query_update)
		licznik = licznik + 1
			

os.system("pause")