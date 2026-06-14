#!/bin/bash
# ==============================================================================
# Skripta 01_start_and_prepare.sh pokrece Docker kontejnere i priprema HDFS zone.
# ==============================================================================

# Lokacija docker-compose.yml fajla
PROJECT_DIR="/home/vlada/Documents/asvvsp/asvsp_kt33"

echo "=================================================="
echo "  KORAK 1: Pokretanje Docker-a i priprema HDFS-a"
echo "=================================================="

# 1. Provera i podizanje Docker kontejnera
echo "Pokrecu se Docker kontejneri..."
cd $PROJECT_DIR

# Podizu se kontejneri u pozadini ako vec nisu podignuti
docker compose up -d

echo "Cekamo da kljucni servisi budu potpuno spremni (healthy)..."
until [ "$(docker inspect --format='{{.State.Health.Status}}' postgresql)" = "healthy" ]; do
    echo "Cekam bazu podataka (PostgreSQL)..."
    sleep 3
done

until [ "$(docker inspect --format='{{.State.Health.Status}}' kafka)" = "healthy" ]; do
    echo "Cekam Kafku..."
    sleep 3
done

# SLeep od 5 sekundi da se HDFS NameNode potpuno stabilizuje
echo "Servisi su podignuti. Stabilizacija HDFS sistema (sacekajte 5s)..."
sleep 5

echo "Iskljucuje se HDFS Safe Mode..."
docker exec namenode hdfs dfsadmin -safemode leave

echo "--------------------------------------------------"
echo "Kreiranje HDFS zona (ukoliko ne postoje)..."

# Kreiranje zona unutar namenode kontejnera
docker exec namenode hdfs dfs -mkdir -p /data/raw_zone
docker exec namenode hdfs dfs -mkdir -p /data/transformed_zone

echo "HDFS zone proverene/kreirane."

# 2. Provera da li CSV vec postoji u raw_zone na HDFS-u
echo "Proverava se da li CSV fajl vec postoji u raw_zone..."
FILE_EXISTS=$(docker exec namenode hdfs dfs -test -e /data/raw_zone/hourly_data_combined_1990_to_1999.csv && echo "true" || echo "false")

if [ "$FILE_EXISTS" = "true" ]; then
    echo ">>> CSV fajl vec postoji u raw_zone — PRESKACE SE ucitavanje radi ustede vremena."
else
    echo ">>> CSV ne postoji na HDFS-u. Pokrece se kopiranje..."
    
    # Kopiramo iz work foldera u namenode kontejner u /tmp
    echo "Kopira se CSV u namenode kontejner..."
    docker cp $PROJECT_DIR/work/hourly_data_combined_1990_to_1999.csv namenode:/tmp/hourly_data_combined_1990_to_1999.csv
    
    # Prebacujemo iz kontejnera na HDFS raw_zone
    echo "Prebacuje se CSV sa kontejnera na HDFS..."
    docker exec namenode hdfs dfs -put /tmp/hourly_data_combined_1990_to_1999.csv /data/raw_zone/
    
    echo ">>> CSV uspešno učitan u raw_zone!"
fi

echo ""
echo "=================================================="
echo "  KORAK 1 ZAVRSEN USPESNO"
echo "=================================================="
