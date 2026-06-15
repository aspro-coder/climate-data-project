#!/bin/bash
# ==============================================================================
# Interaktivna skripta run_realtime_pipeline.sh za upravljanje sa svih 5 real-time upita
# ==============================================================================

# Definicija paketa za Spark i putanje
SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0"
STREAM_DIR="/home/jovyan/work/streaming_consumer"

# Instalacija zavisnosti (requests) na spark-master i spark-worker
install_dependencies() {
    echo "Instalira se 'requests' biblioteka na Spark klaster..."
    docker exec spark-master pip3 install requests >/dev/null 2>&1 || true
    docker exec spark-worker1 pip3 install requests >/dev/null 2>&1 || true
    echo " Biblioteke su spremne."
}

# Funkcija za ciscenje checkpoint-a i Elasticsearch indeksa na osnovu izbora
clean_query_state() {
    local q_num=$1
    echo "--------------------------------------------------"
    echo "Ciscenje stanja za Upit $q_num..."
    
    case $q_num in
        1)
            echo "Brise se HDFS checkpoint za Upit 1..."
            docker exec namenode hdfs dfs -rm -r -f /checkpoints/q1_abu_dhabi_temp >/dev/null 2>&1 || true
            echo "Cisti se Elasticsearch indeks: realtime_abu_dhabi_temp..."
            curl -s -X POST "http://localhost:9200/realtime_abu_dhabi_temp/_delete_by_query" \
                 -H "Content-Type: application/json" \
                 -d '{"query":{"match_all":{}}}' >/dev/null || true
            ;;
        2)
            echo "Brise se HDFS checkpoint za Upit 2..."
            docker exec namenode hdfs dfs -rm -r -f /checkpoints/q2_bangkok_flood >/dev/null 2>&1 || true
            echo "Cisti se Elasticsearch indeks: realtime_bangkok_flood_risk..."
            curl -s -X POST "http://localhost:9200/realtime_bangkok_flood_risk/_delete_by_query" \
                 -H "Content-Type: application/json" \
                 -d '{"query":{"match_all":{}}}' >/dev/null || true
            ;;
        3)
            echo "Brise se checkpoint za Upit 3..."
            docker exec spark-master rm -rf /tmp/checkpoint/q3_dubai_heat >/dev/null 2>&1 || true
            echo "Cisti se Elasticsearch indeks: realtime_dubai_heat_risk..."
            curl -s -X POST "http://localhost:9200/realtime_dubai_heat_risk/_delete_by_query" \
                 -H "Content-Type: application/json" \
                 -d '{"query":{"match_all":{}}}' >/dev/null || true
            ;;
        4)
            echo "Brise se lokalni checkpoint za Upit 4..."
            docker exec spark-master rm -rf /tmp/checkpoint/q4_chicago_wind >/dev/null 2>&1 || true
            echo "Cisti se Elasticsearch indeks: realtime_chicago_wind..."
            curl -s -X POST "http://localhost:9200/realtime_chicago_wind/_delete_by_query" \
                 -H "Content-Type: application/json" \
                 -d '{"query":{"match_all":{}}}' >/dev/null || true
            ;;
        5)
            echo "Brise se HDFS checkpoint za Upit 5..."
            docker exec namenode hdfs dfs -rm -r -f /checkpoints/q5_beijing_pressure >/dev/null 2>&1 || true
            echo "Cisti se Elasticsearch indeks: realtime_beijing_pressure..."
            curl -s -X POST "http://localhost:9200/realtime_beijing_pressure/_delete_by_query" \
                 -H "Content-Type: application/json" \
                 -d '{"query":{"match_all":{}}}' >/dev/null || true
            ;;
    esac
    
    echo " Sistem je ociscen i spreman za start!"
    echo "--------------------------------------------------"
}

# Inicijalna instalacija pri pokretanju skripte
clear
echo "=================================================="
echo "  PRIPREMA REAL-TIME (STREAMING) PIPELINE-A"
echo "=================================================="
install_dependencies

# Glavna petlja menija
while true; do
    echo ""
    echo "=================================================="
    echo "       IZABERITE REAL-TIME UPIT ZA POKRETANJE      "
    echo "=================================================="
    echo "1) Pokreni Job 1 (Abu Dhabi Temp vs Hist)"
    echo "2) Pokreni Job 2 (Bangkok Flood Risk)"
    echo "3) Pokreni Job 3 (Dubai Heat Risk)"
    echo "4) Pokreni Job 4 (Chicago Wind)"
    echo "5) Pokreni Job 5 (Beijing Pressure)"
    echo "--------------------------------------------------"
    echo "q) Izlaz iz skripte"
    echo "=================================================="
    read -p "Unesite opciju [1-5 ili q]: " opt

    case $opt in
        1)
            clean_query_state 1
            echo "Pokrece se JOB 1 u real-time rezimu..."
            echo "Pritisnite Ctrl+C za zaustavljanje upita i povratak u meni."
            echo "--------------------------------------------------"
            docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages $SPARK_PACKAGES $STREAM_DIR/job1_elastic_search.py
            ;;
        2)
            clean_query_state 2
            echo "Pokrece se JOB 2 u real-time rezimu..."
            echo "Pritisnite Ctrl+C za zaustavljanje upita i povratak u meni."
            echo "--------------------------------------------------"
            docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages $SPARK_PACKAGES $STREAM_DIR/job2_elastic_search.py
            ;;
        3)
            clean_query_state 3
            echo "Pokrece se JOB 3 u real-time rezimu..."
            echo "Pritisnite Ctrl+C za zaustavljanje upita i povratak u meni."
            echo "--------------------------------------------------"
            docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages $SPARK_PACKAGES $STREAM_DIR/job3_elastic_search.py
            ;;
        4)
            clean_query_state 4
            echo "Pokrece se JOB 4 u real-time rezimu..."
            echo "Pritisnite Ctrl+C za zaustavljanje upita i povratak u meni."
            echo "--------------------------------------------------"
            docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages $SPARK_PACKAGES $STREAM_DIR/job4_elastic_search.py
            ;;
        5)
            clean_query_state 5
            echo "Pokrece se JOB 5 u real-time rezimu..."
            echo "Pritisnite Ctrl+C za zaustavljanje upita i povratak u meni."
            echo "--------------------------------------------------"
            docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages $SPARK_PACKAGES $STREAM_DIR/job5_elastic_search.py
            ;;
        q|Q)
            echo "Izlazak iz streaming menija."
            break
            ;;
        *)
            echo "Nepostojeca opcija. Molim Vas izaberite ponovo."
            sleep 2
            clear
            ;;
    esac
done
