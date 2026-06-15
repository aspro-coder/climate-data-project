#!/bin/bash
# ==============================================================================
# Skripta 03_run_batch_queries.sh pokrece ukupno 11 batch upita jedan za drugim unutar Spark kontejnera.
# Ukoliko neki upit baci gresku, skripta se zaustavlja
# ==============================================================================

# Komanda za spark-submit unutar kontejnera sa pratecim postgres drajverom
SPARK_SUBMIT="docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /home/jovyan/work/jars/postgresql-42.7.3.jar"

# Putanja do foldera sa batch upitima unutar Spark kontejnera
BATCH_DIR="/home/jovyan/work/batch"

echo "=================================================="
echo "  KORAK 3: Pokretanje Batch upita"
echo "=================================================="

# Lista fajlova gde svaki izvrsava po jedan od 11 batch upita
QUERIES=(
    "batch_query_1_jedan.py"
    "batch_query_2.py"
    "batch_query_3.py"
    "batch_query_4.py"
    "batch_query_5.py"
    "batch_query_6.py"
    "batch_query_7.py"
    "batch_query_7_bonus.py"
    "batch_query_8.py"
    "batch_query_9.py"
    "batch_query_10.py"
)

TOTAL=${#QUERIES[@]}
CURRENT=0

echo "Ukupan broj upita za izvrsavanje: $TOTAL"
echo "--------------------------------------------------"

for QUERY in "${QUERIES[@]}"; do
    CURRENT=$((CURRENT + 1))
    
    echo ""
    echo "--------------------------------------------------"
    echo "  Pokrece se upit $CURRENT/$TOTAL: $QUERY"
    echo "--------------------------------------------------"
    
    # Izvrsavanje upita pomocu Spark-a
    $SPARK_SUBMIT $BATCH_DIR/$QUERY
    
    # Provera Exit status-a. 0 znaci da je proslo bez greške
    if [ $? -eq 0 ]; then
        echo " USPESNO: Fajl $QUERY je zavrsen."
        echo "Pauza od 4 sekunde pre sledeceg upita radi preglednosti ispisa..."
        sleep 4
    else
        echo " GRESKA: Doslo je do problema(greke) u fajlu $QUERY!"
        echo "Zaustavlja se izvrsavanje!"
        exit 1
    fi
done

echo ""
echo "=================================================="
echo "  SVI BATCH UPITI SU USPESNO IZVRSENI!"
echo "=================================================="
