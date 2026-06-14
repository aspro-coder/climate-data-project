#!/bin/bash
# ==============================================================================
# Skripta 02_transform_data.sh pokrece Spark job za transformaciju CSV -> Parquet u transformed_zone.
# Ako transformisani podaci vec postoje, preskace se korak radi ustede vremena.
# ==============================================================================

echo "=================================================="
echo "  KORAK 2: Transformacija podataka (CSV -> Parquet)"
echo "=================================================="

echo "Provera da li transformisani Parquet podaci vec postoje u transformed_zone..."

# Proverava se postojanje _SUCCESS fajla koji garantuje da je prethodni Spark job uspesno zavrsen
TRANSFORMED_EXISTS=$(docker exec namenode hdfs dfs -test -e /data/transformed_zone/transformed_weather_data/_SUCCESS && echo "true" || echo "false")

if [ "$TRANSFORMED_EXISTS" = "true" ]; then
    echo ">>> Transformisani Parquet podaci vec postoje — preskace se transformacija radi ustede vremena."
else
    echo ">>> Transformisani podaci ne postoje. Pokrecem se Spark job..."
    echo "Napomena: Ovo moze potrajati nekoliko minuta..."
    
    # Izvrsavamo Spark submit unutar spark-master kontejnera
    docker exec spark-master /spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      /home/jovyan/work/transform_raw_to_transformed.py
    
    # Provera statusa izvrsavanja Spark job-a
    if [ $? -eq 0 ]; then
        echo ">>> Transformacija podataka je uspesno zavrsena!"
    else
        echo ">>> GRESKA: Spark job za transformaciju nije uspeo!"
        exit 1
    fi
fi

echo ""
echo "=================================================="
echo "  KORAK 2 ZAVRSEN"
echo "=================================================="
