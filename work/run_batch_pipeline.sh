#!/bin/bash
# ==============================================================================
# Glavna skripta za batch deo. skripta run_batch_pipeline.sh automatizuje ceo batch pipeline.
# ==============================================================================
# 'set -e' garantuje da ako bilo koji korak baci gresku, cela skripta tog momenta staje!
set -e

# Ciscenje ekrana radi urednosti i preglednosti
clear

echo "================================================================="
echo "  POKRETANJE KOMPLETNOG BATCH PIPELINE-A  "
echo "================================================================="
echo " Startovano u: $(date)"
echo "-----------------------------------------------------------------"

# 1. Pokretanje celokupne infrastrukture i priprema HDFS-a
./01_start_and_prepare.sh

echo ""
echo ">>> Prelazak na sledeci korak za 3 sekunde..."
sleep 3

# 2. Pokretanje transformacije podataka
./02_transform_data.sh

echo ""
echo ">>> Prelazak na sledeci korak za 3 sekunde..."
sleep 3

# 3. Pokretanje svih batch upita
./03_run_batch_queries.sh

echo ""
echo "================================================================="
echo "  STATUS: USPESNO ZAVRSENO!"
echo "  Svi batch upiti su izvrseni uspesno, podaci su u bazi. Sve je spremno za Metabase vizualizacije."
echo "  Zavrseno u: $(date)"
echo "================================================================="
