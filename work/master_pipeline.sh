#!/bin/bash
# ==============================================================================
# Glavna(master) skripta master_pipeline.sh pokreće kompletan Batch pipeline, a zatim automatski otvara Real-time meni
# ==============================================================================

# Ako se desi greska u batch delu, prekidamo se sve odmah
set -e

clear
echo "================================================================="
echo "   START MASTER PIPELINE-A: INTEGRISANI BATCH & REAL-TIME"
echo "================================================================="
echo " Pokrenuto: $(date)"
echo "================================================================="
echo ""

# 1. POKRETANJE BATCH PIPELINE-A
echo ">>> Pokrecem BATCH pipeline..."
echo "-----------------------------------------------------------------"
./run_batch_pipeline.sh

# Privremeno iskljucujem 'set -e' jer će real-time upiti slati Ctrl+C,
# a ne zelim da Ctrl+C sruši master skriptu
set +e

echo ""
echo "================================================================="
echo "    BATCH PIPELINE JE USPESNO ZAVRSEN!"
echo "   Svi istorijski podaci su u PostgreSQL bazi."
echo "================================================================="
echo ""
echo ">>> Priprema za prelazak na REAL-TIME (streaming) deo..."
echo ">>> Pokrece se interaktivni meni za 5 sekundi..."
sleep 5

# 2. AUTOMATSKO POKRETANJE REAL-TIME MENIJA
./run_realtime_pipeline.sh

echo ""
echo "================================================================="
echo "   MASTER PIPELINE JE ZATVOREN."
echo "   Kraj rada! Hvala na paznji! :)"
echo "================================================================="
