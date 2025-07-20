#!/bin/bash

# Configura qui i parametri
INPUT_FILE="5aeff182-52ab-46d3-af1d-57ac75b3a21e.csv"
LINES_PER_FILE=5000000   # Numero di righe per ogni file (esclusa intestazione)
PREFIX="part_"

# 1. Estrai intestazione
head -n 1 "$INPUT_FILE" > header.csv

# 2. Rimuovi intestazione e dividi per righe
tail -n +2 "$INPUT_FILE" | split -l $LINES_PER_FILE -d --additional-suffix=.csv - "$PREFIX"

# 3. Aggiungi l'intestazione a ogni file
for f in ${PREFIX}*.csv; do
    cat header.csv "$f" > temp && mv temp "$f"
done

# 4. Pulisci
rm header.csv

echo "Divisione completata. File generati: ${PREFIX}00.csv, ${PREFIX}01.csv, ..."
