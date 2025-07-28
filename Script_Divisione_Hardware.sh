#!/bin/bash

# Script per dividere CSV per HWID - VERSIONE CORRETTA
# Configurazione
INPUT_FILE="5aeff182-52ab-46d3-af1d-57ac75b3a21e.csv"
OUTPUT_DIR="split_hwid"

# Array degli HWID da cercare
HWIDS=("SW-065" "SW-088" "SW-106" "SW-115")

echo "🚀 Inizio divisione del file $INPUT_FILE per HWID..."

# Crea directory di output se non esiste
mkdir -p "$OUTPUT_DIR"

# Estrai l'intestazione
echo "📋 Estrazione intestazione..."
head -n 1 "$INPUT_FILE" > "${OUTPUT_DIR}/header.csv"

# Per ogni HWID, crea il file con intestazione + righe filtrate
for hwid in "${HWIDS[@]}"; do
    output_file="${OUTPUT_DIR}/${hwid}.csv"
    echo "🔍 Processando $hwid..."
    
    # Copia l'intestazione
    cp "${OUTPUT_DIR}/header.csv" "$output_file"
    
    # Cerca e appendi le righe che contengono l'HWID specifico
    # Usando grep per cercare l'HWID nella riga e appendere al file
    tail -n +2 "$INPUT_FILE" | grep "$hwid" >> "$output_file"
    
    echo "✅ Completato $hwid"
done

# Pulizia file temporaneo
rm "${OUTPUT_DIR}/header.csv"

# Statistiche finali
echo ""
echo "✅ Divisione completata!"
echo "📊 Statistiche dei file generati:"
echo "----------------------------------------"

total_original=$(wc -l < "$INPUT_FILE")
echo "File originale: $total_original righe"
echo ""

total_split=0
for hwid in "${HWIDS[@]}"; do
    output_file="${OUTPUT_DIR}/${hwid}.csv"
    if [ -f "$output_file" ]; then
        lines=$(wc -l < "$output_file")
        data_lines=$((lines - 1))  # Escludi intestazione
        size=$(du -h "$output_file" | cut -f1)
        echo "📄 $hwid.csv: $data_lines righe dati + intestazione ($size)"
        total_split=$((total_split + data_lines))
    else
        echo "⚠️  $hwid.csv: File non trovato"
    fi
done

echo "----------------------------------------"
echo "Totale righe dati processate: $total_split"
echo "File salvati in: $OUTPUT_DIR/"

# Verifica se abbiamo processato tutte le righe
expected_data_lines=$((total_original - 1))
if [ $total_split -eq $expected_data_lines ]; then
    echo "✅ Verifica OK: tutte le righe sono state processate"
elif [ $total_split -lt $expected_data_lines ]; then
    echo "ℹ️  $((expected_data_lines - total_split)) righe non matchano nessun HWID (potrebbero avere HWID diversi)"
else
    echo "⚠️  Attenzione: ci sono più righe del previsto (possibili duplicati)"
fi

echo ""
echo "🔍 Test rapido dei file generati:"
for hwid in "${HWIDS[@]}"; do
    output_file="${OUTPUT_DIR}/${hwid}.csv"
    if [ -f "$output_file" ] && [ $(wc -l < "$output_file") -gt 1 ]; then
        echo "📄 Prime 2 righe di $hwid.csv:"
        head -n 2 "$output_file"
        echo ""
    fi
done
