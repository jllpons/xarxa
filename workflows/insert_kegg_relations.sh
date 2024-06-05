#!/usr/bin/env bash

# Exit on any error
set -e

# Default path for the configuration file
config_path="config/configuration.json"

load_config() {
    # Load the configuration file
    echo ""
    echo "Loading the configuration from: $config_path"
    kegg_organism=$(jq -r '.organism_data.kegg_id' $config_path)
    echo ""

    kegg_organism=$(jq -r '.organism_data.kegg_id' $config_path)
    echo "KEGG Organism Code: $kegg_organism"
}

setup_logging() {

    date=$(date "+%Y-%m-%d-%H-%M-%S")
    log_dir="logs/processed_data/${date}_insert_kegg_relations"
    log_level="INFO"
    mkdir -p "$log_dir"
}

insert_kegg_relations() {
    # Insert the KEGG relations into the database
    echo ""
    echo "Inserting KEGG relations into the database"
    echo "==========================================="

    python src/fetch_kegg_relations.py $kegg_organism --log $log_level \
    | tee "$log_dir/insert_kegg_relations.out.tsv" \
    | python src/upsert_table.py kegg_relations --log $log_level
}


echo "Inserting KEGG relations for the organism: $kegg_organism"
load_config
setup_logging
insert_kegg_relations

