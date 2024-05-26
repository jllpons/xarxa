#!/usr/bin/env bash

# Exit on any error
set -e

# Default path for the configuration file
config_path="config/configuration.json"

display_help() {
    echo "Usage: $0 [options]"
    echo "Automates the initialization and population of the database based on the configuration file."
    echo ""
    echo "The script encompasses the following key operations:"
    echo ""
    echo "  1. Initialize the database:"
    echo "     1.1. CREATE TABLES"
    echo "  2. Populate the database with all of the necessary data for the organism:"
    echo "     2.1. Fetch and insert all organism UniProt entries into the 'uniprot' table."
    echo "     2.2. Fetch and insert all features found on the RefSeq annotated genome for the organism."
    echo "     2.3. Fetch and insert all KEGG entries for the organism into the 'kegg' table."
    echo ""
    echo "Options:"
    echo "   --config <path>  specify the path to the configuration file. Default: $config_path"
    echo "   -h, --help       show this help message"
    echo ""
    echo "Example:"
    echo "  $0 --c config/configuration.json"
    exit 1
}

load_config() {
    # Load the configuration file
    echo ""
    echo "Loading the configuration from: $config_path"
    connection_string=$(jq -r '.database_data.connection' $config_path)
    organism_taxid=$(jq -r '.organism_data.taxid' $config_path)
    refseq_genbank_genome_file=$(jq -r '.organism_data.refseq_genbank_genome_file' $config_path)
    kegg_organism=$(jq -r '.organism_data.kegg_id' $config_path)

    echo "Connection string: $connection_string"
    echo "Organism taxid: $organism_taxid"
    echo "RefSeq GenBank Genome File: $refseq_genbank_genome_file"
    echo "KEGG Organism Code: $kegg_organism"
}

setup_logging() {

    date=$(date "+%Y-%m-%d-%H-%M-%S")
    log_dir="logs/processed_data/${date}_build_db"
    log_level="INFO"
    mkdir -p "$log_dir"
}

handle_error() {
    # Handle errors in the workflow

    echo "Error on line $1"
    exit 1
}
trap 'handle_error $LINENO' ERR

generate_tables(){

    echo ""
    echo "Step 1.1: Generate Tables"
    echo ""
    echo "TODO: Implement"
    }

fetch_uniprot_data(){

    echo ""
    echo "Step 2.1: Fetch and Insert UniProt Entries for Taxid: $organism_taxid"
    echo ""
    python src/fetch_uniprot_organism_json.py "$organism_taxid" --log "$log_level" \
    | tee "$log_dir/fetch_uniprot_organism_json.out.json" \
    | python src/process_uniprot_json_entry.py --log "$log_level" \
    | tee "$log_dir/process_uniprot_json_entry.out.tsv" \
    | python src/upsert_table.py "uniprot" --log "$log_level"
}

fetch_refseq_genome(){

    echo ""
    echo "Step 2.2: Fetch and Insert RefSeq Genome Features"
    echo ""
    python src/fetch_refseq_genbank_genome.py "$refseq_genbank_genome_file" --log "$log_level" \
    | tee "$log_dir/fetch_refseq_genome.out.tsv" \
    | python src/upsert_table.py "refseq" --log "$log_level"
}

fetch_kegg_ids(){

    echo ""
    echo "Step 2.3: Fetch and Insert KEGG Entries for Organism: $kegg_organism"
    echo ""
    python src/fetch_kegg_organism.py "$kegg_organism" --log "$log_level" \
    | tee "$log_dir/fetch_kegg_organism.out.tsv" \
    | python src/upsert_table.py "kegg" --log "$log_level"
}

# Parse command-line arguments for optional config path
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --config) config_path="$2"; shift 2 ;;
        -h|--help) display_help ;;
        *) echo "Unknown parameter passed: $1"; display_help; exit 1 ;;
    esac
done

load_config
setup_logging
echo ""
echo "Initializing database for organism with taxid: $organism_taxid"
echo "================================================================================"
echo ""
echo "Phase 1: Initialize the database"
echo "--------------------------------------------------------------------------------"
create_tables
echo ""
echo "Phase 2: Populate the database with organism data"
echo "--------------------------------------------------------------------------------"
echo ""
fetch_uniprot_data
fetch_refseq_genome
fetch_kegg_ids
echo ""
echo "Finished building the database."
echo "=========================================================================
