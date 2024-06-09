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
    echo "  1. Fetch and process required data:"
    echo "     1.1. Download and process all of the UniProtKB entries associated with a given taxID"
    echo "     1.2. Parse and extract all of the features found on the RefSeq annotated genome for the organism."
    echo "     1.3. Download all of the KEGG entries for the organism."
    echo "  2. Generate the ID Mapper table that will be used to link the different identifiers:"
    echo "     2.1. Build the ID Mapper table that will be used to link the different identifiers."
    echo "     2.2. Insert the ID Mapper table into the database."
    echo "  3. Populate the database with the organism data:"
    echo "     3.1. Insert all organism UniProt entries into the 'uniprot' table."
    echo "     3.2. Insert all organism RefSeq genome features into the 'refseq' table."
    echo "     3.3. Insert all organism KEGG entries into the 'kegg' table."
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


mk_data_dir(){
    # Create the data directory if it does not exist

    uniprotkb_dir=$"data/uniprotkb"
    refseq_dir=$"data/refseq"
    kegg_dir=$"data/kegg"
    id_mapper_dir=$"data/id_mapper"
    mkdir -p data
    mkdir -p "$uniprotkb_dir"
    mkdir -p "$refseq_dir"
    mkdir -p "$kegg_dir"
    mkdir -p "$id_mapper_dir"
}


fetch_uniprot_entries(){

    echo ""
    echo "Step 1.1: Download UniProt Entries for Taxid: $organism_taxid"
    echo ""


    python src/fetch_uniprot_organism_json.py "$organism_taxid" --log "$log_level" \
    | tee "$log_dir/fetch_uniprot_organism_json.out.json"  > "$uniprotkb_dir/uniprot_entries.json"
}


process_uniprot_entries(){

    echo ""
    echo "Step 1.2: Process UniProt Entries for Taxid: $organism_taxid"
    echo ""

    python src/process_uniprot_json_entry.py --log "$log_level" < "$uniprotkb_dir/uniprot_entries.json" \
    | tee "$log_dir/process_uniprot_json_entry.out.tsv" > "$uniprotkb_dir/uniprot_entries.tsv"
}


fetch_refseq(){

    echo ""
    echo "Step 1.3: Fetch RefSeq Genome Features"
    echo ""

    python src/fetch_refseq_genome.py "$refseq_genbank_genome_file" --log "$log_level" \
    | tee "$log_dir/fetch_refseq_genome.out.tsv" > "$refseq_dir/refseq_genome.tsv"
}


fetch_kegg(){

    echo ""
    echo "Step 1.4: Fetch KEGG Entries for Organism: $kegg_organism"
    echo ""

    python src/fetch_kegg_organism.py "$kegg_organism" --log "$log_level" \
    | tee "$log_dir/fetch_kegg_organism.out.tsv" > "$kegg_dir/kegg_organism.tsv"
}

mk_id_mapper_table(){

    echo ""
    echo "Step 2.1: Build ID Mapper Table"
    echo ""

    cut -f1-5 "$uniprotkb_dir/uniprot_entries.tsv" \
    | tee "$log_dir/uniprot_entries_cut.out.tsv" > "$id_mapper_dir/uniprot_entries_cut.tsv"

    cut -f1-3 "$refseq_dir/refseq_genome.tsv" \
    | tee "$log_dir/refseq_genome_cut.out.tsv" > "$id_mapper_dir/refseq_genome_cut.tsv"

    cut -f1 "$kegg_dir/kegg_organism.tsv" \
    | tee "$log_dir/kegg_organism_cut.out.tsv" > "$id_mapper_dir/kegg_organism_cut.tsv"


    python src/match_ids.py --log "$log_level" \
        $id_mapper_dir/uniprot_entries_cut.tsv \
        $id_mapper_dir/refseq_genome_cut.tsv \
        $id_mapper_dir/kegg_organism_cut.tsv \
    | tee "$log_dir/mk_id_mapper_dir_table.out.tsv" > "$id_mapper_dir/id_mapper.tsv"
}


insert_id_mapper_table(){

    echo ""
    echo "Step 2.2: Upsert ID Mapper Table"
    echo ""

    python src/upsert_table.py "id_mapper" "$id_mapper_dir/id_mapper.tsv"
}


insert_uniprot_table(){

    echo ""
    echo "Step 3.1: Insert UniProt Entries"
    echo ""

    python src/upsert_table.py --log "$log_level" \
        "uniprot" \
        "$uniprotkb_dir/uniprot_entries.tsv"
}


insert_refseq_table(){

    echo ""
    echo "Step 3.2: Insert RefSeq Genome Features"
    echo ""

    python src/upsert_table.py --log "$log_level" \
        "refseq" \
        "$refseq_dir/refseq_genome.tsv"
}


insert_kegg_table(){

    echo ""
    echo "Step 3.3: Insert KEGG Entries"
    echo ""

    python src/upsert_table.py --log "$log_level" \
        "kegg" \
        "$kegg_dir/kegg_organism.tsv"
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
echo "Phase 1: Download and process required data"
echo "--------------------------------------------------------------------------------"
echo ""
mk_data_dir
fetch_uniprot_entries
process_uniprot_entries
fetch_refseq
fetch_kegg
echo ""
echo "Phase 2: Generate the ID Mapper table"
echo "--------------------------------------------------------------------------------"
echo ""
mk_id_mapper_table
insert_id_mapper_table
echo ""
echo "Phase 3: Populate the database with the organism data"
echo "--------------------------------------------------------------------------------"
echo ""
insert_uniprot_table
insert_refseq_table
insert_kegg_table
echo ""
echo "Finished building the database."
echo "========================================================================="
