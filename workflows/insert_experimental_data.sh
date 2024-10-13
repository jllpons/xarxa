#!/usr/bin/env bash

# Exit on any error
set -e

# Default path for the configuration file
config_path="config/configuration.json"
# Defalut path for the experimental file
experimental_path="config/experimental.json"

# Function to display help message
display_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "This script automates the integration of experimental datasets into the database."
    echo "Based on the data found in the configuration and experimental files. Encompasses the folloing steps:"
    echo ""
    echo "Step 1. Generate the 'experimental_condition' table based on the defined 'experimental_conditions' parameter."
    echo ""
    echo "Step 2. Integrate the transcriptomics data. For each transcriptomics experiment:"
    echo ""
    echo "  - Insert/update the 'transcriptomics' table with the corresponding 'condition_a' and 'condition_b'."
    echo "  - Insert/update the 'transcriptomics_counts' table for each replicate of both conditions."
    echo ""
    echo "Options:"
    echo "   --config <path>  specify the path to the configuration file. Default: $config_path"
    echo "   -h, --help       show this help message"
    echo ""
    echo "Example:"
    echo "  $0 --config config/configuration.json"
    echo ""
    exit 0
}

load_config() {
    # Load the configuration file
    echo ""
    echo "Loading the configuration from: $config_path"
    connection_string=$(jq -r '.database_data.connection' $config_path)

    echo "Connection string: $connection_string"
}

setup_logging() {

    date=$(date "+%Y-%m-%d-%H-%M-%S")
    log_dir="logs/processed_data/${date}_integrate_experimental_data"
    log_level="INFO"
    mkdir -p "$log_dir"
}

handle_error() {
    # Handle errors in the workflow

    echo "Error on line $1"
    exit 1
}
trap 'handle_error $LINENO' ERR


generate_experiment_conditions() {

    echo "Generating the 'experimental_condition' table based on the defined 'experimental_conditions' parameter."
    echo "--------------------------------------------------------------------------------"
    echo ""

    jq -c ".experimental_conditions[]" $experimental_path | while read condition; do

        condition_name=$(echo $condition | jq -r '.name')
        condition_description=$(echo $condition | jq -r '.description')
        condition_type=$(echo $condition | jq -r '.type')
        tab=$'\t'

        content=$(cat <<EOF
$condition_name$tab$condition_description$tab$condition_type
EOF
)
        echo "$content" \
        | tee -a $log_dir/experimental_condition.out.tsv \
        | python src/upsert_table.py experimental_condition --log $log_level

    done
}


integrate_transcriptomics() {
    jq -c ".transcriptomics[]" $experimental_path | while read experiment; do

        file_path=$(echo $experiment | jq -r '.file')
        condition_a=$(echo $experiment | jq -r '.condition_a')
        condition_b=$(echo $experiment | jq -r '.condition_b')
        condition_a_counts=($(echo $experiment | jq -r '.condition_a_counts[]'))
        condition_b_counts=($(echo $experiment | jq -r '.condition_b_counts[]'))

        echo ""
        echo "Integrating Transcriptomics Data for:"
        echo " - File path: $file_path"
        echo " - Condition A: $condition_a"
        echo " - Condition B: $condition_b"
        echo " - Condition A Counts: $condition_a_counts"
        echo " - Condition B Counts: $condition_b_counts"
        echo "--------------------------------------------------------------------------------"
        echo ""

        python src/upsert_table.py transcriptomics $file_path $condition_a $condition_b --log $log_level

        echo ""
        echo "Inserting counts for condition A"
        echo ""

        # iterate over files in condition_a_counts and use index to specify the replicate number
        for index in $(seq 0 $((${#condition_a_counts[@]} - 1))); do
            replicate=$(($index + 1))
            echo " - replicate: $replicate"
            echo " - file: ${condition_a_counts[$index]}"

            python src/upsert_table.py transcriptomics_counts ${condition_a_counts[$index]} $condition_a $replicate --log $log_level
        done

        echo ""
        echo "Inserting counts for condition B"
        echo ""

        for index in $(seq 0 $((${#condition_b_counts[@]} - 1))); do
            replicate=$(($index + 1))
            echo "replicate: $replicate"
            echo "file: ${condition_b_counts[$index]}"

        done


    done
}

integrate_proteomics() {
    jq -c ".proteomics[]" $experimental_path | while read experiment; do

        file_path=$(echo $experiment | jq -r '.file')
        condition_a=$(echo $experiment | jq -r '.condition_a')
        condition_b=$(echo $experiment | jq -r '.condition_b')
        condition_a_counts=($(echo $experiment | jq -r '.condition_a_intensities[]'))
        condition_b_counts=($(echo $experiment | jq -r '.condition_b_intensities[]'))

        echo ""
        echo "Integrating Proteomics Data for:"
        echo " - File path: $file_path"
        echo " - Condition A: $condition_a"
        echo " - Condition B: $condition_b"
        echo " - Condition A Intensities: $condition_a_counts"
        echo " - Condition B Intensities: $condition_b_counts"
        echo "--------------------------------------------------------------------------------"
        echo ""

        cat $file_path | tail -n +2 | python src/upsert_table.py proteomics "-" $condition_a $condition_b --log $log_level
        echo "Proteomics data integrated successfully"

        echo ""
        echo "Inserting intensities for condition A"
        echo ""

        for index in $(seq 0 $((${#condition_a_counts[@]} - 1))); do
            replicate=$(($index + 1))
            echo " - replicate: $replicate"
            echo " - file: ${condition_a_counts[$index]}"

            cat ${condition_a_counts[$index]} | tail -n +2 | python src/upsert_table.py proteomics_replicates "-" $condition_a $replicate --log $log_level
        done

        echo ""
        echo "Inserting intensities for condition B"
        echo ""

        for index in $(seq 0 $((${#condition_b_counts[@]} - 1))); do
            replicate=$(($index + 1))
            echo " - replicate: $replicate"
            echo " - file: ${condition_b_counts[$index]}"

            cat ${condition_b_counts[$index]} | tail -n +2 | python src/upsert_table.py proteomics_replicates "-" $condition_b $replicate --log $log_level
        done

    done
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
echo "Step 1. Generate the 'experimental_condition' table based on the defined 'experimental_conditions' parameter."
echo "================================================================================"
echo ""
generate_experiment_conditions
echo ""
echo "Step 2. Integrate the transcriptomics data. For each transcriptomics experiment:"
echo ""
integrate_transcriptomics
echo ""
echo "Step 3. Integrate the proteomics data. For each proteomics experiment:"
echo ""
integrate_proteomics
echo ""
echo "Finished Integrating Experimental Data"
echo "================================================================================"


