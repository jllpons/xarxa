# Xarxa Database Overview

## Introduction

By integrating different data types, such as gene, transcript, and protein information along with their interactions and relationships, Xarxa aims to provide a platform for the study of regulatory networks within an organism of interest.

## Table of Contents

- [Introduction](#introduction)
- [Database Design](#database-design)
- [Organism-Related Tables](#organism-related-tables)
- [Relationship Tables](#protein-level-tables)
- [Experimental Tables](#experimental-tables)
- [Integration and Interactivity](#integration-and-interactivity)
- [Table Descriptions](#table-descriptions)
    - [id_mapper Table](#id_mapper-table)
    - [uniprot Table](#uniprot-table)
    - [uniprot_keyword Table](#uniprot_keyword-table)
    - [uniprot_go_term Table](#uniprot_go_term-table)
    - [uniprot_ec_number Table](#uniprot_ec_number-table)
    - [refseq Table](#refseq-table)
    - [kegg Table](#kegg-table)
    - [kegg_pathway Table](#kegg_pathway-table)
    - [kegg_ko Table](#kegg_ko-table)
    - [kegg_relations Table](#kegg_relations-table)
    - [string_interactions Table](#string_interactions-table)
    - [experimental_condition Table](#experimental_condition-table)
    - [transcriptomics Table](#transcriptomics-table)
    - [transcriptomics_counts Table](#transcriptomics_counts-table)

## Database Design

The Xarxa database is designed to store and integrate a wide variety of biological data related to genes, proteins, and their interactions. The database schema includes tables that store information about different biological products and their relationships, enabling comprehensive analyses of regulatory networks.

## Organism-Related Tables

These tables store information specific to the organism being studied, including data from various biological databases like UniProt, RefSeq, and KEGG.

## Relationship Tables

These tables capture the interactions and relationships between different biological entities, such as protein-protein interactions, gene-protein relationships, and pathway information from KEGG and STRING databases.

## Experimental Tables

These tables store data generated from various experiments, including transcriptomics and proteomics, detailing the conditions under which the experiments were performed and the results obtained.

## Integration and Interactivity

Integration of data across different sources and types is achieved through mapping tables and relational constraints, allowing for interactive queries and comprehensive analyses of biological networks.


## Table Descriptions

### `id_mapper` Table

#### Purpose

The `id_mapper` table is used to map the different identifiers used in the different tables.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
|  `uniprot_accession` | `VARCHAR(20)` | The UniProtKB accession number |
|  `refseq_locus_tag` | `VARCHAR(20)` | The RefSeq locus tag |
| `locus_tag` | `VARCHAR(20)` | The locus tag |
|  `kegg_accession` | `VARCHAR(20)` | The KEGG accession number |
| `refseq_protein_id` | `VARCHAR(20)` | The RefSeq protein ID |

#### Constraints

- **Unique constraint**: `uniprot_accession`, `refseq_locus_tag`, `locus_tag`, `kegg_accession`, and `refseq_protein_id` must be unique.

### `uniprot` Table

#### Purpose

The `uniprot` table stores information from all known proteins in the UniProtKB database
for a given organism.

| Column Name | Data Type | Description |
|-------------|-----------|-------------
| `uniprot_accession` | `VARCHAR(20)` | The UniProtKB accession number assigned to the protein |
| `locus_tag` | `VARCHAR(20)[]` | List of the locus tag(s) of the gene(s) codifying for the specific protein |
| `orf_names` | `VARCHAR(20)[]` | Names that are temporarily attributed to an open reading frame (ORF) by a sequencing project |
| `kegg_accession` | `VARCHAR(20)[]` | List of the KEGG accession numbers referring the specific protein |
| `refseq_protein_id` | `VARCHAR(20)` | The RefSeq protein ID |
| `embl_protein_id` | `VARCHAR(20)` | The EMBL protein ID |
| `protein_name` | `VARCHAR(255)` | Full name of the protein as recommended by the UniProtKB |
| `protein_existence` | `VARCHAR(255)` | The evidence for the existence of the protein |
| `sequence` | `TEXT` | The amino acid sequence of the protein |

#### Constraints

- **Primary Key**: `uniprot_accession`, the UniProt accession number for the protein.

### `uniprot_keyword` Table

#### Purpose

The `uniprot_keyword` table stores the keywords associated with the proteins in the `uniprot` table.
Keywords are used to describe the activity, function, biological role or any other feature of the protein
using a controlled vocabulary.

| Column Name | Data Type | Description |
|-------------|-----------|-------------
| `uniprot_accession` | `VARCHAR(20)` | The UniProtKB accession number assigned to the protein |
| `keyword` | `VARCHAR(20)` | The keyword associated with the protein |

#### Constraints

- **Primary Key**: `uniprot_accession` and `keyword` as composite primary key.
- **Foreign Key**: `uniprot_accession` references the `uniprot_accession` column in the `uniprot` table.
- **Indexes**: `uniprot_accession` is indexed for faster retrieval of the keywords associated with a protein.

### `uniprot_go_term` Table

#### Purpose

The `uniprot_go_term` table stores the Gene Ontology (GO) terms associated with the proteins in the `uniprot` table.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `uniprot_accession` | `VARCHAR(20)` | The UniProtKB accession number assigned to the protein |
| `go_term` | `VARCHAR(20)` | The GO term associated with the protein |

#### Constraints

- **Primary Key**: `uniprot_accession` and `go_term` as composite primary key.
- **Foreign Key**: `uniprot_accession` references the `uniprot_accession` column in the `uniprot` table.
- **Indexes**: `uniprot_accession` is indexed for faster retrieval of the GO terms associated with a protein.

### `uniprot_ec_number` Table

#### Purpose

The `uniprot_ec_number` table stores the Enzyme Commission (EC) numbers associated with the proteins in the `uniprot` table.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `uniprot_accession` | `VARCHAR(20)` | The UniProtKB accession number assigned to the protein |
| `ec_number` | `VARCHAR(20)` | The EC number associated with the protein |

#### Constraints

- **Primary Key**: `uniprot_accession` and `ec_number` as composite primary key.
- **Foreign Key**: `uniprot_accession` references the `uniprot_accession` column in the `uniprot` table.
- **Indexes**: `uniprot_accession` is indexed for faster retrieval of the EC numbers associated with a protein.

### `uniprot_ptm` Table

#### Purpose

The `uniprot_ptm` table stores the post-translational modifications (PTMs) associated with the proteins in the `uniprot` table.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `uniprot_accession` | `VARCHAR(20)` | The UniProtKB accession number assigned to the protein |
| `ptm_start` | `INTEGER` | The start position of the PTM in the protein sequence |
| `ptm_end` | `INTEGER` | The end position of the PTM in the protein sequence |
| `ptm_description` | `VARCHAR(255)` | The description of the PTM |

#### Constraints

- **Primary Key**: `uniprot_accession`, `ptm_start`, and `ptm_end` as composite primary key.
- **Foreign Key**: `uniprot_accession` references the `uniprot_accession` column in the `uniprot` table.
- **Indexes**: `uniprot_accession` is indexed for faster retrieval of the PTMs associated with a protein.

### `refseq` Table

#### Purpose

Stores the main identifiers and relevant data extracted from the RefSeq annotation of the organism whole genome.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `refseq_locus_tag` | `VARCHAR(20)` | The RefSeq locus tag |
| `locus_tag` | `VARCHAR(20)[]` | List of the locus tag(s) referring to same gene |
| `refseq_accession` | `VARCHAR(20)[]` | List of RefSeq accession numbers referring to the same gene |
| `strand_location` | `VARCHAR(20)` | The strand location of the gene |
| `start_position` | `INT` | The start position of the gene |
| `end_position` | `INT` | The end position of the gene |
| `translated_protein_sequence` | `TEXT` | The translated protein sequence of the gene |

#### Constraints

- **Primary Key**: `refseq_locus_tag`, the RefSeq locus tag for the gene.

### `kegg` Table

#### Purpose

Stores the main identifiers extracted from the KEGG database for the organism.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `kegg_accession` | `VARCHAR(20)` | The KEGG accession number |

#### Constraints

- **Primary Key**: `kegg_accession`, the KEGG accession number for the gene.

### `kegg_pathway` Table

#### Purpose

Maps KEGG accession to the KEGG pathway they belong to.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `kegg_accession` | `VARCHAR(20)` | The KEGG accession |
|`kegg_pathway` | `VARCHAR(20)` | The KEGG pathway the accession belongs to |

#### Constraints

- **Primary Key**: `kegg_accession`, the KEGG accession number for the gene.
- **Indexes**: `kegg_accession` is indexed for faster retrieval of the KEGG pathway associated with the accession.

### `kegg_ko` Table

#### Purpose

Stores the KEGG Orthology (KO) identifiers associated with the KEGG accession numbers.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `kegg_accession` | `VARCHAR(20)` | The KEGG accession number |
| `ko_id` | `VARCHAR(20)` | The KEGG Orthology (KO) identifier |

#### Constraints

- **Primary Key**: `kegg_accession` and `ko_id` as composite primary key.
- **Foreign Key**: `kegg_accession` references the `kegg_accession` column in the `kegg` table.

### `kegg_relations` Table

#### Purpose

Stores the relationships between KEGG entries, found in the KGML files of all
the KEGG pathways for the organism.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `source` | `VARCHAR(20)` | The source KEGG accession number |
| `target` | `VARCHAR(20)` | The target KEGG accession number |
| `kegg_pathway` | `VARCHAR(20)` | The KEGG pathway where the relationship is found |
| `type` | `VARCHAR(20)` | The type of relationship between the source and target KEGG entries |
| `subtype` | `VARCHAR(20)` | The subtype of relationship between the source and target KEGG entries |
| `subtype_name` | `VARCHAR(20)` | If the subtype is a compound, the name of the compound |

#### Constraints

- **Primary Key**: `source`, `target`, `kegg_pathway`, `type`, `subtype`, and `subtype_name` as composite primary key, since two relations may only differ in the subtype name or any other column.
- **Indexes**: `source` and `target` are indexed for faster retrieval of the relationships between KEGG entries.

### `string_interactions` Table

#### Purpose

Contains the interactions retrieved from the STRING database when querying the whole proteome of the organism.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `protein_a` | `VARCHAR(20)` | The RefSeq Locus Tag of the first protein |
| `protein_b` | `VARCHAR(20)` | The RefSeq Locus Tag of the second protein |
| `neighborhood` | `INTERGER` | The neighborhood score |
| `neighborhood_transferred` | `INTERGER` | The neighborhood transferred score |
| `fusion` | `INTERGER` | The gene fusion events score |
| `phylogenetic_cooccurrence` | `INTERGER` | The phylogenetic co-occurrence score |
| `homology` | `INTERGER` | The homology score |
| `coexpression` | `INTERGER` | The co-expression score |
| `coexpression_transferred` | `INTERGER` | The co-expression transferred score |
| `experimental` | `INTERGER` | The experimental score |
| `experimental_transferred` | `INTERGER` | The experimental transferred score |
| `database` | `INTERGER` | The database score |
| `database_transferred` | `INTERGER` | The database transferred score |
| `textmining` | `INTERGER` | The textmining score |
| `textmining_transferred` | `INTERGER` | The textmining transferred score |
| `combined_score` | `INTERGER` | The combined score |

#### Constraints

- **Primary Key**: `protein_a` and `protein_b` as composite primary key.
- **Indexes**: `protein_a` and `protein_b` are indexed for faster retrieval of the interactions between proteins.

### `experimental_condition` Table

#### Purpose

Stores the experimental condition used in the different experiments.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `condition_name` | `VARCHAR(20)` | The name of the experimental condition |
| `description` | `TEXT` | The description of the experimental condition |
| `type` | `VARCHAR(20)` | The type of the experimental condition |

#### Constraints

- **Primary Key**: `name`, the name of the experimental condition.
- **Constraints**: `type` must be one of the following values: `transcriptomics`, `proteomics`, `phosphoproteomics`.

### `transcriptomics` Table

#### Purpose

Stores the main identifiers and relevant data extracted from the transcriptomics experiments. 

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `experimental_id` | `VARCHAR(20)` | The ID used to identify the gene in the experiment |
| `condition_a` | `VARCHAR(255)` | The name of the first experimental condition |
| `condition_b` | `VARCHAR(255)` | The name of the second experimental condition |
| `log2_fold_change` | `DOUBLE PRECISION` | The log2 fold change of the gene |
| `p_value` | `DOUBLE PRECISION` | The p-value of the gene |
| `adjusted_p_value` | `DOUBLE PRECISION` | The adjusted p-value of the gene |

#### Constraints

- **Primary Key**: `experimental_id`, `condition_a`, and `condition_b` as composite primary key.
- **References**: `condition_a` and `condition_b` reference the `name` column in the `experimental_condition` table.

### `transcriptomics_counts` Table

#### Purpose

Stores the mapped read counts of the genes in the different transcriptomics experiments.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `experimental_id` | `VARCHAR(20)` | The ID used to identify the gene in the experiment |
| `condition_name` | `VARCHAR(255)` | The name of the first experimental condition |
| `replicate` | `INTEGER` | The replicate number of the condition |
| `read_count` | `DOUBLE PRECISION` | The read count of the gene |
| `normalized_read_count` | `DOUBLE PRECISION` | The normalized read count of the gene |

#### Constraints

- **Primary Key**: `experimental_id`, `condition name`, and `replicate` as composite primary key.
- **References**: `condition_name` references the `condition_name` column in the `experimental_condition` table.

