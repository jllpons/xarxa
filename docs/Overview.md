# Xarxa Database Overview

## Introduction

By integrating different data types, such as gene, transcript, and protein information along with their interactions and relationships, Xarxa aims to provide a platform for the study reglatory networks within an organism of interest.

## Table of Contents

- [Introduction](#introduction)
- [Database Design](#database-design)
```
#TODO
    - [Organism-Related Tables](#organism-related-tables)
    - [Protein-Level Tables](#protein-level-tables)
    - [Experimental Tables](#experimental-tables)
- [Integration and Interactivity](#integration-and-interactivity)
```

- [Table Descriptions](#table-descriptions)
    - [uniprot Table](#uniprot-table)
    - [refseq Table](#refseq-table)
    - [kegg Table](#kegg-table)
    - [kegg_relations Table](#kegg_relations-table)
    - [string_interactions Table](#string_interactions-table)
    - [experimental_condition Table](#experimental_condition-table)
    - [transcriptomics Table](#transcriptomics-table)
    - [transcriptomics_counts Table](#transcriptomics_counts-table)

### uniprot Table

#### Purpose

The `uniprot` table stores information from all known proteins in the UniProtKB database
for a given organism.

| Column Name | Data Type | Description |
|-------------|-----------|-------------
| `uniprot_accession` | `VARCHAR(20)` | The UniProtKB accession number assigned to the protein |
| `locus_tag` | `VARCHAR(20)[]` | List of the locus tag(s) of the gene(s) codifying for the specific protein |
| `orf_names` | `VARCHAR(20)[]` | Names that are temporarily attributed to an open reading frame (ORF) by a sequencing project |
| `kegg_accession` | `VARCHAR(20)[]` | List of the KEGG accession numbers referring the specific protein |
| `embl_protein_id` | `VARCHAR(20)` | The EMBL protein ID |
| `refseq_accession` | `VARCHAR(20)` | The RefSeq accession number |
| `keywords` | `VARCHAR(20)[]` | Summary of the UniProtKB entry using controlled vocabulary |
| `protein_name` | `VARCHAR(255)` | Full name of the protein as recommended by the UniProtKB |
| `protein_existence` | `VARCHAR(255)` | The evidence for the existence of the protein |
| `sequence` | `TEXT` | The amino acid sequence of the protein |
| `go_term` | `VARCHAR(20)[]` | List of Gene Ontology (GO) terms associated with the protein |
| `ec_number` | `VARCHAR(20)[]` | List of Enzyme Commission (EC) numbers associated with the protein |
| `ptm` | `JSONB` | Post-translational modifications (PTMs) of the protein |

#### Constraints

- **Primary Key**: `uniprot_accession`, the UniProt accession number for the entity.

#### Interactions

None

### refseq Table

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

- **Primary Key**: `refseq_locus_tag`, the RefSeq locus tag for the entity.

#### Interactions

None

### kegg Table

#### Purpose

Stores the main identifiers and relevant data extracted from all KEGG entries for the organism.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `kegg_accession` | `VARCHAR(20)` | The KEGG accession number |
| `kegg_pathway` | `VARCHAR(20)[]` | List of KEGG pathways associated with the gene |
| `kegg_orthology` | `VARCHAR(20)[]` | List of KEGG orthology groups associated with the gene |

#### Constraints

- **Primary Key**: `kegg_accession`, the KEGG accession number for the entity.

#### Interactions

None

### kegg_relations Table

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

- **Primary Key**: There is no primary key for this table since two relationships may only differ, for example, by the name of te compound.
- **References**: `source` and `target` reference the `kegg_accession` column in the `kegg` table.

#### Interactions


### string_interactions Table

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
- **References**: `protein_a` and `protein_b` reference the `refseq_accession` column in the `refseq` table.

### experimental_condition Table

#### Purpose

Stores the experimental condition used in the different experiments.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `name` | `VARCHAR(20)` | The name of the experimental condition |
| `description` | `TEXT` | The description of the experimental condition |
| `type` | `VARCHAR(20)` | The type of the experimental condition |

#### Constraints

- **Primary Key**: `name`, the name of the experimental condition.
- **Constraints**: `type` must be one of the following values: `transcriptomics`, `proteomics`, `phosphoproteomics`.

#### Interactions

None

### transcriptomics Table

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

- **Primary Key**: `experimental_id`, the ID used to identify the gene in the experiment.
- **References**: `condition_a` and `condition_b` reference the `name` column in the `experimental_condition` table.

#### Interactions

None

### transcriptomics_counts Table

#### Purpose

Stores the mapped read counts of the genes in the different transcriptomics experiments.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `experimental_id` | `VARCHAR(20)` | The ID used to identify the gene in the experiment |
| `condition_name` | `VARCHAR(255)` | The name of the first experimental condition |
| `replicate` | `INTEGER` | The replicate number of the condition |
| `read_count` | `INTEGER` | The read count of the gene |
| `normalized_read_count` | `DOUBLE PRECISION` | The normalized read count of the gene |

#### Constraints

- **Primary Key**: `experimental_id`, `condition name`, and `replicate` as composite primary key.
- **References**: `condition_name` references the `condition_name` column in the `experimental_condition` table.

#### Interactions

None

