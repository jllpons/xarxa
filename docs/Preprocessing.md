# Recommendations for Data Preprocessing in Some Datasets

Some datasets must be preprocessed or even generated before they can be inserted
into the database.

This page contains some recommendations for preprocessing data in some datasets.

## Table of Contents

- [String Data](#string-data)
- [Transcriptomics Data](#transcriptomics-data)

## String Data

Many times, the organism of interest will not be present among the available organisms in the STRING database. It is recommended to upload the proteome of the organism to the STRING database and download the protein network data.

This can be achieved combining the printval.py and tab_to_fasta.py scripts to generate a FASTA file with the proteome of the organism, which can be uploaded to the STRING database.

```shell
python src/printval.py refseq_genome refseq_locus_tag,protein_sequence \
| python src/tab_to_fasta.py > my_proteome.fasta
```

After uploading the proteome to the STRING database, we can download the protein network data. The file name should look something like this:

| File | Description |
| --- | --- |
| STRGXXXXXXX.protein.physical.links.v12.0.txt.gz (NNN.N KB) | protein network data (physical subnetwork, scored links between proteins) |

The we can use sed, tail, and awk to format the data in the file to a TSV file that can be inserted into the database.

```shell
cat <STRGXXXXXXX.protein.physical.links.v12.0.txt> \
| sed 's/ /\t/g' \
| tail -n +2   \
| awk -F'\t' '{
  sub(/^[^.]*\./, "", $1);
  sub(/^[^.]*\./, "", $2);
  print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
}' OFS='\t' > string_interactions_formatted.tsv
```

Finally, we can insert the data into the database. It will probably take some time since the STRING dataset is quite large.

```shell
python src/upsert_table.py string_interactions string_interactions_formatted.tsv
```

## Transcriptomics Data

We'll use an example dataset (`genAvsWT_all.tsv`) to demonstrate how to preprocess transcriptomics data.

If we use `xsv` with the `headers` subcommand, we can see the headers of the file.
```shell
xsv headers genAvsWT_all.tsv
```
```
1   <gene_id>
2   geneA1
3   geneA3
4   wt1
5   wt2
6   wt3
7   log2FoldChange
8   pvalue
9   padj
10  gene_name
11  gene_chr
12  gene_start
13  gene_end
14  gene_strand
15  gene_length
16  gene_biotype
17  gene_description
18  tf_family
19  genA1_readcount
20  genA3_readcount
21  wt1_readcount
22  wt2_readcount
23  wt3_readcount
24  genA1_fpkm
25  genA3_fpkm
26  wt1_fpkm
27  wt2_fpkm
28  wt3_fpkm
```

We can see that geneA has a missing replicate (replicate 2). We can add a dummy column to the file to make it easier to insert the data into the database.

```shell
cat geneAvsWT_all.tsv | sed 's/$/\tNULL/' > geneAvsWT_all_with_dummy.tsv
```

Now we can see that the file has an extra dummy column at the end.

```shell
xsv headers geneAvsWT_all_with_dummy.tsv
```
```
1   <gene_id>
2   geneA1
3   geneA3
4   wt1
5   wt2
6   wt3
7   log2FoldChange
8   pvalue
9   padj
10  gene_name
11  gene_chr
12  gene_start
13  gene_end
14  gene_strand
15  gene_length
16  gene_biotype
17  gene_description
18  tf_family
19  genA1_readcount
20  genA3_readcount
21  wt1_readcount
22  wt2_readcount
23  wt3_readcount
24  genA1_fpkm
25  genA3_fpkm
26  wt1_fpkm
27  wt2_fpkm
28  wt3_fpkm
29  NULL
```

For the `transcriptomics_quantification` table, we want to generate two files, 
one with the wt sample and anohter one with the geneA sample.

For wild type:

```shell
xsv select 1,21,22,23,4,5,6 geneAvsWT_all_with_dummy.tsv \
| xsv fmt -t $'\t' \
| tail -n +2  \
| grep --color=never 'MyLocusTag' > wt_quantification.tsv
```

In the snippet above we used:
- `xsv select` to select the columns we want to keep
- `xsv fmt` to format the output as a TSV file
- `tail` to remove the header
- `grep` to only keep the rows that have the a gene_id of interest (imagine
  "NovelGene..." or "sRNA..."). `--color=never` is used to avoid potential
  escape sequences that might be present in the output.


for the geneA sample, we must use the dummy column (29th) as filler for the missing
replicate.

```shell
xsv select 1,19,29,20,2,29,3 test.tsv \
| xsv fmt -t $'\t' \
| tail -n +2  \
| grep --color=never 'MyLocusTag' > geneA_quantification.tsv
```

Finally, we can insert the data into the database.

```shell
python src/upsert_table.py transcriptomics_quantification wt_quantification.tsv "gene_id" "wt" 
````

```shell
python src/upsert_table.py transcriptomics_quantification geneA_quantification.tsv "gene_id" "geneA" 
```

For the 'transcriptomics_differential' table, we can use:

```shell
xsv select 1,7,8,9 geneAvsWT_all_with_dummy.tsv \
| xsv fmt -t $'\t' \
| tail -n +2  \
| grep --color=never 'MyLocusTag' > differential.tsv
```

And then insert the data into the database.

```shell
python src/upsert_table.py transcriptomics_differential differential.tsv "gene_id" "geneA_vs_wt"
```

## Proteomics Data

Suppose we have a -very big- proteomics dataset in a file.

### PTMs

```shell
xsv headers peptideGroups.tsv
```
```
1    Checked
2    Tags
3    Confidence
4    PSM Ambiguity
5    Sequence
6    Modifications
7    Modifications (all possible sites)
8    Qvality PEP
9    Qvality q-value
10   SVM_Score
11   # Protein Groups
12   # Proteins
13   # PSMs
14   Master Protein Accessions
15   Positions in Master Proteins
16   Modifications in Master Proteins (all Sites)
17   Modifications in Master Proteins
18   Master Protein Descriptions
19   Protein Accessions
20   # Missed Cleavages
21   Theo. MH+ [Da]
22   Sequence Length
23   Contaminant
24   Abundance: F1: Sample
25   Abundance: F2: Sample
26   Abundance: F3: Sample
27   Abundance: F4: Sample
28   Abundance: F5: Sample
29   Abundance: F6: Sample
30   Abundance: F7: Sample
31   Abundance: F8: Sample
32   Abundance: F9: Sample
...
236  XCorr (by Search Engine): H2 Sequest HT
237  XCorr (by Search Engine): I2 Sequest HT
238  XCorr (by Search Engine): J2 Sequest HT
239  XCorr (by Search Engine): K2 Sequest HT
240  XCorr (by Search Engine): L2 Sequest HT
241  Top Apex RT [min]
```

First, we should see if all protein accessions are from the same type:

```
xsv select 14 peptideGroups.tsv | sort | uniq | less
```

We can use `xsv` to select the columns we want to keep and format the output as a TSV file. 
We can also combine it with `grep` to only keep the rows that have the protein accession of interest.

```shell
xsv select 14,6,17,5,15,4,8,9,11,3 peptideGroups.tsv \
| xsv fmt -t $'\t' \
| grep --color=never "^MyLocusTag" > peptideGroups.PTMs.MyLocusTag.tsv
```

Finally, we can insert the data into the database.

```shell
TODO
```

### Quantification

```shell
xsv headers proteins.tsv
```
```
1    Checked
2    Tags
3    Protein FDR Confidence: Combined
4    Master
5    Proteins Unique Sequence ID
6    Protein Group IDs
7    Accession
8    Description
9    Sequence
10   FASTA Title Lines
11   Exp. q-value: Combined
12   Contaminant
13   Sum PEP Score
14   # Decoy Protein: Combined
15   Coverage [%]
16   # Peptides
17   # PSMs
18   # Protein Unique Peptides
19   # Unique Peptides
20   # AAs
21   MW [kDa]
22   calc. pI
23   Score Sequest HT: A2 Sequest HT
24   Score Sequest HT: B2 Sequest HT
25   Score Sequest HT: C2 Sequest HT
26   Score Sequest HT: D2 Sequest HT
...
89   Abundance: F3: Sample
90   Abundance: F4: Sample
91   Abundance: F5: Sample
92   Abundance: F6: Sample
93   Abundance: F7: Sample
...
104  Abundances (Normalized): F6: Sample
105  Abundances (Normalized): F7: Sample
106  Abundances (Normalized): F8: Sample
107  Abundances (Normalized): F9: Sample
108  Abundances (Normalized): F10: Sample
...
116  Abundances Count: F6: Sample
117  Abundances Count: F7: Sample
118  Abundances Count: F8: Sample
119  Abundances Count: F9: Sample
120  Abundances Count: F10: Sample
121  Abundances Count: F11: Sample
122  Abundances Count: F12: Sample
123  # Protein Groups
124  Modifications
```

First, we should see if all protein accessions are from the same type:

```
xsv select 7 proteins.tsv | sort | uniq | less
```

We can use `xsv` to select the columns we want to keep and format the output as a TSV file.

```shell
xsv select 7,9,13,11,89,93-95,104-107,117-119 proteins.tsv \
| xsv fmt -t $'\t' \
| grep --color=never "^MyLocusTag" > proteins.quantification.MyCondition.MyLocusTag.tsv
```

Finally, we can insert the data into the database.

```shell
TODO
```


