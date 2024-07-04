#!/usr/bin/env python

"""
"""

from dataclasses import dataclass, field
import logging
from typing import Optional, List, Set

import psycopg2

from lib.table_id_mapper import map_id
from lib.table_string_interactions import get_string_targets
from lib.table_kegg_relations import get_kegg_targets
from lib.table_uniprot import get_gene_name
from lib.table_experimental_condition import condition_is_valid
from lib.table_transcriptomics import get_log2_fold_change

logger = logging.getLogger(__name__)

@dataclass
class Node:
    uniprot_accession: Optional[str] = None
    refseq_locus_tag: Optional[List[str]] = field(default_factory=list)
    locus_tag: Optional[List[str]] = field(default_factory=list)
    kegg_accession: Optional[List[str]] = field(default_factory=list)
    gene_name: Optional[str] = None
    weight: Optional[float] = None

    @property
    def id(self):


        if self.locus_tag:

            if self.gene_name:
                return ";".join([self.gene_name, self.locus_tag[0]])
            return self.locus_tag[0]

        if self.refseq_locus_tag:

            if self.gene_name:
                return ":".join([self.gene_name, self.refseq_locus_tag[0]])
            return self.refseq_locus_tag[0]


        if self.uniprot_accession:

            if self.gene_name:
                return ";".join([self.gene_name, self.uniprot_accession])
            return self.uniprot_accession

        return "NULL"

    def __str__(self):

        return self.id

    def has_no_ids(self):
        return not self.uniprot_accession and not self.refseq_locus_tag and not self.locus_tag and not self.kegg_accession


@dataclass(frozen=True)
class Relationship:
    source_node: Node
    target_node: Node
    interaction: str
    directed: bool
    source: str
    neighborhood_level: int

    weight: Optional[float] = None


    def __str__(self):
        return "\t".join(
            [
                str(self.source_node),
                str(self.source_node.weight),
                str(self.target_node),
                str(self.target_node.weight),
                self.interaction,
                str(self.directed).lower(),
                self.source,
                str(self.neighborhood_level),
                str(self.weight),
            ]
        )


    def __hash__(self):
        return hash((self.source_node.id, self.target_node.id))


    def __eq__(self, other):
        if isinstance(other, Relationship):
            return (self.source_node.id, self.target_node.id) == (other.source_node.id, other.target_node.id)
        return False


def build_node(conn: psycopg2.extensions.connection, id: str) -> Node | None:


    id_mappers = map_id(conn, id)

    node = Node()
    for id_mapper in id_mappers:

        if id_mapper.uniprot_accession:
            if not node.uniprot_accession:
                node.uniprot_accession = id_mapper.uniprot_accession
            elif node.uniprot_accession != id_mapper.uniprot_accession:
                logging.error(f"Multiple Uniprot accessions for {id}: {node.uniprot_accession}, {id_mapper.uniprot_accession}")
                raise ValueError

        if id_mapper.refseq_locus_tag:
            if id_mapper.refseq_locus_tag not in node.refseq_locus_tag:
                node.refseq_locus_tag.append(id_mapper.refseq_locus_tag)

        if id_mapper.locus_tag:
            if id_mapper.locus_tag not in node.locus_tag:
                node.locus_tag.append(id_mapper.locus_tag)

        if id_mapper.kegg_accession:
            if id_mapper.kegg_accession not in node.kegg_accession:
                node.kegg_accession.append(id_mapper.kegg_accession)

    if node.uniprot_accession:
        node.gene_name = get_gene_name(node.uniprot_accession, conn)

    if node.has_no_ids():
        return None

    return node


def add_kegg_relationships(
        conn: psycopg2.extensions.connection,
        node: Node,
        relationships: Set[Relationship],
        neighborhood_level: int,
        ) -> None:

    for kegg_accession in node.kegg_accession:

        kegg_targets = get_kegg_targets(conn, kegg_accession)

        for target in kegg_targets:

            target_node = build_node(conn, target)

            if not target_node:
                logging.warning(f"No ids found for {target}")
                continue

            repationship = Relationship(
                source_node=node,
                target_node=target_node,
                interaction="pp",
                directed=True,
                source="kegg",
                neighborhood_level=neighborhood_level,
            )

            if repationship not in relationships:
                relationships.add(repationship)


def add_string_relationships(
        conn: psycopg2.extensions.connection,
        node: Node,
        string_threshold: int,
        relationships: Set[Relationship],
        neighborhood_level: int,
        ) -> None:

    for refseq_locus_tag in node.refseq_locus_tag:

        string_targets = get_string_targets(conn, refseq_locus_tag, string_threshold)

        if not string_targets:
            continue

        for target in string_targets:

            target_node = build_node(conn, target)

            if not target_node:
                logging.warning(f"No ids found for {target}")
                continue

            repationship = Relationship(
                source_node=node,
                target_node=target_node,
                interaction="pp",
                directed=False,
                source="string",
                neighborhood_level=neighborhood_level,
            )

            inv_repationship = Relationship(
                source_node=target_node,
                target_node=node,
                interaction="pp",
                directed=False,
                source="string",
                neighborhood_level=neighborhood_level,
            )

            if repationship not in relationships and inv_repationship not in relationships:
                relationships.add(repationship)


def get_transcriptomics_weight(
        conn: psycopg2.extensions.connection,
        node: Node,
        condition_a: str,
        condition_b: str,
        ) -> float | None:

    weights = []

    if node.locus_tag:
        for locus_tag in node.locus_tag:
            weight = get_log2_fold_change(conn, locus_tag, condition_a, condition_b)
            if weight:
                weights.append(weight)

    if node.refseq_locus_tag:
        for refseq_locus_tag in node.refseq_locus_tag:
            weight = get_log2_fold_change(conn, refseq_locus_tag, condition_a, condition_b)
            if weight:
                weights.append(weight)

    if weights:
        return max(weights, key=abs)


def run_build_graph(
        conn: psycopg2.extensions.connection,
        id_list: List[str],
        depth: int,
        string_threshold: int,
        ) -> Set[Relationship]:

    relationships = set()

    query_nodes = []
    for id in id_list:
        node = build_node(conn, id)
        if not node:
            logging.warning(f"No ids found for {id}")
        else:
            query_nodes.append(node)

    for i in range(depth):

        neighborhood_level = i + 1

        for query_node in query_nodes:

            add_kegg_relationships(conn, query_node, relationships, neighborhood_level)

        for query_node in query_nodes:

            add_string_relationships(conn, query_node, string_threshold, relationships, neighborhood_level)

        query_nodes = [relationship.target_node for relationship in relationships]

    return relationships


def adjust_weights_with_expression_data(
        conn: psycopg2.extensions.connection,
        relationships: Set[Relationship],
        experiment_type: str,
        condition_a: str,
        condition_b: str,

        ) -> None:

    if not condition_is_valid(conn, experiment_type, condition_a):
        logging.error(f"Invalid condition: {condition_a}")
        raise ValueError
    if not condition_is_valid(conn, experiment_type, condition_b):
        logging.error(f"Invalid condition: {condition_b}")
        raise ValueError

    match experiment_type:

        case "transcriptomics":

            for relationship in relationships:

                relationship.source_node.weight = get_transcriptomics_weight(
                                                conn,
                                                relationship.source_node,
                                                condition_a,
                                                condition_b
                                                )
                relationship.target_node.weight = get_transcriptomics_weight(
                                                conn,
                                                relationship.target_node,
                                                condition_a,
                                                condition_b
                                                )

        case "proteomics":

            raise NotImplementedError


