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

logger = logging.getLogger(__name__)

@dataclass
class Node:
    uniprot_accession: Optional[str] = None
    refseq_locus_tag: Optional[List[str]] = field(default_factory=list)
    locus_tag: Optional[List[str]] = field(default_factory=list)
    kegg_accession: Optional[List[str]] = field(default_factory=list)

    @property
    def id(self):
        return "::".join(
            [
                self.uniprot_accession if self.uniprot_accession else "NULL",
                ";".join(self.refseq_locus_tag) if self.refseq_locus_tag else "NULL",
                ";".join(self.locus_tag) if self.locus_tag else "NULL",
                ";".join(self.kegg_accession) if self.kegg_accession else "NULL",
            ]
        )

    def __str__(self):

        return self.id

    def has_no_ids(self):
        return not self.uniprot_accession and not self.refseq_locus_tag and not self.locus_tag and not self.kegg_accession


@dataclass(frozen=True)
class Relationship:
    source_node: Node
    target_node: Node
    directed: bool
    source: str
    neighborhood_level: int

    weight: Optional[float] = None


    def __str__(self):
        return ",".join(
            [
                str(self.source_node),
                str(self.target_node),
                str(self.directed),
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
                directed=False,
                source="string",
                neighborhood_level=neighborhood_level,
            )

            inv_repationship = Relationship(
                source_node=target_node,
                target_node=node,
                directed=False,
                source="string",
                neighborhood_level=neighborhood_level,
            )

            if repationship not in relationships and inv_repationship not in relationships:
                relationships.add(repationship)


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

