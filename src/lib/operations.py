#!/usr/bin/env python

"""
"""

# Retrive all proteins involved in a specific pathway
QUERY_RETRIEVE_ALL_PROTEINS_FOR_PATHWAY = """
SELECT {master_id_table}.{master_id}
FROM {master_id_table}
JOIN {protein_effector_table} ON {master_id_table}.{master_id} = {protein_effector_table}.{effector_id}
JOIN {pathway_table} 
WHERE p.{pathway_id} = %s
"""




