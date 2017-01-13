# -*- coding: utf-8 -*-
import click
from neo4jrestclient.client import GraphDatabase
import re


def get_graph_db(host):
    return GraphDatabase("http://{0}/db/data/".format(host))

@click.group()
def cli():
    pass

id_mapping = dict()

def get_node(statement):
    matcher = re.match(r"[a-z ]+\(_([0-9]+):(`[A-Za-z]+`)[^{]+({.+})\) *", statement)

    if matcher:
        group = matcher.groups()
        return dict(
            group = group,
            query = "(_{0}:{1} {2})".format(group[0], group[1], group[2]),
            replace_id = "_{0}".format(group[0]),
        )
        
    return None

def push_id_mapping(node):
    id_mapping[node.get("replace_id")] = node

def replace_id_from_mapping(statement, relationship):
    result = statement
    groups = relationship.groups()
    query_rules = []

    for group in groups:
        pk = "_" + group
        node = id_mapping.get(pk)
        query_rules.append(node.get("query"))

    result = "Match {0} {1}".format(",".join(query_rules), statement)

    return result
    

def get_relationship(relationship):
    return re.match(r"[a-zA-Z ]*_([0-9]+)-\[:.*\]->_([0-9]+)",relationship)
        


@cli.command()
@click.argument('file_path') 
@click.option('--host', default='localhost:7474', help='Enter a neo4j db host')
def restore(file_path, host):
    """
    Restore neo4j syntax to database.

    You able to use `neo4j -c dump > dump.sql`
    """

    gdb = get_graph_db(host)
    index = 1
    
    with open(file_path, "r") as f:
        commands = f.read().split("\n")
    
        for command in commands:
            if command:
                current_statement = command
                node = get_node(current_statement)
                current_relationship = get_relationship(current_statement)
    
                if node:
                    push_id_mapping(node)
                elif current_relationship:
                    current_statement = replace_id_from_mapping(current_statement, current_relationship)
                    
                result = gdb.query(q=current_statement)
                print(current_statement, result.get_response(), index)
                index = index + 1

if __name__ == '__main__':
    cli()

