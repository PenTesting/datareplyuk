# NEO4J MODULE ------------------------------------------------------------------------------------#
"""
Author: Dr. C. Hadjinikolis
Date:   19/10/2016

Details:    Un-comment conde in lines 19-20 and run only the first time you run __init__.py. Note
            that this script is cleaning the database at the start of every run.
"""

# /Users/christoshadjinikolis/neo4j/CompanyHouse
# - LIBRARIES -------------------------------------------------------------------------------------#
from py2neo import Graph, Node, Relationship

# - CONNECTING TO NEO4J LOCAL SERVER --------------------------------------------------------------#
graph = Graph('localhost', user='neo4j', password='neo4j1')
graph.delete_all()

# CREATE UNIQUENESS CONSTRAINTS -------------------------------------------------------------------#
# Works only the first time - need to remove afterwards (Nothing we can do --> it's a py2neo issue)
# graph.schema.create_uniqueness_constraint("Officer", "id")
# graph.schema.create_uniqueness_constraint('Company', 'id')


# - SUB-FUNCTIONS ---------------------------------------------------------------------------------#
def create_company_node(company):
    """
    Simple function that gets an officer object and creates and returns a node object out of it.
    :param company: An Company object.
    :return: A Node object.
    """
    print ("Node:" + company.name + " of type: Company.")
    node = Node("Company",
                id=company.id,
                name=company.name,
                type=company.type,
                status=company.status,
                effective_from=company.effective_from[2],
                postal_code=company.postal_code,
                jurisdiction=company.jurisdiction,
                sic_codes=company.sic_codes)

    tx = graph.begin()
    tx.create(node)
    tx.commit()
    return node


def create_officer_node(officer):
    """
    Simple function that gets an officer object and creates and returns a node object out of it.
    :param officer: An Officer object.
    :return: A Node object.
    """

    print ("NODE:" + officer.name + " of type: Officer.")
    node = Node("Officer",
                id=officer.id,
                name=officer.name,
                DoB=officer.DoB[1],
                nationality=officer.nationality,
                CoR=officer.CoR)

    tx = graph.begin()
    tx.create(node)
    tx.commit()
    return node


def create_relationship(officer_node, company_node, officer, company):
    """
    Simple function that takes node objects as inputs and creates a relationship between them.
    :param officer_node: Officer-node object (Node).
    :param company_node: Company-node object (Node).
    :param officer: Officer object (Officer)
    :param company: Company object (Company).
    """

    try:
        print ("RELATIONSHIP: Officer:" +
               str(officer.name) + " is a: " +
               str(officer.roles[company.id]) +
               " at " + str(company.name))

        relation = Relationship(officer_node,
                                officer.roles[company.id],
                                company_node,
                                active=officer.active_roles[company.id])
        tx = graph.begin()
        tx.merge(relation)
        tx.commit()

    except KeyError as ex:
        print "For Officer id:" + str(officer.id) + ".[" + ex.message + "]"
        print "key:" + ex.message + " is missing..."
        print "Suggested fix: check number of results return per page for every request."



# ---- END OF FILE --------------------------------------------------------------------------------#
