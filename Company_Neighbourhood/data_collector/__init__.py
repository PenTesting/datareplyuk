"""
Author: Dr. C. Hadjinikolis
Date:   19/10/2016

Details:    The data collector package is intended for populating a neo4j database with a company's
            n-hop neighbourhood of companies and officers. To run you will need:
            1. A neo4j running instance.
            2. Company House API credentials.
            3. You may have to delete the contents of /Users/<user>/.neo4j/known_host.
            4. Check module_neo4j for unique constraints issues.

Improvments:    Need to create a module dedicated to requests - they are badly handled atm.
"""
# - LIBRARIES -------------------------------------------------------------------------------------#
from module_neo4j import create_officer_node, create_company_node, create_relationship
from module_company import Company
from module_officer import Officer
from requests.auth import HTTPBasicAuth
import datetime
import requests
import time


# - GLOBAL VARIABLES ------------------------------------------------------------------------------#
COMPANIES = {}
COMPANY_NODES = {}
OFFICERS = {}
OFFICER_NODES = {}


# - SUB-FUNCTIONS ---------------------------------------------------------------------------------#
def get_company_neighbourhood(company_id):
    """
    This function is the core of the program. It is iteratively executed for each company identified
    in the immediate n-hop network of the original company_id input.
    :param company_id: A company's unique identifier.
    :return: A list of company ids associated with "company_id" through directors with
    (active/inactive) positions in
    both (List of Strings).
    """

    # CREATE CALL STRING --------------------------------------------------------------------------#
    call = 'https://api.companieshouse.gov.uk/company/' + unicode(str(company_id))
    print "--------------------------------------------------------------------------------"
    print "NEIGHBOURHOOD FOR: " + unicode(str(company_id)) + " @: " + str(datetime.datetime.now())
    print "--------------------------------------------------------------------------------"

    try:
        # MAKE CALL -------------------------------------------------------------------------------#
        response = requests.get(url=call,
                                auth=HTTPBasicAuth('3oYfAJNSaKgrxgG0RPcpxIPe6J7p2n3EHYJfyJvg', ''))

        # CHECK IF RATE LIMIT HAS BEEN VIOLATED ---------------------------------------------------#
        if response.status_code is 429:
            print("Too many requests!!! Sleeping for 5 minutes!")
            time.sleep(5 * 60)
            print("Woke up and repeating request for " + str(company_id) + ".")
            response = requests.get(url=call,
                                    auth=HTTPBasicAuth('3oYfAJNSaKgrxgG0RPcpxIPe6J7p2n3EHYJfyJvg',
                                                       ''))

        if response.status_code is 200:

            # PRINT JSON OUTPUT FOR TESTING -------------------------------------------------------#
            print "--------------------------- GENERAL COMPANY DATA -------------------------------"
            # rsp = json.loads(response.content)  # convert response into json format.
            # pprint.pprint(rsp)  # PrettyPrint

            cmpny = Company(company_id,
                              response.json()['company_name'],
                              response.json()['type'],
                              response.json()['jurisdiction'],
                              response.json()['company_status'])

            # ADD EXTRA STATIC DETAILS IF AVAILABLE -----------------------------------------------#
            if 'sic_codes' in response.json():
                cmpny.sic_codes = response.json()['sic_codes']
            if 'registered_office_address' in response.json():
                if 'postal_code' in response.json()['registered_office_address']:
                    cmpny.postal_code = response.json()['registered_office_address']['postal_code']
            if 'date_of_creation' in response.json():
                if len(response.json()['date_of_creation']) is 10:
                    cmpny.effective_from[0] = int(response.json()['date_of_creation'][8:])
                    cmpny.effective_from[1] = int(response.json()['date_of_creation'][5:7])
                    cmpny.effective_from[2] = int(response.json()['date_of_creation'][0:4])

            # PRINT GENERAL DATA ------------------------------------------------------------------#
            print "Id:\t" + str(cmpny.id)
            print "Name:\t" + str(cmpny.name)
            print "Type:\t" + str(cmpny.type)
            print "Status:\t" + str(cmpny.status)
            print "Jurisdiction:\t" + str(cmpny.jurisdiction)

            # PRINT CONDITIONAL DATA --------------------------------------------------------------#
            if 'sic_codes' in response.json():
                print "Sic codes:\t" + str(cmpny.sic_codes)
            if 'registered_office_address' in response.json():
                if 'postal_code' in response.json()['registered_office_address']:
                    print "PC:\t" + str(cmpny.postal_code)
            if 'date_of_creation' in response.json():
                if len(response.json()['date_of_creation']) is 10:
                    print "Effective from:\t" + str(cmpny.effective_from)

            # SAVE COMPANY ------------------------------------------------------------------------#
            COMPANIES[cmpny.id] = cmpny

            # CREATE COMPANY NODE -----------------------------------------------------------------#
            print("-------------------------------------------------------------------------------")
            company_node = create_company_node(cmpny)
            COMPANY_NODES[cmpny.id] = company_node

            # GET OFFICERS ASSOCIATED WITH COMPANY ------------------------------------------------#
            officers = cmpny.get_officers()     # with this alone we get all officers associated
                                                # with a company

            # While server hangs up then try again after 5 minutes
            while officers is None:
                print "Repeat request for getting all officers..."
                officers = cmpny.get_officers()

            if officers == -1:
                print "Issue with this officer-->skipping neighbouhood for this company..."
                return None

            # GET ADDITIONAL DATA ABOUT EACH OFFICER & WHERE ELSE THEY SERVE/-ED ------------------#
            for key, value in officers.iteritems():

                # CREATE NEW OFFICER OBJECT -------------------------------------------------------#
                # If officer_key has NOT been encountered before:
                if key not in OFFICERS:
                    officer = Officer(key)

                    # While server hangs up then try again after 5 minutes
                    officer_companies = officer.get_companies()
                    while officer_companies is None:
                        print "Repeat request for getting all officer roles..."
                        officer_companies = officer.get_companies()

                    if officer_companies == -1:
                        print "Issue with this officer-->skipping..."
                        continue

                    for company_id in officer_companies:     # ...and additional data if present!
                        # Make sure that the company at hand (company.id) is not included in its own
                        # "relationships"
                        if company_id is not cmpny.id:
                            # Note: Previous officer may be overwritten
                            cmpny.relationships[company_id] = officer.id

                    # ADD NEW OFFICER INTO OFFICERs LIST ------------------------------------------#
                    OFFICERS[key] = officer

                    # CREATE OFFICER NODE ---------------------------------------------------------#
                    print("-----------------------------------------------------------------------")
                    # Yet another essay! Sorry but this needs some extensive elaboration. Almost
                    # anywhere we look for director data, for reasons we still don't get, it seems
                    # like the same people are stored under multiple unique identifiers. This calls
                    # for some fuzzy join to resolve redundancies.
                    #
                    # Unfortunately, when stored with different ids, it is usually the case that they
                    # are not stored with all possible attributes they actually have, so we can only
                    # effectively join on a director's name.
                    #
                    # In fact no joining will be attempted. Instead:
                    # 1. When an officer with the same name is found a new officer object will be
                    # added in OFFICERS.
                    # 2. An officer node however, will only be created if a node with the same
                    # director name does not exist.
                    # 3. When the collection process is over a routine will be run to identify
                    # whether nodes added are also populated with all available attributes. If
                    # not, this will be added as a separate process.

                    # CHECK IF AN OFFICER_NODE WITH THE SAME NAME EXISTS --------------------------#
                    officer_exists = False
                    for officer_key, officer_object in OFFICER_NODES.iteritems():
                        # If name already exists:
                        officer_name = officer.name.encode('ascii', 'ignore').decode('ascii')
                        if officer_name == officer_object.properties["name"]\
                                .encode('ascii', 'ignore')\
                                .decode('ascii'):
                            # Then don't create a new node
                            # Notice: a "redundant" officer object exists tho!
                            officer_exists = True
                            existing_officer_key = officer_key
                            break

                    # SO IF AN OFFICER WITH THE SAME NAME EXISTS USE THE EXISTING NODE ------------#
                    if officer_exists:
                        officer_node = OFFICER_NODES[existing_officer_key]

                        # CHECK IF THE EXISTING NODE LACKS *NOW* AVAILABLE DATA -------------------#
                        if "CoR" not in officer_node.properties:
                            if officer.CoR is not None:
                                officer_node.properties["CoR"] = officer.CoR

                        if "DoB" not in officer_node.properties:
                            if officer.DoB is not None:
                                officer_node.properties["DoB"] = officer.DoB[1]

                        if "nationality" not in officer_node.properties:
                            if officer.nationality is not None:
                                officer_node.properties["nationality"] = officer.nationality

                    # ELSE, IF NO OFFICER WITH THE SAME NAME EXISTS CREATE A NEW NODE -------------#
                    else:
                        officer_node = create_officer_node(officer)
                        OFFICER_NODES[officer.id] = officer_node

                    # CREATE RELATIONSHIP ---------------------------------------------------------#
                    create_relationship(officer_node, company_node, officer, cmpny)

                # if officer has been encountered before
                else:
                    officer = OFFICERS[key]

                    for company_id in officer.companies:
                        # Make sure that the company at hand (company.id) is not included in its own
                        # "relationships"
                        if company_id is not cmpny.id:
                            # Note: Previous officer may be overwritten
                            cmpny.relationships[company_id] = officer.id

                    # FIND THE NODE THAT WAS USED BEFORE ------------------------------------------#
                    # Notice: Remember that all directors (even same people with different ids) are
                    # sotred in to OFFICERS, but not all directors are stored into OFFICER_NODES. So
                    # we need to find the node that was used for a director that has been used
                    # before using their name.

                    for officer_key, officer_object in OFFICER_NODES.iteritems():
                        officer_name = officer.name.encode('utf-8')
                        if officer_name == officer_object.properties["name"].encode('utf-8'):
                            # Then don't create a new node (Notice: an "redundant" officer object
                            # exists tho!)
                            existing_officer_key = officer_key
                            break

                    officer_node = OFFICER_NODES[existing_officer_key]  # Note: No new data
                                                                        # collected in this case!

                    # CREATE RELATIONSHIP ---------------------------------------------------------#
                    create_relationship(officer_node, company_node, officer, cmpny)

                # PURPOSELY DELAY DATA COLLECTION TO AVOID REACHING RATE LIMIT --------------------#
                time.sleep(2.5)   # sleep for 2.5 seconds to avoid rate limitations

            # RETURN NEIGHBOURING COMPANIES -------------------------------------------------------#
            return cmpny.relationships

        # IF RESPONSE STATUS IS NOT VALID ---------------------------------------------------------#
        else:
            print "Response status code: " + str(response.status_code)
            print "Something went wrong!"
            return None

        # END IF ----------------------------------------------------------------------------------#

    # IF SERVER DROPS CONNECTION ------------------------------------------------------------------#
    except requests.exceptions.ChunkedEncodingError as ex:

        print ex.message
        print "Connection reset by peer! Sleep for 5 mins..."
        time.sleep(5 * 60)
        return -1

# - MAIN CODE -------------------------------------------------------------------------------------#
if __name__ == "__main__":

    # START WITH GETTING THE COMPANY's UNIQUE ID --------------------------------------------------#
    first_company_id = raw_input("Please provide company_id (e.g. Reply Ltd = 03847202 or "
                                 "Vodafone Ltd = 01471587): ")

    # GET NEIGHBOURHOOD HOP RADIUS ----------------------------------------------------------------#
    max_hops = int(raw_input("Maximum number of hops away from company: "))

    # GET COMPANY NEIGHBOURHOOD -------------------------------------------------------------------#
    hops = 0
    neighbourhood = [first_company_id]
    while hops < max_hops:
        hops += 1
        print("-----------------------------------------------------------------------------------")
        print "HOP #" + str(hops)
        print("-----------------------------------------------------------------------------------")

        print("COMPANIES COLLECTED SO FAR:")
        for key, value in COMPANIES.iteritems():
            print str(key) + ",",

        print
        print("NEW NEIGHBOURHOOD:"),
        for element in neighbourhood:
            print str(element) + ",",

        print
        print("-----------------------------------------------------------------------------------")

        # IF DIRECT LINKS HAVE BEEN IDENTIFIED ----------------------------------------------------#
        if len(neighbourhood) > 0:
            # stores the new set of adjacent companies for each company in "adjacent_companies"
            next_neighbourhood = []
            for company in neighbourhood:

                # Get direct links for every company (links are 1-hop neighbours)
                neighbours_of_company = get_company_neighbourhood(company)

                # While server drops call then repeat request after 5 mins..
                while neighbours_of_company == -1:
                    print "Repeating get neightbourhood request... "
                    neighbours_of_company = get_company_neighbourhood(company)

                if neighbours_of_company is None:
                    print "NO DATA FOUND FOR THIS COMPANY..."
                    continue

                # Add neighbours to be traversed only if they haven't been traversed already
                for company_key, company_director_role in neighbours_of_company.iteritems():
                    next_neighbourhood.append(company_key)

            # REMOVE DUPLICATES in "next_neighbourhood" FOR NEXT LOOP -----------------------------#
            # Assume that a company has 2 directors. If both of them are also assigned in another
            # company A then that company (A) will appear twice in the first company's
            # neighbourhood and needs to be removed.
            temp_neighbourhood = list(set(next_neighbourhood))

            # CURRENTLY TRAVERSED COMPANIES -------------------------------------------------------#
            # Find all currently traverse companies
            keys = list(COMPANIES.keys())

            # FINALISE NEIGHBOURHOOD --------------------------------------------------------------#
            neighbourhood = list(set(temp_neighbourhood) - set(keys))

        # IF NO MORE LINKS ARE AVAILABLE ----------------------------------------------------------#
        else:
            print("-------------------------------------------------------------------------------")
            print "HOP #" + str(hops) + ": No more neighbouring companies!"
            print("-------------------------------------------------------------------------------")
            break

    # END IF --------------------------------------------------------------------------------------#

# - END OF FILE -----------------------------------------------------------------------------------#
