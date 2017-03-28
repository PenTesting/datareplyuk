# - COMPANY MODULE --------------------------------------------------------------------------------#
# - LIBRARIES -------------------------------------------------------------------------------------#
from array import array
from requests.auth import HTTPBasicAuth
import requests
import time


# CLASS -------------------------------------------------------------------------------------------#
class Company:
    """
    name: Company name (String)
    type: Company type, e.g. "LtD" (String).
    effective_from: Date of creation (Integer array[3]).
    postal_code: Postal code (String).
    jurisdiction Where the company operates (String).
    sic_codes:  Standard Industrial Classification SIC is a system for classifying industries
                by a four-digit code (List of Ints).
    relationships: Support edges with other companies (MAP: Key: Company_id, Value: Officer_id).
    officers: Map of company officers and their corresponding roles (Key:officer_uid Value:role).
    acitve_officers: Map of boolean values.
    """

    # CONSTRUCTOR ---------------------------------------------------------------------------------#
    def __init__(self, company_id, company_name, cmp_type, jurisdiction, status):
        self.id = company_id
        self.name = company_name
        self.type = cmp_type
        self.status = status
        self.effective_from = array("i", [-1, -1, -1])
        self.postal_code = None
        self.jurisdiction = jurisdiction
        self.sic_codes = []
        self.relationships = {}
        self.officers = {}
        self.active_officers = {}

    # GET COMPANY OFFICERS ------------------------------------------------------------------------#
    def get_officers(self):
        """
        This function's main purpose is to return a company's directors (active and inactive).
        :return: List of companies associated with a director (Lilst of Strings).
        :return: A map of officer ids and their corresponding roles (Dictionary, Key: officer_id,
        Value: company role)
        """
        try:
            # MAKE REST CALL ----------------------------------------------------------------------#
            response = requests.get(url='https://api.companieshouse.gov.uk/company/' + str(self.id)
                                        + '/officers?items_per_page=10000&start_index=0',
                                    auth=HTTPBasicAuth('3oYfAJNSaKgrxgG0RPcpxIPe6J7p2n3EHYJfyJvg'
                                                       , ''))

            # items_per_page=10000 is set to 10000 as this is an extraordinary large number,
            # decreasing the possibility that we could miss on an active officer

            # CHECK IF RATE LIMIT HAS BEEN VIOLATED -----------------------------------------------#
            if response.status_code is 429:
                print("Too many requests!!! Sleeping for 5 minutes!")
                time.sleep(5 * 60)
                print("Woke up and repeating request for " + str(self.id) + ".")
                response = requests.get(url='https://api.companieshouse.gov.uk/company/'
                                            + str(self.id)
                                            + '/officers?items_per_page=10000&start_index=0',
                                        auth=HTTPBasicAuth('3oYfAJNSaKgrxgG0RPcpxIPe6J7p2n3EHYJfyJvg',
                                                           ''))

            # GET ALL OFFICERS --------------------------------------------------------------------#
            elif response.status_code is 200:

                # Check if there are results to be collected
                if 'total_results' in response.json():
                    total_results = response.json()['total_results']
                    if total_results is not 0:

                        # PRINT JSON OUTPUT FOR TESTING -------------------------------------------#
                        print "------------------------------ OFFICERS ----------------------------"

                        # This will be stored to the company module and is not related to the user
                        # module.

                        for item in response.json()['items']:

                            # GET USER UNIQUE ID --------------------------------------------------#
                            uid = item["links"]["officer"]["appointments"]
                            uid = uid[10:uid.find("appointments") - 1]

                            # CHECK IF OFFICER IS NOT RESIGNED ------------------------------------#
                            if "resigned_on" not in item:

                                # ACTIVE ROLE? (YES or NO) ----------------------------------------#
                                self.active_officers[uid] = True

                                # SAVE OFFICER AND ROLE -------------------------------------------#
                                self.officers[uid] = item["officer_role"]

                        # END FOR LOOP ------------------------------------------------------------#

                        # CHECK FOR MORE PAGES TO COLLECT WITH MORE RESULTS -----------------------#
                        # If less items are returned from the available items then increase the
                        # results index and repeat request until no more data are available.
                        start_index = response.json()['items_per_page']
                        while total_results > start_index:

                            print "Retrieved " + str(start_index) \
                                  + " out of " + str(total_results) + " officers."

                            response = requests.get(url='https://api.companieshouse.gov.uk/company/'
                                                        + str(self.id)
                                                        + '/officers?items_per_page=10000&'
                                                        +'start_index='
                                                        +str(start_index),
                                                    auth=HTTPBasicAuth(
                                                        '3oYfAJNSaKgrxgG0RPcpxIPe6J7p2n3EHYJfyJvg', ''))

                            # Check if rate limit has been reached...
                            if response.status_code is 429:
                                print("Too many requests!!! Sleeping for 5 minutes!")
                                time.sleep(5 * 60)
                                print("Woke up and repeating request for " + str(self.id) + ".")
                                response = requests.get(url='https://api.companieshouse.gov.uk/company/'
                                                            + str(self.id)
                                                            + '/officers?items_per_page=10000&'
                                                            + 'start_index='
                                                            + str(start_index),
                                                        auth=HTTPBasicAuth(
                                                                '3oYfAJNSaKgrxgG0RPcpxIPe6J7p2n3EHYJfyJvg',
                                                                ''))

                            # Check if response is not 200 as should...
                            if response.status_code is not 200:
                                print "Response status code: " + str(response.status_code)
                                print "Something went wrong!"
                                return -1

                            for item in response.json()['items']:

                                # GET USER UNIQUE ID
                                # -----------------------------------------------------------------#
                                uid = item["links"]["officer"]["appointments"]
                                uid = uid[10:uid.find("appointments") - 1]

                                # CHECK IF OFFICER IS NOT RESIGNED
                                # -----------------------------------------------------------------#
                                if "resigned_on" not in item:
                                    # ACTIVE ROLE? (YES or NO)
                                    # -------------------------------------------------------------#
                                    self.active_officers[uid] = True

                                    # SAVE OFFICER AND ROLE
                                    # -------------------------------------------------------------#
                                    self.officers[uid] = item["officer_role"]

                            # END FOR LOOP --------------------------------------------------------#

                            # Move to next page by increasing the results_index
                            start_index += response.json()['items_per_page']

                            # Sleep for 2 secs to avoid rate limit restrictions
                            time.sleep(2)

                         # END WHILE LOOP ---------------------------------------------------------#

                        # PRINT DATA FOR TESTING --------------------------------------------------#
                        for key, value in self.officers.iteritems():
                            print "Officer: " + str(key) + \
                                  "\tRole: " + str(value) + \
                                  "\tActive: " + str(self.active_officers[key])

                        return self.officers

                    else:

                        # IF NO OFFICERS ARE REGISTERED -------------------------------------------#
                        print('No  officers registered!')
                        return -1

                    # END IF ----------------------------------------------------------------------#
                else:

                    # IF TOTAL RESULTS IS MISSING FROM THE JSON FILE THEN -------------------------#
                    print('No  officers registered!')
                    return -1

                # END IF --------------------------------------------------------------------------#

            # IF RESPONSE STATUS IS NOT VALID -----------------------------------------------------#
            else:
                print "Response status code: " + str(response.status_code)
                print "Something went wrong!"
                return -1

            # END IF ------------------------------------------------------------------------------#

        # IF SERVER DROPS CONNECTION --------------------------------------------------------------#
        except requests.exceptions.ChunkedEncodingError as ex:

            print ex.message
            print "Connection reset by peer! Sleep for 5 mins..."
            time.sleep(5 * 60)
            return None

# ---- END OF FILE --------------------------------------------------------------------------------#
