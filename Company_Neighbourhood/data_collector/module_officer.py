# OFFICER MODULE ----------------------------------------------------------------------------------#
# - LIBRARIES -------------------------------------------------------------------------------------#
from requests.auth import HTTPBasicAuth
from array import array
import datetime
import requests
import time


# CLASS -------------------------------------------------------------------------------------------#
class Officer:
    """
    Every user associated with a company should have the following attributes:
    name: User's full name (string).
    ids: List of ids associated with the user (list of Strings).
    dob: Date of birth composed just of month and year (2 element array).
    nationality: User's nationality (string).
    CoR: Country of Residence (String).
    roles: Current role in each companie (Map of String Values).
    active_roles: Current active roles (Map of boolean values).
    companies: List of company codes (String[]).
    companies_names: List of names of every company a director si appointed to (A map).
    """

    # CONSTRUCTOR ---------------------------------------------------------------------------------#
    def __init__(self, officer_id):
        self.id = officer_id
        self.name = None
        self.DoB = array("i", [-1, -1])
        self.nationality = None
        self.CoR = None
        self.roles = {}
        self.active_roles = {}
        self.companies = []

    def get_companies(self):
        """
        This function's main purpose is to return a director's assignments (active and inactive).
        Additional data about  a director may also be collected.
        :return: List of companies associated with a director (Lilst of Strings).
        """

        print("-------------------------------------------------------------------------------")
        print("Collecting data for Officer:" + str(self.id) + " @: " + str(datetime.datetime.now()))
        print("-------------------------------------------------------------------------------")

        try:
            # MAKE REST CALL ----------------------------------------------------------------------#
            response = requests.get(url='https://api.companieshouse.gov.uk/officers/' + str(self.id)
                                        + '/appointments?items_per_page=10000&start_index=0',
                                    auth=HTTPBasicAuth('3oYfAJNSaKgrxgG0RPcpxIPe6J7p2n3EHYJfyJvg', ''))

            # items_per_page=10000 is set to 10000 as this is an extraordinary large number, decreasing
            # the possibility that we could miss on an active officer. However, items returned per
            # page is set to a max equal to 50... so additional measures are taken below to deal with
            #  this.

            # CHECK IF RATE LIMIT HAS BEEN VIOLATED -----------------------------------------------#
            if response.status_code is 429:
                print("Too many requests!!! Sleeping for 5 minutes!")
                time.sleep(5 * 60)
                print("Woke up and repeating request for " + str(self.id) + ".")
                response = requests.get(url='https://api.companieshouse.gov.uk/officers/'
                                            + str(self.id)
                                            + '/appointments?items_per_page=10000&start_index=0',
                                        auth=HTTPBasicAuth(
                                                '3oYfAJNSaKgrxgG0RPcpxIPe6J7p2n3EHYJfyJvg',
                                                ''))

            # GET ALL OFFICERS --------------------------------------------------------------------#
            elif response.status_code is 200:

                # GET NAME & ADDITIONAL DETAILS IF PRESENT ----------------------------------------#
                # The 0 index refers to the 1st element in the json output
                self.name = response.json()["items"][0]["name"]\
                    .encode('ascii', 'ignore')\
                    .decode('ascii')

                # ADD EXTRA STATIC DETAILS IF AVAILABLE -------------------------------------------#
                if "date_of_birth" in response.json():
                    year = int(response.json()["date_of_birth"]["year"])
                    month = int(response.json()["date_of_birth"]["month"])
                    self.DoB = array("i", [month, year])
                if "nationality" in response.json()["items"][0]:
                    self.nationality = response.json()["items"][0]["nationality"]
                if "country_of_residence" in response.json()["items"][0]:
                    self.CoR = response.json()["items"][0]["country_of_residence"]

                # LIST ASSOCIATED ACTIVE COMPANIES ------------------------------------------------#
                for item in response.json()['items']:
                    if 'resigned_on' not in item:
                        company_id = item["appointed_to"]["company_number"]
                        self.companies.append(company_id)
                        self.roles[company_id] = item["officer_role"]
                        self.active_roles[company_id] = True

                # END FOR LOOP  -------------------------------------------------------------------#

                # CHECK FOR MORE PAGES TO COLLECT WITH MORE RESULTS -------------------------------#
                # If less items are returned from the available items then increase the
                # results index and repeat request until no more data are available.
                total_results = response.json()['total_results']
                start_index = response.json()['items_per_page']
                while total_results > start_index:
                    print "Retrieved " + str(start_index) \
                          + " out of " + str(total_results) + " appointments."

                    response = requests.get(url='https://api.companieshouse.gov.uk/officers/'
                                                + str(self.id)
                                                + '/appointments?items_per_page=10000&'
                                                + 'start_index='
                                                + str(start_index),
                                            auth=HTTPBasicAuth(
                                                    '3oYfAJNSaKgrxgG0RPcpxIPe6J7p2n3EHYJfyJvg',
                                                    ''))

                    # Check if rate limit has been reached...
                    if response.status_code is 429:
                        print("Too many requests!!! Sleeping for 5 minutes!")
                        time.sleep(5 * 60)
                        print("Woke up and repeating request for " + str(self.id) + ".")
                        response = requests.get(url='https://api.companieshouse.gov.uk/officers/'
                                                    + str(self.id)
                                                    + '/appointments?items_per_page=10000&'
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

                    # LIST ASSOCIATED ACTIVE COMPANIES --------------------------------------------#
                    for item in response.json()['items']:
                        if 'resigned_on' not in item:
                            company_id = item["appointed_to"]["company_number"]
                            self.companies.append(company_id)
                            self.roles[company_id] = item["officer_role"]
                            self.active_roles[company_id] = True

                    # END FOR LOOP ----------------------------------------------------------------#

                    # Move to next page by increasing the results_index
                    start_index += response.json()['items_per_page']

                    # Sleep for 2 secs to avoid rate limit restrictions
                    time.sleep(2)

                # END WHILE LOOP ------------------------------------------------------------------#

                # PRINT DATA FOR TESTING ----------------------------------------------------------#
                for key, value in self.roles.iteritems():
                    print "Company: " + str(key) + \
                          "\tRole: " + str(value) + \
                          "\tActive: " + str(self.active_roles[key])

                return self.companies

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

# - END OF FILE -----------------------------------------------------------------------------------#
