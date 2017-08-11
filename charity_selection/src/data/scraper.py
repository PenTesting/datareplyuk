"""
Created by Dr. Christos Hadjinikolis
Sr. Data Scientist | Data Reply UK

Description: Scraping html from checkDirector.co.uk with beautifulSoup & TOR
"""
import os
import socket
import time
import urllib.request as req
from urllib.parse import quote

import pandas as pd
import socks
from bs4 import BeautifulSoup as bS
from tqdm import tqdm

from settings import INTERIM, EXTERNAL

# Global Variables
DEBUG = False
LINK = 'http://beta.charitycommission.gov.uk/charity-search/?q='


# Methods' section
def get_ids(registered_charities_df):
    """
    The method collects the id of each charity in the xlsx register.
    :param registered_charities_df: The xlsx register in the form of a df.
    :return: The same dataframe extended with an id column
    """

    # Create an empty dataframe for storing the respective id for every registered charity
    registered_charities_df["id"] = ""

    print('Collecting ids for registered charities:')
    print('--------------------------------------------------------\n\n')

    # set counter
    i = 0

    # loop through all charities in the xlsx file
    with tqdm(total=len(registered_charities_df["Organisation"])) as pbar:
        for charity in registered_charities_df["Organisation"]:

            link = LINK + quote(charity, safe='')

            try:
                if DEBUG:
                    print('------------------- FETCHING HTML ----------------------')
                    print(charity + ' --> ' + quote(charity, safe=''))
                    print('--------------------------------------------------------')

                response = req.urlopen(link, timeout=30)
                html = response.read()

                # load url and parse it with beatiful soup
                soup = bS(html, "html.parser")

                try:
                    top_result_id = soup\
                        .find("ol", class_="results-list")\
                        .find("li", class_="EvenRow")\
                        .find("h3")\
                        .find("a")['title'].split(":")[1:][0]
                except (AttributeError):
                    print("Missed due to error")

                # Add id to dataframe
                registered_charities_df['id'][i] = top_result_id
                i += 1

                if DEBUG:
                    print(top_result_id)
                    input('Press any key to continue ... ')

                time.sleep(1)

                pbar.update(1)  # increase bar by 1

            except (req.URLError,
                    socket.timeout,
                    req.HTTPError,
                    socks.GeneralProxyError,
                    IOError) as error:
                print('Error: ' + error)

    registered_charities_df.to_csv(os.path.join(INTERIM, 'charities_with_ids.csv'))

    return registered_charities_df


def get_info(charities_with_ids):
    """
    A function that collects data info for every registered charity.
    :param charities_with_ids: A dataframe of Charity Names and their respective ids (df).
    :return: An extended df with additional columns; 1 for every piece of information collected (df)
    """

    # TODO: Implement this method
    print("To complete...")

# Main method
if __name__ == "__main__":

    # Load xlsx file with charity names
    REGISTERED_CHARITIESS_DF = pd.read_excel(os.path.join(EXTERNAL, 'Registered_Charities.xlsx'))

    # Get charity ids
    CHARITIES_WITH_IDS = get_ids(REGISTERED_CHARITIESS_DF)

    # TODO: @kaxil --> Collect data from Overview and Financials through get_info()
    # e.g. http://beta.charitycommission.gov.uk/charity-details/?regid=1060508&subid=0
    # use this class --> ~/src/data/Charity.py

    # The function should got to http://beta.charitycommission.gov.uk/charity-details/?regid= <<ID>>
    # get_info(CHARITIES_WITH_IDS = get_ids(REGISTERED_CHARITIESS_DF))

    # TODO: fix error in my code
    # run it and you will see it... some extract ifs should do it (if None...blah blah)
