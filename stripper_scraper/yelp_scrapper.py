# -*- coding: utf-8 -*-

from pymongo import MongoClient
from bs4 import BeautifulSoup
import urllib.request
import argparse
import re

SEARCH_TERM = 'Stipper Club'
YELP_URL = 'https://www.yelp.com'
CITIES = ['Portland OR', 'New York NY', 'Los Angeles CA', 'Chicago IL', 'Houston TX', 'Phoenix AZ', 'Philadelphia PA',
          'San Antonio TX', 'San Diego CA', 'Dallas TX', 'San Jose CA', 'Austin TX', 'San Francisco CA', 'Charlotte NC',
          'Seattle WA', 'Denver CO', 'Washington DC', 'Boston MA']


def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Yelp.com crawler/scrapper. Input a list of cities, a search term, "
                                                 "an output file and an optional review limit",
                                     fromfile_prefix_chars='@')

    parser.set_defaults(
        city_file="../data/cities.txt",
        output_file="../data/review_data.csv",
        search_term="Stipper Club",
        review_limit=False,
    )
    parser.add_argument('--city-file', '-c',
                        help="Text file containing the list of cities you want to search. "
                             "Default: '%(default)s")
    parser.add_argument('--output-file', '-o',
                        help="CSV file to write scraping results to."
                             "Default: '%(default)s'")
    parser.add_argument('--search-term', '-s',
                        help="Search term for query."
                             "Default: '%(default)s'")
    parser.add_argument('--review-limit', '-r',
                        action='store_true',
                        help="(Optional) Limit for number of total number of reviews to scrape."
                             "Default: '%(default)s'")

    namespace = parser.parse_args(args)
    return namespace


def crawl_results(search_term, city, collection=None):
    yelp_search_url = '{}/search?find_desc={}s&find_loc={}'.format(YELP_URL, search_term.replace(' ', '+'),
                                                                   city.replace(' ', '+'))
    soup = BeautifulSoup(urllib.request.urlopen(yelp_search_url).read(), "html5lib")

    # grab list of results, iterate through list and then find the next page
    # crawl results and pull of the results
    # if there is a 'next' page go to it and then repeat
    while True:
        results = soup.findAll('div', attrs={'class': re.compile(r'biz-listing-large')})
        header = soup.find('h1').text.strip().lstrip()
        num_pages = soup.find('div', attrs={'class': re.compile(r'page-of-pages arrange_unit arrange_unit--fill')}).text
        print("Scraping page: {}, {}".format(header, num_pages.lstrip()))
        for r in results:
            sub_href = r.find('a')
            if sub_href:
                if sub_href['href'].startswith('/biz/'):
                    result_page = '{}{}'.format(YELP_URL, sub_href['href'])
                    crawl_business_reviews(result_page, city, collection)
        next_page = soup.find('a', attrs={'class': 'u-decoration-none next pagination-links_anchor'})
        if next_page:
            yelp_result_page = '{}{}'.format(YELP_URL, next_page['href'])
            soup = BeautifulSoup(urllib.request.urlopen(yelp_result_page).read(), "html5lib")
        else:
            break


def crawl_business_reviews(yelp_result_page, city, collection=None):
    counter = 0
    soup = BeautifulSoup(urllib.request.urlopen(yelp_result_page).read(), "html5lib")
    business_dict = {}
    business_dict['business-name'] = soup.find('h1').text.strip().lstrip()
    try:
        business_dict['score'] = soup.find('div', attrs={'class':'biz-rating biz-rating-very-large clearfix'}).img['alt'].split()[0]
        business_dict['review-count'] = soup.find('div', attrs={'class':'biz-rating biz-rating-very-large clearfix'}).span.text.split()[0]
    except AttributeError:
        business_dict['score'] = 0
        business_dict['review-count'] = 0
        return
    print('Scraping reviews for {}, n = {}'.format(business_dict['business-name'], business_dict['review-count']))
    while True:
        counter = get_page_reviews(soup, business_dict, city, counter, collection)
        # Find if there is a next button, if not end loop
        next_page = soup.find('a', attrs={'class': 'u-decoration-none next pagination-links_anchor'})
        if next_page:
            yelp_result_page = next_page['href']
            soup = BeautifulSoup(urllib.request.urlopen(yelp_result_page).read(), "html5lib")
        else:
            break


def get_page_reviews(soup, business_dict, city, counter, collection=None):
    result_list = soup.findAll('div', attrs={'class': 'review-content'})
    for result in result_list:
        review_details = {}
        review_details.update(business_dict)
        review_details['city'] = city
        review_details['text'] = result.p.text
        review_details['date'] = result.span.text.strip().lstrip()
        review_details['rating'] = result.img['alt'].split()[0]
        if collection is not None:
            collection.insert_one(review_details)
        else:
            print(review_details)
        counter += 1
    print("\tAdded {} results of {}".format(counter, business_dict['review-count']))
    return counter


if __name__ == '__main__':
    client = MongoClient()
    db = client['StripperClub']
    for city in CITIES:
        collection = db[city]
        crawl_results(SEARCH_TERM, city, collection)