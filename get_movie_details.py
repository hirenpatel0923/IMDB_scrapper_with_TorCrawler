from bs4 import BeautifulSoup
import requests, csv
from TorCrawler import TorCrawler
import time
import requests


def get_basic_details(process_name, page_soup, url):
    lst = url.split('/')
    #title
    movie_title_with_year = page_soup.select('h1')[0]
    movie_title_with_year.find('span').decompose()
    movie_title = movie_title_with_year.text.strip()
    print(movie_title)

    #ratings
    rating_div = page_soup.find(class_='ratingValue')
    ratings = ''
    if rating_div is not None:
        ratings = rating_div.strong.span.text.strip()

    #genres
    all_div = page_soup.find_all(class_='see-more inline canwrap')
    g = []
    for div in all_div:
        if div.h4.text.strip() == 'Genres:':
            genres = div.find_all('a')
            for genre in genres:
                g.append(genre.text.strip())
        else:
            div.decompose()

    with open(process_name + '_movies.csv','a', encoding='UTF-8') as f:
        f.write(lst[4]+ ',' + movie_title + ',' + ratings + ',')
        for genre in g:
            f.write(genre + ' ')
        f.write('\n')

    load_more_count = 0
    load_more_key = ''
    while True:
        if load_more_count == 10 or load_more_key == 'break':
            break
        elif load_more_count == 0:
            review_page = requests.get(url + '/reviews?spoiler=hide&sort=reviewVolume&dir=desc&ratingFilter=0')
            review_page_soup = BeautifulSoup(review_page.text, 'html.parser')
            with open(process_name + '_reviwes.csv', 'a', encoding='UTF-8') as f:
                load_more_count = load_more_count + 1
                load_more_key = get_all_reviews(review_page_soup, lst[4], f)
        else:
            temp_url = url + "/reviews/_ajax?sort=reviewVolume&dir=desc&spoiler=hide&ref_=undefined&paginationKey="+load_more_key
            review_page = requests.get(temp_url)
            review_page_soup = BeautifulSoup(review_page.text, 'html.parser')
            with open(process_name + '_reviwes.csv', 'a', encoding='UTF-8') as f:
                load_more_count = load_more_count + 1
                load_more_key = get_all_reviews(review_page_soup, lst[4], f)


def get_all_reviews(review_page_soup, title, f):
    review_divs = review_page_soup.find_all(class_='lister-item-content')
    for div in review_divs:
        ratings = div.find(class_="rating-other-user-rating")
        rats = ''
        username = ''
        userlink = ''
        if ratings is not None:
            rats = ratings.span.text.strip()
        user_lnk = div.find(class_="display-name-link")
        if user_lnk is not None:
            username = user_lnk.a.text.strip()
            userlink = user_lnk.a.get('href').strip()
        review = div.find(class_="text show-more__control")
        if review is not None:
            for br in review.find_all('br'):
                br.extract()
        #r = [review.text.replace(',',' ').split()]
        f.write(title + ',' +username + ',' + userlink +','+ rats + ',' + review.text.replace(',',' ') + '\n')

    load_more = review_page_soup.find(class_="load-more-data")
    if load_more.get('data-key') is not None:
        return load_more.get('data-key')
    else:
        return 'break'