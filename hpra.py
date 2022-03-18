import csv
import datetime
import json
import os.path
import threading
import traceback

import pandas as pd
import requests
from bs4 import BeautifulSoup
from UliPlot.XLSX import auto_adjust_xlsx_column_width

headers = ['Licence holder', 'Pano AU', 'URL', 'Trade Name', 'Active Substances', 'Dosage Form', 'Licence Holder',
           'Licence Number', 'ATC Code', 'Authorised/Withdrawn', 'Licence Issued', 'Legal status', 'Supply Status',
           'Advertising Status', 'Conditions of Licence', 'Marketing Status', 'Documents',
           'Educational Materials - HCP', 'Educational Materials - Patient', 'Generics Information']
outcsv = "Out.csv"
lock = threading.Lock()
threadcount = 3
semaphore = threading.Semaphore(threadcount)
site = "http://www.hpra.ie"
result = f"{site}/homepage/medicines/medicines-information/find-a-medicine/"
scraped = []


def scrape(url):
    with semaphore:
        try:
            pprint(f"Working on {url}")
            soup = getSoup(url)
            data = {
                "Licence holder": soup.find('span', {"class": "product_licenceholder"}).text,
                "Pano AU": soup.find('span', {"class": "pano AU"}).text,
                "URL": url,
            }
            divs = soup.find_all("div", {"class": "item_group"})
            for div in divs[:3]:
                for item in div.find_all('div', {"class": "item_row"}):
                    try:
                        val = item.find("span", {"class": "item_element"}).text
                        if item.find("span", {"class": "item_element"}).find('a') is not None:
                            val += f' ({item.find("span", {"class": "item_element"}).find("a")["href"]})'
                        data[item.find("span", {"class": "item_element_title"}).text] = val
                    except:
                        pprint(f"Error {item.text}")
            for div in divs[3:]:
                h3 = div.find("h3").text
                data[h3] = ""
                for item in div.find_all('div', {"class": "item_row"}):
                    try:
                        val = item.find("span", {"class": "item_element_title"}).text
                    except:
                        val = item.find("span", {"class": "item_element"}).text
                    if item.find("span", {"class": "item_element"}).find('a') is not None:
                        url = item.find("span", {"class": "item_element"}).find("a")["href"]
                        if url.startswith("/"):
                            url = f"{site}{url}"
                        val += f' ({url}), '
                    data[h3] += val
                if data[h3].endswith(", "):
                    data[h3] = data[h3][:-2]
                # pprint(div.text)
            pprint(json.dumps(data, indent=4))
            append(data)
        except:
            traceback.print_exc()
            pprint(f"Error {url}")
            with open("Error.txt", 'a') as efile:
                efile.write(url + "\n")


def main():
    global scraped
    logo()
    if not os.path.isfile(outcsv):
        with open(outcsv, "w", newline='', encoding='utf8') as outfile:
            c = csv.DictWriter(outfile, fieldnames=headers)
            c.writeheader()
    with open(outcsv, "r", newline='', encoding='utf8') as outfile:
        scraped = [x['URL'] for x in csv.DictReader(outfile, fieldnames=headers)]
    soup = getSoup(f"{result}/results")
    last = int(soup.find("a", {"title": "Last Page"})['href'].split("=")[-1])
    pprint(f"Total number of pages {last}")
    pprint(f"Total medicine: {soup.find('span', {'id': 'ContentPlaceHolderBody_C001__lblshowing'}).text.split()[3]}")
    pprint(f"Last updated: {soup.find('span', {'id': 'ContentPlaceHolderBody_C001__lbllastupdated'}).text}")
    pprint(f"Already scraped: {len(scraped) - 1}")
    threads = []
    for i in range(1, last + 1):
        pprint(f"Working on page#{i}")
        try:
            pagesoup = getSoup(f"{result}/results?page={i}")
            for medicine in pagesoup.find_all("a", {"class": "productname"}):
                url = result + medicine['href']
                if url not in scraped:
                    thread = threading.Thread(target=scrape, args=(url,))
                    thread.start()
                    threads.append(thread)
                else:
                    pprint(f"Already scraped {url}")
        except:
            pprint(f"Error on page#{i}")
    for thread in threads:
        thread.join()
    convert()
    pprint(f"Scraping finished, output results are in {outcsv}")


def append(data):
    with lock:
        with open(outcsv, "a", newline='', encoding='utf8') as outfile:
            c = csv.DictWriter(outfile, fieldnames=headers)
            c.writerow(data)
            scraped.append(data['URL'])


def pprint(msg):
    with lock:
        m = f"{datetime.datetime.now()}" + " | " + msg
        print(m)
        with open("logs.txt", 'a') as logfile:
            logfile.write(f"{m}\n")


def getSoup(url):
    # with open("index.html") as ifile:
    #     return BeautifulSoup(ifile.read(), 'lxml')
    return BeautifulSoup(requests.get(url).content, 'lxml')


def logo():
    os.system("color 0a")
    print(fr"""
        .__                                .__         
        |  |__  ______ _______ _____       |__|  ____  
        |  |  \ \____ \\_  __ \\__  \      |  |_/ __ \ 
        |   Y  \|  |_> >|  | \/ / __ \_    |  |\  ___/ 
        |___|  /|   __/ |__|   (____  / /\ |__| \___  >
             \/ |__|                \/  \/          \/ 
=================================================================
                hpra.ie medicine scraper by:
              https://github.com/evilgenius786
=================================================================
[+] Multithreaded (Thread count: {threadcount})
[+] Resumable
[+] Super fast
[+] Error/exception handling
[+] Proper logging
_________________________________________________________________
""")


def convert():
    df = pd.read_csv(outcsv)
    with pd.ExcelWriter(outcsv.replace(".csv", ".xlsx")) as writer:
        df.to_excel(writer, sheet_name="MySheet", index=False)
        auto_adjust_xlsx_column_width(df, writer, sheet_name="MySheet", margin=0, index=False)


if __name__ == '__main__':
    main()
    # convert()
