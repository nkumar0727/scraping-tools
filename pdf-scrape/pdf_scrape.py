# TODO: does not work for relative URI; make it work for hrefs like
# /stanford/CH3_templates

from sys import argv
import httplib2
from bs4 import BeautifulSoup, SoupStrainer
import html
from threading import Thread, Lock
from time import time
import re

http_prefix = 'http://'
MAX_THREADS = 8
total_pdf_succeeded = 0
pdf_succeeded = 0
succeeded_lock = Lock()
failed_lock = Lock()
pdf_file_path = './'

def thread_worker(failed_jobs, job_list, idx):
    while idx < len(job_list):
        res = download_pdf(job_list[idx])
        if res is False:
            with failed_lock:
                failed_jobs.append(job_list[idx])
        idx += MAX_THREADS

def download_pdf(link_url):
    http = httplib2.Http()
    try:
        status, pdf_response = http.request(link_url)
        if status.status is not 200:
            print('ERROR: PDF %s could not be downloaded' % link_url)
            return False
        else:
            pdf_path = link_url.split('/')
            pdf_name = pdf_path[-1]
            global pdf_file_path
            full_pdf_location = (pdf_file_path + '/' + pdf_name).replace('//', '/')
            open(full_pdf_location, 'wb').write(pdf_response)
            print('Downloaded %s to file %s' % (link_url, full_pdf_location))
            global pdf_succeeded
            with succeeded_lock:
                pdf_succeeded += 1
            return True
    except OSError as e:
        print('ERROR: File %s could not be created from %s' % (full_pdf_location, link_url))
        return False
    except Exception as e:
        print('ERROR: request to %s for %s failed' % (link_url, str(e)))
        return False

def create_pdf_list_from_links(page_url, link_list):
    print(page_url)
    page_url = page_url[0:page_url.rfind('/')]
    print(page_url)
    pdf_list = []
    for link in link_list:
        if link.has_attr('href') and link['href'].endswith('.pdf'):
            url = link['href']
            print('LOOKING AT: ' + url)
            if re.match(r'^https?://.+\Z', url):
                print('absolute url')
                pdf_list.append(url)
            elif re.match(r'^/?.+\Z', url):
                print('relative url')
                if not url.startswith('/'):
                    url = '/' + url
                pdf_list.append((page_url + url))
            else:
                print('Url %s is not recognized' % url)
    return pdf_list

def get_page_from_url(http_obj, page_url):
    if not page_url.startswith(http_prefix):
        page_url = http_prefix + page_url
    status, response = http_obj.request(page_url)
    if status.status is not 200:
        print('Bad url: %s' % page_url)
        return None
    return response

def check_robots_txt(url):
    cleaned_url = url.replace('http://', '')
    domain = cleaned_url.split('/')[0]
    robots_txt_url = 'http://' + (domain + '/robots.txt').replace('//', '/')
    print(robots_txt_url)
    http = httplib2.Http()
    try:
        status, response = http.request(robots_txt_url)
        if status.status != 200:
            print('Problem finding robots.txt: error code %d' % status.status)
        else:
            print('Robots.txt found:')
            print(response)
    except Exception as e:
        print('Problem finding robotx.txt: %s' % str(e))


def main(argv):
    argc = len(argv)
    if argc <= 2 or argc % 2 == 0:
        print('Usage: python pdf_scrape.py [<url> <file_path>] [<url> <file_path>] ...')
        return
    http = httplib2.Http()
    start_time = time()
    total_pdfs = 0
    for url_id in range(1, len(argv), 2):
        target_url = argv[url_id].strip()
        #check_robots_txt(target_url)
        page_response = get_page_from_url(http, target_url)
        if page_response is not None:    
            link_list = BeautifulSoup(page_response, 'html.parser', parse_only=SoupStrainer('a'))
            pdf_list = create_pdf_list_from_links(target_url, link_list)
            total_pdfs += len(pdf_list)
            thread_list = []
            failed_pdf_list = []
            global pdf_file_path
            pdf_file_path = argv[url_id+1]
            for idx in range(MAX_THREADS):
                thread_list.append(Thread(target=thread_worker, args=(failed_pdf_list, pdf_list, idx,)))
            for thread in thread_list:
                thread.start()
            for thread in thread_list:
                thread.join()
            global pdf_succeeded, total_pdf_succeeded
            total_pdf_succeeded += pdf_succeeded
            print('\n%d/%d downloaded for %s in path %s' % (pdf_succeeded, len(pdf_list), target_url, argv[url_id+1]))
            if len(failed_pdf_list) != 0:
                print('Failed to download for %s in path %s: ' % (target_url, argv[url_id+1]))
                for failed_pdf in failed_pdf_list:
                    print(failed_pdf)
            print('\n')
            pdf_succeeded = 0
    end_time = time()
    print('\n%d/%d downloaded in total' % (total_pdf_succeeded, total_pdfs))
    print('Completed in %d seconds' % (end_time - start_time))
                        
if __name__ == "__main__":
    main(argv)

