import multiprocessing as mp
import os
import re
import copy
from bs4 import BeautifulSoup
from .utils import args, logger, requests_get
from .json_data import Metadata
from .utils import search_terms as master_search_terms
from .scrape_html import HtmlDocument
from .scrape_text import TextDocument


class SECEdgar(object):
    def __init__(self):
        self.storage_folder = None
       

    def download_filings(self, company_description, edgar_search_string,
                         filing_search_string, date_search_string,
                         start_date, end_date,
                         do_save_full_document, count=100):
        """
        returns string of extracted etxt, and warnings, if any
        """

        filings_links = self.download_filings_links(edgar_search_string,
                                                    company_description,
                                                    filing_search_string,
                                                    date_search_string,
                                                    start_date, end_date, count)

        filings_list = []

        logger.info("Identified " + str(len(filings_links)) +
                    " filings, gathering relevant  document links...")

        is_multiprocessing = args.multiprocessing_cores > 0
        if is_multiprocessing:
            pool = mp.Pool(processes = args.multiprocessing_cores)

        for i, index_url in enumerate(filings_links):
            # Get the URL for the document which packages all
            # of the parts of the filing
            base_url = re.sub('-index.htm.?','',index_url) + ".txt"
            filings_list.append([index_url, base_url, company_description])
            filing_metadata = Metadata(index_url)

            if re.search(date_search_string,
                         str(filing_metadata.sec_period_of_report)):
                filing_metadata.sec_index_url = index_url
                filing_metadata.sec_url = base_url
                filing_metadata.company_description = company_description
                if is_multiprocessing:
                    # multi-core processing. Add jobs to pool.
                    pool.apply_async(self.download_filing,
                                     args=(filing_metadata, do_save_full_document),
                                     callback=self.process_log_cache)
                else:
                    # single core processing
                    log_cache = self.download_filing(filing_metadata, do_save_full_document)
                    self.process_log_cache(log_cache)
        if is_multiprocessing:
            pool.close()
            pool.join()
        logger.debug("Finished attempting to download all the %s forms for %s",
                     filing_search_string, company_description)


    def process_log_cache(self, log_cache):
        """Output log_cache messages via logger
        """
        for msg in log_cache:
            msg_type = msg[0]
            msg_text = msg[1]
            if msg_type=='process_name':
                id = '(' + msg_text + ') '
            elif msg_type=='INFO':
                logger.info(id + msg_text)
            elif msg_type=='DEBUG':
                logger.debug(id + msg_text)
            elif msg_type=='WARNING':
                logger.warning(id + msg_text)
            elif msg_type=='ERROR':
                logger.error(id + msg_text)



    def download_filings_links(self, edgar_search_string, company_description,
                               filing_search_string, date_search_string,
                               start_date, end_date, count):
        """edgar_search_string: 10-digit integer CIK code, or ticker
     
         filing_search_string: e.g. '10-K'
        start_date: ccyymmdd
        end_date: ccyymmdd
        count:
        retusn: linkList, a list of links to main pages for each filing found
        """

        sec_website = "https://www.sec.gov/"
        browse_url = sec_website + "cgi-bin/browse-edgar"
        requests_params = {'action': 'getcompany',
                           'CIK': str(edgar_search_string),
                           'type': filing_search_string,
                           'datea': start_date,
                           'dateb': end_date,
                           'owner': 'exclude',
                           'output': 'html',
                           'count': count}
        logger.info('_' * 100)
        logger.info(
            "Querying EDGAR database for " + filing_search_string + ", Search: " +
            str(edgar_search_string) + " (" + company_description + ")")

        linkList = []  # List of all links from the CIK page
        continuation_tag = 'first pass'

        while continuation_tag:
            r = requests_get(browse_url, params=requests_params)
            if continuation_tag == 'first pass':
                logger.debug("EDGAR search URL: " + r.url)
                logger.info('-' * 100)
            data = r.text
            soup = BeautifulSoup(data, "html.parser")
            for link in soup.find_all('a', {'id': 'documentsbutton'}):
                URL = sec_website + link['href']
                linkList.append(URL)
            continuation_tag = soup.find('input', {'value': 'Next ' + str(count)}) # finds a button labelled 'Next 100' for example
            if continuation_tag:
                continuation_string = continuation_tag['onclick']
                browse_url = sec_website + re.findall('cgi-bin.*count=\d*', continuation_string)[0]
                requests_params = None
        return linkList


    def download_filing(self, filing_metadata, do_save_full_document):
      
        log_cache = [('process_name', str(os.getpid()))]
        filing_url = filing_metadata.sec_url
        company_description = filing_metadata.company_description
        log_str = "Retrieving: %s, %s, period: %s, index page: %s" \
            % (filing_metadata.sec_company_name,
                    filing_metadata.sec_form_header,
                    filing_metadata.sec_period_of_report,
                    filing_metadata.sec_index_url)
        log_cache.append(('DEBUG', log_str))

        r = requests_get(filing_url)
        filing_text = r.text
        filing_metadata.add_data_from_filing_text(filing_text[0:10000])

        # Iterate through the DOCUMENT types that we are seeking,
        
        filtered_search_terms = {doc_type: master_search_terms[doc_type]
                                 for doc_type in args.documents}
        for document_group in filtered_search_terms:
            doc_search = re.search("<DOCUMENT>.{,20}<TYPE>" + document_group +
                                   ".*?</DOCUMENT>", filing_text,
                                   flags=re.DOTALL | re.IGNORECASE)
            if doc_search:
                doc_text = doc_search.group()
                doc_metadata = copy.copy(filing_metadata)
                # look for form type near the start of the document.
                type_search = re.search("(?i)<TYPE>.*",
                                        doc_text[0:10000])
                if type_search:
                    document_type = re.sub("^(?i)<TYPE>", "", type_search.group())
                    document_type = re.sub(r"(-|/|\.)", "",
                                         document_type)  # remove hyphens etc
                else:
                    document_type = "document_TYPE_not_tagged"
                    log_cache.append(('ERROR',
                                      "form <TYPE> not given in form?: " +
                                      filing_url))
                local_path = os.path.join(self.storage_folder,
                        company_description + '_' + \
                        filing_metadata.sec_cik + "_" + document_type + "_" + \
                        filing_metadata.sec_period_of_report)
                doc_metadata.document_type = document_type
                # doc_metadata.form_type_internal = form_string
                doc_metadata.document_group = document_group
                doc_metadata.metadata_file_name = local_path

                # search for a <html>...</html> block in the DOCUMENT
                html_search = re.search(r"<(?i)html>.*?</(?i)html>",
                                        doc_text, re.DOTALL)
                xbrl_search = re.search(r"<(?i)xbrl>.*?</(?i)xbrl>",
                                        doc_text, re.DOTALL)
    
                text_search = re.search(r"<(?i)text>.*?</(?i)text>",
                                        doc_text, re.DOTALL)
                if text_search and html_search \
                        and text_search.start() < html_search.start() \
                        and html_search.start() > 5000:
                    html_search = text_search
                if xbrl_search:
                    doc_metadata.extraction_method = 'xbrl'
                    doc_text = xbrl_search.group()
                    main_path = local_path + ".xbrl"
                    reader_class = HtmlDocument
                elif html_search:
                    # if there's an html block inside the DOCUMENT then just
                    # take this instead of the full DOCUMENT text
                    doc_metadata.extraction_method = 'html'
                    doc_text = html_search.group()
                    main_path = local_path + ".htm"
                    reader_class = HtmlDocument
                else:
                    doc_metadata.extraction_method = 'txt'
                    main_path = local_path + ".txt"
                    reader_class = TextDocument
                doc_metadata.original_file_size = str(len(doc_text)) + ' chars'
                sections_log_items = reader_class(
                    doc_metadata.original_file_name,
                    doc_text, doc_metadata.extraction_method).\
                    get_excerpt(doc_text, document_group,
                                doc_metadata,
                                skip_existing_excerpts=False)
                log_cache = log_cache + sections_log_items
                if do_save_full_document:
                    with open(main_path, "w") as filename:
                        filename.write(doc_text)
                    log_str = "Saved file: " + main_path + ', ' + \
                        str(round(os.path.getsize(main_path) / 1024)) + ' KB'
                    log_cache.append(('DEBUG', log_str))
                    filing_metadata.original_file_name = main_path
                else:
                    filing_metadata.original_file_name = \
                         "file was not saved locally"
        return(log_cache)



