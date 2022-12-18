
import re
import time
from statistics import median
from bs4 import BeautifulSoup, NavigableString, Tag, Comment
from .utils import logger
import time
from datetime import datetime
import copy
import os
from abc import ABCMeta
import multiprocessing as mp

from .utils import search_terms as master_search_terms
from .utils import args, logger



class Document(object):
    __metaclass__ = ABCMeta

    def __init__(self, file_path, doc_text, extraction_method):
        self._file_path = file_path
        self.doc_text = doc_text
        self.extraction_method = extraction_method
        self.log_cache = []

    def get_excerpt(self, input_text, form_type, metadata_master,
                    skip_existing_excerpts):
        
        start_time = time.process_time()
        self.prepare_text()
        prep_time = time.process_time() - start_time
        file_name_root = metadata_master.metadata_file_name
        for section_search_terms in master_search_terms[form_type]:
            start_time = time.process_time()
            metadata = copy.copy(metadata_master)
            warnings = []
            section_name = section_search_terms['itemname']
            section_output_path = file_name_root + '_' + section_name
            txt_output_path = section_output_path + '_excerpt.txt'
            metadata_path = section_output_path + '_metadata.json'
            failure_metadata_output_path = section_output_path + '_failure.json'

            search_pairs = section_search_terms[self.search_terms_type()]
            text_extract, extraction_summary, start_text, end_text, warnings = \
                self.extract_section(search_pairs)
            time_elapsed = time.process_time() - start_time
            # metadata.extraction_method = self.extraction_method
            metadata.section_name = section_name
            if start_text:
                start_text = start_text.replace('\"', '\'')
            if end_text:
                end_text = end_text.replace('\"', '\'')
            metadata.endpoints = [start_text, end_text]
            metadata.warnings = warnings
            metadata.time_elapsed = round(prep_time + time_elapsed, 1)
            metadata.section_end_time = str(datetime.utcnow())
            if text_extract:
                # success: save the excerpt file
                metadata.section_n_characters = len(text_extract)
                with open(txt_output_path, 'w', encoding='utf-8',
                          newline='\n') as txt_output:
                    #text_extract=re.sub('|','',text_extract)
                    text_extract = text_extract.replace('|', ' ')
                    txt_output.write(text_extract)
                log_str = ': '.join(['SUCCESS Saved file for',
                                         section_name, txt_output_path])
                self.log_cache.append(('DEBUG', log_str))
                try:
                    os.remove(failure_metadata_output_path)
                except:
                    pass
              
            else:
                log_str = ': '.join(['No excerpt located for ',
                                         section_name, metadata.sec_index_url])
                self.log_cache.append( log_str)
                try:
                    os.remove(metadata_path)
                except:
                    pass
          
            if args.write_sql:
                metadata.save_to_db()
        return(self.log_cache)

    def prepare_text(self):
        # handled in child classes
        pass

#flag for using html2text. set to true for now.
USE_HTML2TEXT = True

class HtmlDocument(Document):
    soup = None
    plaintext = None

    def __init__(self, *args, **kwargs):
        super(HtmlDocument, self).__init__(*args, **kwargs)

    def search_terms_type(self):
        return "html"

    def prepare_text(self):
       
        html_text = self.doc_text
        # remove whitespace sometimes found inside tags,
        html_text = re.sub('<\s', '<', html_text)
        html_text = re.sub('(<small>|</small>)', '', html_text,
                           flags=re.IGNORECASE)
        # for simplistic no-tags HTML documents (example: T 10K 20031231),
        # make sure the section headers get treated as new blocks.
        html_text = re.sub(r'(\nITEM\s{1,10}[1-9])', r'<br>\1', html_text,
                           flags=re.IGNORECASE)

        start_time = time.process_time()
        try:
            soup = BeautifulSoup(html_text, 'lxml')
        except:
            soup = BeautifulSoup(html_text, 'html.parser')      # default parser
        parsing_time_elapsed = time.process_time() - start_time
        log_str = 'parsing time: ' + '% 3.2f' % \
                     (parsing_time_elapsed) + 's; ' + "{:,}". \
                     format(len(html_text)) + ' characters; ' + "{:,}". \
                     format(len(soup.find_all())) + ' HTML elements'
        self.log_cache.append(('DEBUG', log_str))

        if len(html_text) / len(soup.find_all()) > 500:
            html_text = re.sub(r'\n\n', r'<br>', html_text,
                               flags=re.IGNORECASE)
            soup = BeautifulSoup(html_text, 'html.parser')

        # Remove numeric tables from soup and save them to a text file
        tables_generator = (s for s in soup.find_all('table'))
        #path to table file to be changed
        tables_debug_file = open(r'C:/Users/i.abbasi/ImpaxNLPInfoRetriever/FilingScraper/edgar/output_files/tables_detected.txt', 'wt', encoding='utf-8')
        for s in tables_generator:
            #s.replace_with('[DATA_TABLE_REMOVED]')
            tables_debug_file.write( ' '+'\n')
            tables_debug_file.write(' ' + '\n'.join([x for x in s.text.splitlines()
            if x.strip()]).encode('latin-1','replace').decode('latin-1')+ ' ')
        tables_debug_file.close()
        self.soup = soup

        if USE_HTML2TEXT:
      
            import html2text
            h = html2text.HTML2Text(bodywidth=0)
            h.ignore_emphasis = True
            self.plaintext = h.handle(str(soup)) 
        else:
          
            paragraph_string = ''
            document_string = ''
            all_paras = []
            ec = soup.find()
            is_in_a_paragraph = True
            while not (ec is None):
                if is_line_break(ec) or ec.next_element is None:
                    # end of paragraph tag 
                    if is_in_a_paragraph:
                        is_in_a_paragraph = False
                        all_paras.append(paragraph_string)
                        document_string = document_string + '\n\n' + paragraph_string
                else:
                    # continuation of the current paragraph
                    if isinstance(ec, NavigableString) and not \
                            isinstance(ec, Comment):
                        # # remove redundant line breaks and other whitespace at the
                        # # ends, and in the middle, of the string
                        ecs = re.sub(r'\s+', ' ', ec.string)
                        if len(ecs) > 0:
                            if not (is_in_a_paragraph):
                                # set up for the start of a new paragraph
                                is_in_a_paragraph = True
                                paragraph_string = ''
                            # paragraph_string = paragraph_string + ' ' + ecs
                            paragraph_string = paragraph_string + ecs
                ec = ec.next_element
            # clean up multiple line-breaks
            document_string = re.sub('\n\s+\n', '\n\n', document_string)
            document_string = re.sub('\n{3,}', '\n\n', document_string)
            self.plaintext = document_string


    def extract_section(self, search_pairs):
       
        start_text = 'na'
        end_text = 'na'
        warnings = []
        text_extract = None
        for st_idx, st in enumerate(search_pairs):
     
            item_search = re.findall(st['start']+'.*?'+ st['end'],
                                     self.plaintext,
                                     re.DOTALL | re.IGNORECASE)
           
            if item_search:
                longest_text_length = 0
                for s in item_search:
                    if isinstance(s, tuple):
                        # If incorrect use of multiple regex groups has caused
                        # more than one match, then s is returned as a tuple
                        self.log_cache.append(('ERROR',
                                   "Groups found in Regex, please correct"))
                    if len(s) > longest_text_length:
                        text_extract = s.strip()
                        longest_text_length = len(s)
                # final_text_new = re.sub('^\n*', '', final_text_new)
                final_text_lines = text_extract.split('\n')
                start_text = final_text_lines[0]
                end_text = final_text_lines[-1]
                break
        extraction_summary = self.extraction_method + '_document'
        if not text_extract:
            warnings.append('Extraction did not work for HTML file')
            extraction_summary = self.extraction_method + '_document: failed'
        else:
            text_extract = re.sub('\n\s{,5}Table of Contents\n', '',
                                  text_extract, flags=re.IGNORECASE)

        return text_extract, extraction_summary, start_text, end_text, warnings


    def should_remove_table(self, html):
   
        char_counts = []
        if html.stripped_strings:
            for t in html.stripped_strings:
                if len(t) > 0:
                    char_counts.append(len(t))
            return len(char_counts) > 3 and median(char_counts) < 30
        else:
            self.log_cache.append(('ERROR',
                                   "the should_remove_table function is broken"))



def is_line_break(e):


    is_block_tag = e.name != None and e.name in ['p', 'div', 'br', 'hr', 'tr',
                                                 'table', 'form', 'h1', 'h2',
                                                 'h3', 'h4', 'h5', 'h6']
    # handle block tags inside tables: if the apparent block formatting is

    if is_block_tag and e.parent.name == 'td':
        if len(e.parent.findChildren(name=e.name)) == 1:
            is_block_tag = False
    # inspect the style attribute of element e (if any) to see if it has
    # block style, which will appear as a line break in the document
    if hasattr(e, 'attrs') and 'style' in e.attrs:
        is_block_style = re.search('margin-(top|bottom)', e['style'])
    else:
        is_block_style = False
    return is_block_tag or is_block_style

