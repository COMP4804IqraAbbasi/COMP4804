
import re
from .scrape_html import Document


class TextDocument(Document):
    def __init__(self, *args, **kwargs):
        super(TextDocument, self).__init__(*args, **kwargs)

    def search_terms_type(self):
        return "txt"

    def extract_section(self, search_pairs):
   
        start_text = '  '
        end_text = '  '
        warnings = []
        text_extract = None
        for st_idx, st in enumerate(search_pairs):
     
            item_search = re.findall(st['start'] + '.*?' + st['end'],
                                     self.doc_text,
                                     re.DOTALL | re.IGNORECASE)
            if item_search:
                longest_text_length = 0
                for s in item_search:
                    text_extract = s.strip()
                    if len(s) > longest_text_length:
                        longest_text_length = len(text_extract)
                # final_text_new = re.sub('^\n*', '', final_text_new)
                final_text_lines = text_extract.split('\n')
                start_text = final_text_lines[0]
                end_text = final_text_lines[-1]
                break
        if text_extract:
            # final_text = '\n'.join(final_text_lines)
            # text_extract = remove_table_lines(final_text)
            #text_extract = remove_table_lines(text_extract)
            extraction_summary = self.extraction_method + '_document'
        else:
            warnings.append('Extraction did not work for text file')
            extraction_summary = self.extraction_method + '_document: failed'
        return text_extract, extraction_summary, start_text, end_text, warnings




