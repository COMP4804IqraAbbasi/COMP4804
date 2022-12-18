from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

def extract_triplets(text):
    triplets = []
    relation, subject, relation, object_ = '', '', '', ''
    text = text.strip()
    current = 'x'
    for token in text.replace("<s>", "").replace("<pad>", "").replace("</s>", "").split():
        if token == "<triplet>":
            current = 't'
            if relation != '':
                triplets.append({'head': subject.strip(), 'type': relation.strip(),'tail': object_.strip()})
                relation = ''
            subject = ''
        elif token == "<subj>":
            current = 's'
            if relation != '':
                triplets.append({'head': subject.strip(), 'type': relation.strip(),'tail': object_.strip()})
            object_ = ''
        elif token == "<obj>":
            current = 'o'
            relation = ''
        else:
            if current == 't':
                subject += ' ' + token
            elif current == 's':
                object_ += ' ' + token
            elif current == 'o':
                relation += ' ' + token
    if subject != '' and relation != '' and object_ != '':
        triplets.append({'head': subject.strip(), 'type': relation.strip(),'tail': object_.strip()})
    return triplets

# Load model and tokenizer
tokenizer = AutoTokenizer.from_pretrained("Babelscape/rebel-large")
model = AutoModelForSeq2SeqLM.from_pretrained("Babelscape/rebel-large")
gen_kwargs = {
    "max_length": 256,
    "length_penalty": 0,
    "num_beams": 3,
    "num_return_sequences": 3,
}

# Text to extract triplets from
#path to raw text file dump containing filings from various companies 
with open('result.txt', 'r') as file: #path to raw text
    doc = file.read().replace('\n', '')


#todo: docstring
import spacy
import crosslingual_coreference
DEVICE = -1 # Number of the GPU, -1 if want to use CPU
#initialising coreference correction model
coref = spacy.load('en_core_web_sm')
coref.max_length = 1500000 
coref.add_pipe(
    "xx_coref", config={"chunk_size": 2500, "chunk_overlap": 2, "device": DEVICE})

#doc=coref(doc)

from nltk.tokenize import sent_tokenize
text_list=sent_tokenize(doc)
relations_list=[]

rel_cnt=0 
temp_bucket=[]
for text in text_list:
    
   
    # Tokenizer text
    model_inputs = tokenizer(text, max_length=256, padding=True, truncation=True, return_tensors = 'pt')

    # Generate
    generated_tokens = model.generate(
        model_inputs["input_ids"].to(model.device),
        attention_mask=model_inputs["attention_mask"].to(model.device),
        **gen_kwargs,
    )

    # Extract text
    decoded_preds = tokenizer.batch_decode(generated_tokens, skip_special_tokens=False)
    
    # Extract triplets
    for idx, sentence in enumerate(decoded_preds):
        rel_cnt=rel_cnt+1
        temp_bucket.append(extract_triplets(sentence))
        
        print(extract_triplets(sentence))
        relations_list.extend(extract_triplets(sentence))
        if (rel_cnt%100) == 0 :
            with open('tesla_rels.txt', 'a') as f:
                for item in temp_bucket:
                    f.write("%s\n" % item)
            temp_bucket=[]
            
