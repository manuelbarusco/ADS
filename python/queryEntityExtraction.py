import spacy
from spacy import displacy


NER = spacy.load("en_core_web_sm")

raw_text="The locations of all Long Term Care and Residential Care facilties in Nova Scotia by their civic address."

text1= NER(raw_text)

for word in text1.ents:
    print(word.text,word.label_)
