#-*- encoding: utf-8 -*-
# Copyright (c) 2019 Jordi Mas i Hernandez <jmas@softcatala.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this program; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place - Suite 330,
# Boston, MA 02111-1307, USA.

import os
import shutil
import json
from whoosh.util.text import rcompile
from whoosh.analysis import StandardAnalyzer
from whoosh.fields import BOOLEAN, TEXT, Schema, STORED, ID
from whoosh.index import create_in
from findfiles import FindFiles

class MemoryEntry(object):
    def __init__(self, word, filename, is_infinitive, infinitive):
        self.word = word
        self.filename = filename
        self.is_infinitive = is_infinitive
        self.infinitive = infinitive

class IndexCreator(object):

    def __init__(self, json_dir):
        self.json_dir = json_dir
        self.dir_name = "data/indexdir/"
        self.writer = None
        self.duplicated = {}
        self.memory_index = []


    def create(self, in_memory=False):
        tokenizer_pattern = rcompile(r"(\w|·)+(\.?(\w|·)+)*") # Includes l·l
        analyzer = StandardAnalyzer(minsize=1, stoplist=None, expression=tokenizer_pattern)
        schema = Schema(verb_form=TEXT(stored=True, sortable=True, analyzer=analyzer),
                        index_letter=TEXT(stored=True, analyzer=analyzer),
                        file_path=TEXT(stored=True, sortable=True))

        if os.path.exists(self.dir_name):
            shutil.rmtree(self.dir_name)

        os.makedirs(self.dir_name)

        ix = create_in(self.dir_name, schema)

        self.writer = ix.writer()
        return ix

    def _get_first_letter_for_index(self, word_ca):
        s = ''
        if word_ca is None:
            return s

        s = word_ca[0].lower()
        mapping = { u'à' : u'a',
                    u'è' : u'e',
                    u'é' : u'e',
                    u'í' : u'i',
                    u'ó' : u'o',
                    u'ò' : u'o',
                    u'ú' : u'u'} 

        if s in mapping:
            s = mapping[s]

        return s


    def write_entry(self, verb_form, file_path, is_infinitive):

        if is_infinitive is True:
            index_letter = self._get_first_letter_for_index(verb_form)
        else:
            index_letter = None

        self.writer.add_document(verb_form = verb_form,
                                 file_path = file_path,
                                 index_letter = index_letter)


    def _write_term(self, indexed, filename, word, form, is_infinitive, infinitive):
        indexed.add(word)
        self.memory_index.append(MemoryEntry(word, filename, is_infinitive, infinitive))
        return


    def _get_duplicated_forms_across_verbs(self, data, infinitive, sps):
        for form in data[infinitive]:
            for sp in sps:
                for conjugacio in form[sp]:
                    word = conjugacio['word']
                    if word in self.duplicated:
                        infinitives = self.duplicated[word]
                    else:
                        infinitives = set()

                    infinitives.add(infinitive)
                    self.duplicated[word] = infinitives


    def _process_file(self, filename):
        with open(filename) as json_file:
            data = json.load(json_file)
            infinitive = list(data.keys())[0]
            sps = ['singular1', 'singular2', 'singular3', 'plural1', 'plural2', 'plural3']

            self._get_duplicated_forms_across_verbs(data, infinitive, sps)

            #if infinitive != 'cantar':
            #    return 0

            indexed = set()

            for form in data[infinitive]:
                sps = ['singular1', 'singular2', 'singular3', 'plural1', 'plural2', 'plural3']
                for sp in sps:
                    for conjugacio in form[sp]:
                        word = conjugacio['word']

                        if word in indexed:
                            continue

                        words = [x.strip() for x in word.split('/')]
                        for word in words:
                            is_infinitive = form['form'] == "Infinitiu"
                            self._write_term(indexed, filename, word, form['form'], is_infinitive, infinitive)

    def save_index(self):
        self.writer.commit()

    def _add_infinitive_to_duplicates(self):
        duplicated = 0
        for word in self.duplicated.keys():
            infinitives = self.duplicated[word];
            if len(infinitives) <= 1:
                continue

            for infinitive in infinitives:
                for entry in self.memory_index:
                    if entry.word == word and entry.infinitive == infinitive:
                        entry.word = "{0} ({1})".format(word, infinitive)

            duplicated = duplicated + 1

        return duplicated


    def _save_memory_index(self):
        indexed = set()
        for entry in self.memory_index:
            self.write_entry(entry.word, entry.filename, entry.infinitive)
            print(entry.filename)
            print(entry.word)
            print(entry.infinitive)
            print("---")
            indexed.add(entry.word)

        return len(indexed)

    def process_files(self):
        findFiles = FindFiles()
        files = findFiles.find_recursive(self.json_dir, '*.json')
        for filename in files:
            self._process_file(filename)

        duplicated = self._add_infinitive_to_duplicates()
        indexed = self._save_memory_index()

        print("Processed {0} files, indexed {1} variants, duplicated forms across verbs {2}".
             format(len(files), indexed, duplicated))

