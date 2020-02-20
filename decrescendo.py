# coding: utf-8
from collections import defaultdict
import csv
import glob
import itertools
import logging
import os
import sys
import cchardet as chardet
from charset_normalizer import CharsetNormalizerMatches as CnM
import pandas as pd
import xlrd
from io import StringIO

def ingest(path):
    def detect_encoding(file):
        with open(file, 'rb') as f:
            result = chardet.detect(f.read())

        result_log = \
            "\tFile: \t\t %s \n" \
            "\tEncoding: \t %s \n" \
            "\tConfidence:\t %f \n" \
            % (os.path.basename(file), result['encoding'], result['confidence'])

        print(result_log)
        logging.info(result_log)
        return result['encoding']


    def get_data(folder_path: str) -> dict:
        """
        Iterates through a directory to create a Dict of Pandas DataFrames with
        filepaths as their keys.

        :type folder_path: str
        :rtype: dict

        keys: filepaths
        values: pd.DataFrame
        """

        # print("This is the name of the script: ", sys.argv[0])
        print("Initializing Data Retrieval...")

        csvfiles = glob.glob(folder_path + "/**/*.csv", recursive=True)
        xlfiles = glob.glob(folder_path + "/**/*.xls?", recursive=True)
        xlfiles = xlfiles + glob.glob(folder_path + "/**/*.xls", recursive=True)
        # xlfiles = []
        file_dict = {}
        i = 1

        for file in xlfiles:
            print("Reading File %d of %d:" % (i, len(csvfiles) + len(xlfiles)))
            print("\tFull Path: ", file)
            # csv_from_excel(file)
            try:
                df = pd.read_excel(file, sheet_name=None)

                for sheet in df.keys():
                    print("\t\t", sheet, "processed...")
                    df[sheet].index.rename('file_index',inplace=True)
                    file_dict.update({file.join(['', sheet]): df[sheet]})
            except:
                logging.error('COULD NOT LOAD %s' % file)
                print('\t\tFAILED')

            i += 1

        for file in csvfiles:
            # print("Reading File %d of %d:" % (i, len(csvfiles) + len(xlfiles)))
            print("Reading File %d of %d:" % (i, len(csvfiles) + len(xlfiles)))
            print("\tFull Path: ", file)
            try:
                df = pd.read_csv(file, low_memory=False, header='infer', encoding = detect_encoding(file))
                df.index.rename('file_index',inplace=True)
                file_dict.update({file: df})
            except UnicodeDecodeError:
                try:
                    print("Encoding Detection Failed... Attempting to Normalize...")
                    normalized = StringIO(str(CnM.from_path(file).best().first()))
                    df = pd.read_csv(normalized, low_memory=False, header='infer')
                    df.index.rename('file_index',inplace=True)
                    file_dict.update({file: df})
                    print("Success!")
                except:
                    print('Encoding Normalization Failed')
            except:
                logging.error('COULD NOT LOAD %s' % file)
                print('\t\tFAILED')
            i += 1
        return file_dict


    def clean_data(dirty_data: pd.DataFrame) -> pd.DataFrame:
        """
        * Removes Blank and Unnamed Columns
        * Sets headers to lower-case
        * Replaces spaces with underscores in headers
        * Removes Parentheses, periods and double-underscores from headers
        """

        logging.info("Cleaning Data...")

        cleaned_data = dirty_data.dropna(how='all', axis=1)
        cleaned_data = cleaned_data.loc[:, ~
                                        cleaned_data.columns.str.contains('Unnamed')]
        cleaned_data = cleaned_data.loc[:, ~
                                        cleaned_data.columns.str.contains('unnamed')]
        cleaned_data.columns = (
            cleaned_data.columns
            .str.strip()
                .str.lower()
                .str.replace(" ", "_")
                .str.replace("(", "")
                .str.replace(")", "")
                .str.replace(".", "")
                .str.replace("__", "_")
                .str.replace("\n", "_"))

        return cleaned_data


    def merge_common(lists):
        # merge function to  merge all sublist having common elements.
        neigh = defaultdict(set)
        visited = set()
        for each in lists:
            for item in each:
                neigh[item].update(each)

        def comp(node, neigh=neigh, visited=visited, vis=visited.add):
            nodes = set([node])
            next_node = nodes.pop
            while nodes:
                node = next_node()
                vis(node)
                nodes |= neigh[node] - visited
                yield node

        for node in neigh:
            if node not in visited:
                yield sorted(comp(node))


    def group_similar_tables(tabledict: dict):
        print("Grouping Similar Files...")
        # Determine if any of the tables in the TableDict are the same format.
        like_tables = []
        match_threshold = 0.75
        table_info = {}
        for k, v in tabledict.items():
            table_info[k] = list(v.columns.str.strip()
                                .str.lower()
                                .str.replace(" ", "_")
                                .str.replace("(", "")
                                .str.replace(")", "")
                                .str.replace(".", "")
                                .str.replace("__", "_"))

        combos = itertools.combinations(table_info.items(), 2)
        rejects = []
        for combo in combos:
            c1 = set(combo[0][1]) - set(combo[1][1])
            c2 = set(combo[1][1]) - set(combo[0][1])
            score1 = 1 - (len(c1) / len(combo[0][1]))
            score2 = 1 - (len(c2) / len(combo[1][1]))
            if min(score1, score2) >= match_threshold:
                like_tables.append([combo[0][0], combo[1][0]])

        like_tables = list(merge_common(like_tables))
        matched_files = [item for sublist in like_tables for item in sublist]
        rejects = set(list(table_info.keys())) - set(matched_files)

        if len(rejects)> 0: like_tables.append(rejects)

        grouped = []

        for group in like_tables:
            merged = pd.DataFrame()
            for table in group:
                try:
                    tabledict[table]['filename'] = table
                    tabledict[table] = clean_data(tabledict[table])
                    merged = merged.append(tabledict[table], sort=False)
                except:
                    logging.error('COULD NOT MERGE %s' % table)
            grouped.append(merged)
        print("%d Table(s) Analyzed, %d Unique Schema(s) Detected" % (len(tabledict), len(grouped)))
        return grouped

    tables = get_data(path)
    grouped_tables = group_similar_tables(tables)
    return grouped_tables

if __name__ == '__main__':
    path = sys.argv[1]
    tables = ingest(path)
    for i, table in enumerate(tables):
        print('Exporting table %d of %d' % (i+1, len(tables)))
        table.to_csv(path + r'\\Decrescendo Export No ' + str(i) +'.csv')