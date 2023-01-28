import pandas as pd
import timeit
import re

source_file = pd.read_parquet("./pytests/xtract_cnaf_m02201.parquet.gz")

print(source_file.size)
print(source_file.columns)
print(source_file.head(10))


def extract(source):
    # Gestion des valeurs manquantes
    regex_liste_NA = [r'^NA\ ', r'\ NA\ ', r'\ NA$']
    source_ss_NA = source
    for regex in regex_liste_NA:
        source_ss_NA = re.sub(regex, ' ', source_ss_NA)
    source_ss_NA = '' if source_ss_NA == ' ' else source_ss_NA
    source_cut = [elt.split("|") for elt in source_ss_NA.split(" ")]
    source_cut = [elt for elt in source_cut if elt != ['']]
    if source_cut == []:
        val_char, var_char = 'NA', 'NA'
    else:
        # zip fabrique des paires, zip(*) les sépare
        val, var = zip(*source_cut)
        # on récupère des tuples qu'on transforme en listes + concaténation des chaînes
        val_char = ' '.join(list(val))
        var_char = ' '.join(list(var))
    return [val_char, var_char]


n = 100000
# source_liste = source_file.head(n).adresse.tolist()
# tic = timeit.default_timer()
# extract_liste = [extract(s) for s in source_liste]
# elapsed_liste = timeit.default_timer() - tic

tic = timeit.default_timer()
extract_pd = source_file.head(n).adresse.apply(extract)
elapsed_pd = timeit.default_timer() - tic

source_file["adresse_valeurs"] = extract_pd.apply(lambda x: x[0])
source_file["adresse_variables"] = extract_pd.apply(lambda x: x[1])

