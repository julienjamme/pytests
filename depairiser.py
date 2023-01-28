import pandas as pd
import timeit

source_file = pd.read_parquet("./pytests/xtract_cnaf_m02201.parquet.gz")

print(source_file.size)
print(source_file.columns)
print(source_file.head(10))

def extract(source):
    source_cut = [elt.split("|") for elt in source.split(" ")]
    # zip fabrique des paires, zip(*) les sépare
    val, var = zip(*source_cut)
    # on récupère des tuples qu'on transforme en listes + concaténation des chaînes
    val_char = ' '.join(list(val))
    var_char = ' '.join(list(var))
    return [val_char, var_char]

source_liste = source_file[].adresse.tolist()
source = source_liste[0]
extract(source_liste[0])


n = 1000000
# source_liste = source_file.head(n).adresse.tolist()
# tic = timeit.default_timer()
# extract_liste = [extract(s) for s in source_liste]
# elapsed_liste = timeit.default_timer() - tic

tic = timeit.default_timer()
extract_pd = source_file.head(n).adresse.apply(extract)
elapsed_pd = timeit.default_timer() - tic

# print(elapsed_liste)
print(elapsed_pd)
