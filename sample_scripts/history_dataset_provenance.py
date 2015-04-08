#!/usr/bin/python
import sys;
import galaxy_key;
from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.libraries import LibraryClient
from bioblend.galaxy.histories import HistoryClient
#Create a file called galaxy_key and add your key there

gi = GalaxyInstance(url=galaxy_key.galaxy_host, key=galaxy_key.galaxy_key);

hc = HistoryClient(gi);

my_history = hc.get_histories()[0];

my_history_id = my_history['id'];

dataset = hc.show_matching_datasets(my_history_id, 'sum_vector')[0];

dataset_provenance = hc.show_dataset_provenance(my_history_id, dataset['id']);

print(dataset_provenance);


