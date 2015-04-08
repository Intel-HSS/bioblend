#!/usr/bin/python
#Create a file called galaxy_key and add your key there
import galaxy_key;
from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.datasets import DatasetClient

gi = GalaxyInstance(url=galaxy_key.galaxy_host, key=galaxy_key.galaxy_key);

dsc = DatasetClient(gi);

for library in gi.libraries.get_libraries(name='Vectors'):     #iterate over list of dict
  library_id = library['id'];
  for content in gi.libraries.show_library(library_id, contents=True):  #iterate over list of dict
    if(content['name'].find('vB') != -1):
      print(dsc.show_dataset(content['id'], hda_ldda='ldda'));
      break;


