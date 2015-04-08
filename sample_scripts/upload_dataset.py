#!/usr/bin/python
import galaxy_key;
from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.libraries import LibraryClient
#Create a file called galaxy_key and add your key there
import sys;

gi = GalaxyInstance(url=galaxy_key.galaxy_host, key=galaxy_key.galaxy_key);

li = LibraryClient(gi);
for library in gi.libraries.get_libraries(name='Data_object'):     #iterate over list of dict
  library_id = library['id'];
  li.upload_from_galaxy_filesystem(library_id, '/home/karthikg/test/sum/vwewewewe.val', link_data_only='link_to_files', ccc_did = '3456');

