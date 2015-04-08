#!/usr/bin/python
import galaxy_key;
from bioblend.galaxy import GalaxyInstance
#Create a file called galaxy_key and add your key there

gi = GalaxyInstance(url=galaxy_key.galaxy_host, key=galaxy_key.galaxy_key);

history_id = None;
vB = None;
#Search for a dataset called vB.val in your history called dot_product
for history in gi.histories.get_histories(name='dot_product'):  #iterate over list of dict - use your own history name
  history_id = history['id'];
  for dataset in gi.histories.show_matching_datasets(history_id, name_filter='vB.val'):
    if('deleted' in dataset and dataset['deleted'] == True):
      continue;
    vB = dataset; 
    break;
#Set input labeled 'vector' to dataset vB
tool_inputs = dict();
tool_inputs['vector'] = { 'src':'hda', 'id':vB['id'] };
gi.tools.run_tool(history_id, 'sum_reduce', tool_inputs);

