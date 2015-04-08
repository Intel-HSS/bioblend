#!/usr/bin/python
import galaxy_key;
from bioblend.galaxy import GalaxyInstance
#Create a file called galaxy_key and add your key there

gi = GalaxyInstance(url=galaxy_key.galaxy_host, key=standalone_galaxy_key.galaxy_key);

history_id = None;
vlist = [];
#histories
for history in gi.histories.get_histories(name='Vectors'):  #iterate over list of dict - use your own history name
  history_id = history['id'];
  for dataset in gi.histories.show_matching_datasets(history_id):
    if('deleted' in dataset and dataset['deleted'] == True):
      continue;
    if('name' in dataset and (dataset['name'] == 'vA.val' or dataset['name'] == 'vB.val')):
	vlist.append( { 'src' : 'hda', 'id' : dataset['id'] } ); 

if len(vlist) > 0:
  vlist.append(vlist[0]);
  tool_inputs = dict();
  idx = 0;
  for curr_dict in vlist:
    key_value = 'vectors_%d|input'%idx;
    tool_inputs[key_value] = curr_dict;
    idx = idx + 1;
  gi.tools.run_tool(history_id, 'summer', tool_inputs);

