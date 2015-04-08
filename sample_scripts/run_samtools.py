#!/usr/bin/python
import galaxy_key;
import sys;
from bioblend.galaxy import GalaxyInstance
#Create a file called galaxy_key and add your key there

gi = GalaxyInstance(url=galaxy_key.galaxy_host, key=galaxy_key.galaxy_key);

history_id = None;
input_sam = None;
input_ref = None;
input_bam = None;
big_input_bam = None;
#Search for a dataset called vB.val in your history called dot_product
for history in gi.histories.get_histories():  #iterate over list of dict - use your own history name
  history_id = history['id'];
  for dataset in gi.histories.show_matching_datasets(history_id):
    if('deleted' in dataset and dataset['deleted'] == True):
      continue;
    if('name' in dataset and dataset['name'] == 'sim1M_pairs_final.bam'):
      input_bam = dataset; 
    if('name' in dataset and dataset['name'] == 'G15512.HCC1954.1_final.bam'):
      big_input_bam = dataset; 
    #if('name' in dataset and dataset['name'] == 'Hacked_samtools on data 1 and data 2: converted BAM' and dataset['id'] == 'efa5800a97f62f19'):
      #input_bam = dataset; 
    if('name' in dataset and dataset['name'] == 'human_g1k_v37.fasta'):
      input_ref = dataset; 
    if('name' in dataset and dataset['name'] == 'sim1M_pairs0.sam'):
      input_sam = dataset; 

tool_inputs = dict();

if(len(sys.argv) >= 2 and sys.argv[1] == 'view'):
    tool_inputs['source|index_source'] = 'history';
    tool_inputs['source|ref_file'] = { 'src':'hda', 'id':input_ref['id'] };
    tool_inputs['source|input1'] = { 'src':'hda', 'id':input_sam['id'] };
    gi.tools.run_tool(history_id, 'hacked_sam_to_bam', tool_inputs);
else:
    tool_inputs['input1'] = { 'src':'hda', 'id':big_input_bam['id'] };
    gi.tools.run_tool(history_id, 'hacked_samtools_flagstat', tool_inputs);

