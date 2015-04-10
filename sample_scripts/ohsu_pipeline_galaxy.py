#!/usr/bin/python

import galaxy_key;
import sys;
from bioblend.galaxy import GalaxyInstance
import json;

gi = GalaxyInstance(url=galaxy_key.galaxy_host, key=galaxy_key.galaxy_key);

reference = fq1 = fq2 = indels = dbsnp = None;
for history in gi.histories.get_histories():  #iterate over list of dict - use your own history name
    history_id = history['id'];
    for dataset in gi.histories.show_matching_datasets(history_id):
        if('deleted' in dataset and dataset['deleted'] == True):
            continue;
        if('name' in dataset):
            if(dataset['name'] == '1000g_indels.vcf'):
                indels = dataset;
            if(dataset['name'] == 'dbsnp-all.vcf'):
                dbsnp = dataset;
            if(dataset['name'] == 'sim1M_pairs_1.fq'):
                fq1 = dataset;
            if(dataset['name'] == 'sim1M_pairs_2.fq'):
                fq2 = dataset;
            if(dataset['name'] == 'human_g1k_v37.fasta'):
                reference = dataset;

if(not (reference and fq1 and fq2 and indels and dbsnp)):
    print('Not all inputs found - exiting');
    sys.exit(-1);

#workflows
#Search for workflow OHSU_3_stage in your workflows - note workflows should be imported into 
#your account 
workflow_params = dict();
for workflow in gi.workflows.get_workflows(name='OHSU Workflow'):
  workflow_id = workflow['id'];
  workflow_object = gi.workflows.show_workflow(workflow_id);    #dict
  steps_dict = workflow_object['steps'];     #dict
  for step_idx, step_spec in steps_dict.iteritems():
    print("Step %d tool %s input %s"%(int(step_idx), step_spec['tool_id'], step_spec['input_steps']));
    if(step_spec['tool_id'] and step_spec['tool_id'] == 'bwa_mem'):
      bwa_params = json.loads(step_spec['tool_inputs']['params']);
      bwa_readgroup_params = bwa_params['readGroup'];
      bwa_readgroup_params['rgid'] = 'sim1M_pairs';
      bwa_readgroup_params['rglb'] = 'sim1M_pairs';
      bwa_readgroup_params['rgsm'] = 'sim1M_pairs';
      bwa_readgroup_param_string = str(bwa_readgroup_params);
      workflow_params['bwa_mem'] = { 'param' : 'readGroup', 'value' : bwa_readgroup_param_string };
  input_map = workflow_object['inputs'];        #get input labels for workflow
  dataset_input_map = dict();                       #dict to set inputs
  for input_idx, input_spec in input_map.iteritems():
    if(input_spec['label'] == 'human_g1k_v37.fasta'):
      dataset_input_map[input_idx] = { 'id' : reference['id'], 'src' : 'hda' }; 
    if(input_spec['label'] == '1000g_indels.vcf'):
      dataset_input_map[input_idx] = { 'id' : indels['id'], 'src' : 'hda' };
    if(input_spec['label'] == 'dbsnp-all.vcf'):
      dataset_input_map[input_idx] = { 'id' : dbsnp['id'], 'src' : 'hda' };
    if(input_spec['label'] == 'sim1M_pairs_1.fq'):
      dataset_input_map[input_idx] = { 'id' : fq1['id'], 'src' : 'hda' };
    if(input_spec['label'] == 'sim1M_pairs_2.fq'):
      dataset_input_map[input_idx] = { 'id' : fq2['id'], 'src' : 'hda' };
  #Send outputs to new history called 'OHSU_3_step_history' - HISTORY IS CREATED!
  gi.workflows.run_workflow(workflow_id, dataset_map=dataset_input_map, params=workflow_params, history_name="OHSU_workflow");
  #Alternately, send outputs to existing history whose id has been determined in the first 15 lines
  #gi.workflows.run_workflow(workflow_id, dataset_map=dataset_input_map,history_id=history_id);
