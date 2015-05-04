#!/usr/bin/python
import sys;
import uuid;
import bioblend;
import json;
import csv;
import itertools;
from optparse import OptionParser
from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.libraries import LibraryClient
from bioblend.galaxy.folders import FoldersClient

#Exception class used by this module
class ImportDatasetsByUUIDException(Exception):
    def __init__(self, value):
        self.value = value;
    def __str__(self):
        return repr(self.value)

def print_error_and_exit(msg):
    raise ImportDatasetsByUUIDException(msg);
    #sys.stderr.write(msg+'\n');
    #sys.exit(-1);

def parse_auth_file(filename):
    try:
        fd = open(filename,'rb');
    except IOError:
        print_error_and_exit('Could not open auth file '+filename);
    try:
        auth_json = json.load(fd);
    except ValueError:
        print_error_and_exit('Invalid JSON format for auth file '+filename);
    if('galaxy_host' not in auth_json):
        print_error_and_exit('Auth JSON file does not contain key "galaxy_host"');
    if('galaxy_key' not in auth_json):
        print_error_and_exit('Auth JSON file does not contain key "galaxy_key"');
    fd.close();
    return (auth_json.get('galaxy_host'), auth_json.get('galaxy_key'));


#Represent dictionary as object
class DSInfo(object):
    def __init__(self):
        #Fields required for INFO struct
        default_info_dict = { 'uuid':None, 'name':None, 'query_idx':None, 
                'dataset_collection_name':None, 'pair_direction':None, 'pair_id':None,
                'src_hda_id':None, 'src_history_id':None, 'src_ldda_id':None, 'src_library_id':None,
                'target_hda_id':None, 'skip':False };
        self.__dict__ = default_info_dict;

    def validate(self):
        if(not self.uuid):
            self.do_skip('Undefined UUID for info object');
        elif(not self.name):
            self.do_skip('Undefined name for info object corresponding to UUID %s'%(self.uuid));
        elif(self.pair_direction or self.pair_id):
            if(self.pair_direction and self.pair_direction != 'forward' and self.pair_direction != 'reverse'):
                self.do_skip('Unknown paired field value: %s for info object corresponding to UUID %s. Allowed paired field values are forward and reverse'
                    %(str(info.pair_direction), str(self.uuid)));
            if(not self.pair_id):
                self.do_skip('Paired datasets must specify unique pair id - no pair id found for UUID %s'%(str(self.uuid)));
        elif(not((self.src_library_id and self.src_ldda_id) or (self.src_history_id and self.src_hda_id))):
            self.do_skip('No dataset found for UUID %s, will ignore this dataset completely'%(str(self.uuid)));
        #elif(not self.target_hda_id):
            #self.do_skip('Target hda id not found for datasets %s'%(str(self.uuid)));
        #UUID occurs in both history and library, nullify one
        if(self.src_library_id and self.src_ldda_id and self.src_history_id and self.src_hda_id):
            self.src_hda_id = None;
            self.src_history_id = None;
    
    def do_skip(self, message):
        sys.stdout.write('WARNING: '+message+', this dataset/UUID entry will be ignored\n');
        self.skip = True;

    def __str__(self):
        return str(self.__dict__);

def validate_queried_dataset_info(dataset_info_list):
    num_valid_datasets = 0;
    num_library_datasets = 0;
    num_history_datasets = 0;
    for info in dataset_info_list:
        info.validate();
        if(not info.skip):
            num_valid_datasets += 1;
            if(info.src_ldda_id):
                num_library_datasets += 1;
            elif(info.src_hda_id):
                num_history_datasets += 1;
    print('INFO: out of %d queried datasets, %d were valid, %d found in libraries, %d found in histories'
            %(len(dataset_info_list), num_valid_datasets, num_library_datasets, num_history_datasets));

#Check header of UUID query file
def check_and_return_header(uuids_fd):
    uuid_field_names = set(['CCC_DID', 'UUID']);
    first_line = uuids_fd.readline();  #Read first line
    dialect = None;
    has_header = False;
    identifer_field_name = None;
    try:
        sniffer = csv.Sniffer();
        dialect = sniffer.sniff(first_line);
        has_header = sniffer.has_header(first_line);
    except csv.Error:
        dialect = csv.excel();
        dialect.delimiter = '\t';
        dialect.quotechar = '';
        dialect.quoting = csv.QUOTE_NONE;
    print('INFO: for UUID file, delimiter is %s and quote char is %s'
            %('<TAB>' if dialect.delimiter=='\t' else dialect.delimiter,
                '<NONE>' if dialect.quoting == csv.QUOTE_NONE else dialect.quotechar));
    first_line = first_line.strip();
    fieldnames = first_line.split(dialect.delimiter);
    for token in fieldnames:
        if(token in uuid_field_names):
            has_header = True;
            identifer_field_name = token;
            break;
    if(not has_header):
        print_error_and_exit('TSV file with CCC_DIDs/UUIDs: %s does not seem to have a header row'%(uuids_filename));
    return dialect, identifer_field_name, fieldnames;

#First line of TSV file is assumed to be header with field names
def parse_TSV_file(uuids_fd):
    uuid_str_to_info = {};
    line_number = 2;    #First line is header, begin numbering at 1 for ease of diplaying error messages to user
    first_line_offset = 2;    
    dialect,id_field_name,fieldnames = check_and_return_header(uuids_fd);
    csv_reader = csv.DictReader(uuids_fd, fieldnames=fieldnames, dialect=dialect);
    for row in csv_reader:
        uuid_str = row.get(id_field_name, None);
        if(not uuid_str):
            sys.stdout.write('WARNING: no CCC_DID/UUID found in line %d, skipping\n'%(line_number));
            continue;
        if(uuid_str in uuid_str_to_info):
            sys.stdout.write('WARNING: CCC_DID/UUID in line %d was already present on line %d, skipping\n'%(line_number,
                uuid_str_to_info[uuid_str].line_number));
            continue;
        try:
            X = uuid.UUID(uuid_str);
        except:
            sys.stdout.write('WARNING: Incorrectly formatted CCC_DID/UUID on line %d, skipping\n'%(line_number));
            continue;
        uuid_str_to_info[uuid_str] = DSInfo();
        uuid_str_to_info[uuid_str].uuid = uuid_str;
        uuid_str_to_info[uuid_str].query_idx = line_number - first_line_offset;
        uuid_str_to_info[uuid_str].dataset_collection_name = row.get('dataset_collection_name', None);
        uuid_str_to_info[uuid_str].pair_direction = row.get('pair_direction',None);
        uuid_str_to_info[uuid_str].pair_id = row.get('pair_id',None);
        line_number += 1;
    return uuid_str_to_info;

def find_datasets_by_uuids_in_histories(gi, history_client, datasets_dict, search_history_id=None, search_history_name=None):
    if(not datasets_dict):
        return;
    #Iterate over histories
    for history in gi.histories.get_histories(history_id=search_history_id, name=search_history_name):  #iterate over list of dict
        history_id = history['id'];
        for dataset in history_client.show_history(history_id, contents=True, deleted=False, details='all'):
            uuid = dataset.get('uuid', None);
            if(uuid and uuid in datasets_dict):
                datasets_dict[uuid].src_history_id = history_id;
                datasets_dict[uuid].src_hda_id = dataset['id'];
                datasets_dict[uuid].name = dataset.get('name', 'no_name');

def find_datasets_by_uuids_in_libraries(gi, library_client, datasets_dict, search_library_id=None, search_library_name=None):
    if(not datasets_dict):
        return;
    #Iterate over dataset libraries
    for library in gi.libraries.get_libraries(library_id=search_library_id, name=search_library_name):     #iterate over list of dict
        library_id = library['id'];
        for dataset_info in gi.libraries.show_library(library_id, contents=True):  #iterate over list of dict
            dataset = library_client.show_dataset(library_id, dataset_info['id']);
            if(dataset.get('model_class', None) == 'LibraryDataset' and dataset.get('deleted',False) != True):
                uuid = dataset.get('uuid', None);
                if(uuid and uuid in datasets_dict):
                    datasets_dict[uuid].src_library_id = library_id;
                    datasets_dict[uuid].src_ldda_id = dataset['id'];
                    datasets_dict[uuid].name = dataset.get('name', 'no_name');

def get_or_create_history_id(gi, history_client, target_history_name):
    if(not target_history_name):
        print_error_and_exit('Target history name cannot be None');
    #Get id of target history
    target_history_id = None;
    for history in gi.histories.get_histories(name=target_history_name):
        target_history_id = history['id'];
        break;
    #Create history, if it does not exist
    if(not target_history_id):
        target_history_id = history_client.create_history(target_history_name)['id'];
    if(not target_history_id):
        print_error_and_exit('Unable to create history with name : %s'%(target_history_name));
    return target_history_id;

#Import datasets from Galaxy lib
def copy_from_lib(gi, history_client, dataset_info_list, target_history_name=None, target_history_id=None):
    if(not target_history_id):
        target_history_id = get_or_create_history_id(gi, history_client, target_history_name);
    for info in dataset_info_list:
        if(info.skip):
            continue;
        if(info.src_library_id and info.src_ldda_id):
            target_hda = history_client.upload_dataset_from_library(target_history_id, info.src_ldda_id);
            if(not target_hda or 'id' not in target_hda):
                print_error_and_exit('Could import dataset %s into history'%(info.name));
            info.target_hda_id = target_hda['id'];

#Copy to /tmp lib and back to history
def copy_to_tmp_lib_and_back(gi, library_client, history_client, folder_client, tmp_lib_name, dataset_info_list, 
        target_history_name=None, target_history_id=None):
    #Get id of tmp library
    tmp_lib_id = None;
    for library in gi.libraries.get_libraries(name=tmp_lib_name):     #iterate over list of dict
        tmp_lib_id = library['id'];
        break;
    if(not tmp_lib_id):
        print_error_and_exit('Temporary library %s does not exist'%(tmp_lib_name));
    if(not target_history_id):
        target_history_id = get_or_create_history_id(gi, history_client, target_history_name); 
    if(tmp_lib_id and target_history_id):
        #Copy datasets to a unique folder in the tmp library, then upload to target history
        folder_name= str(uuid.uuid4());     #get unique folder name
        #Create folder, returns list of dict
        folder_list = library_client.create_folder(tmp_lib_id, folder_name);
        if(len(folder_list) == 0):
            print_error_and_exit('Could not create folder in tmp library %s\n'%(tmp_lib_name));
        if('id' not in folder_list[0]):
            print_error_and_exit('Could not copy file to folder %s in tmp library %s\n'%(folder_name, tmp_lib_name));
        folder_id = folder_list[0]['id'];
        #copy history datasets into tmp library folder and then re-import
        for info in dataset_info_list:
            if(info.skip):
                continue;
            if(info.src_history_id and info.src_hda_id):
                library_dataset = library_client.copy_from_dataset(tmp_lib_id, info.src_hda_id, folder_id);
                if(not library_dataset or 'id' not in library_dataset):
                    print_error_and_exit('Could not copy dataset %s to tmp library folder'%(info.name));
                target_hda = history_client.upload_dataset_from_library(target_history_id, library_dataset['id']);
                if(not target_hda or 'id' not in target_hda):
                    print_error_and_exit('Could import dataset %s into history'%(info.name));
                info.target_hda_id = target_hda['id'];
        folder_client.delete_folder(folder_id);

def copy_other_history_datasets(gi, history_client, dataset_info_list, target_history_name=None, target_history_id=None):
    if(not target_history_id):
        target_history_id = get_or_create_history_id(gi, history_client, target_history_name); 
    for info in dataset_info_list:
        if(info.skip):
            continue;
        #Is a history dataset from a different history
        if(info.src_history_id and info.src_hda_id and info.src_history_id != target_history_id):
            target_hda = history_client.copy_history_dataset(target_history_id, info.src_hda_id);
            if(not target_hda or 'id' not in target_hda):
                print_error_and_exit('Could import dataset %s into history'%(info.name));
            info.target_hda_id = target_hda['id'];

def create_dataset_collections(gi, history_client, dataset_info_list, target_history_id=None, target_history_name=None):
    if(not target_history_id):
        target_history_id = get_or_create_history_id(gi, history_client, target_history_name);
    #Group elements by collection name
    #Sort first to use groupBy
    dataset_info_list.sort(key=lambda info: info.dataset_collection_name if info.dataset_collection_name else 'None');
    #For each collection name, build payload as expected by Bioblend/Galaxy
    for collection_name, element_info_iter in itertools.groupby(dataset_info_list,
            key=lambda info: info.dataset_collection_name if info.dataset_collection_name else 'None'):
        if(collection_name == 'None'):
            continue;
        collection_elements_list = [];
        #Dictionary storing pair_id -> { forward: {element_dict}, reverse: {element_dict}}
        pair_id_to_direction_dict = {};
        #All elements of a collection must be paired or unpaired (no mixing allowed)
        is_paired_list = False;
        is_first_element_in_collection = True;
        #Skip creating this collection
        skip_collection = False;
        for info in element_info_iter:
            if(info.skip):
                continue;
            #Current element paired?
            is_curr_element_paired = False;
            if(info.pair_direction):  #is actually a paired list
                is_curr_element_paired = True;
                if(is_first_element_in_collection):
                    is_paired_list = True;
            if(is_curr_element_paired != is_paired_list):
                sys.stdout.write('ERROR: dataset collection %s is a mixture of paired and un-paired elements, skipping\n'%(collection_name));
                skip_collection = True;
                break;
            curr_dict = {};
            if(info.target_hda_id):
                curr_dict['src'] = 'hda';
                curr_dict['id'] = info.target_hda_id;
            elif(info.src_library_id and info.src_ldda_id):        #library dataset
                curr_dict['src'] = 'ldda';
                curr_dict['id'] = info.src_ldda_id;
            elif(info.src_history_id and info.src_hda_id):
                curr_dict['src'] = 'hda';
                curr_dict['id'] = info.src_hda_id;
            curr_dict['name'] = info.name;
            #For list of pairs, each element of the list is pair
            #Change curr_dict to reflect info about the pair
            #Since the other part of a pair may not be known, this info will be updated at the end
            if(is_curr_element_paired):
                if(info.pair_id not in pair_id_to_direction_dict):
                    pair_id_to_direction_dict[info.pair_id] = {};
                    new_dict = {};
                    new_dict['src'] = 'new_collection';
                    new_dict['name'] = info.pair_id;      #Pair id
                    new_dict['collection_type'] = 'paired';
                    collection_elements_list.append(new_dict);
                curr_dict['name'] = info.pair_direction;        #Name of pair element should be forward/reverse
                pair_id_to_direction_dict[info.pair_id][info.pair_direction] = curr_dict;
            else:
                collection_elements_list.append(curr_dict);
            is_first_element_in_collection = False;
        #For paired list, update element_identifiers for each pair and check whether all pairs have both halves
        if(not skip_collection and is_paired_list):
            list_idx = 0;
            for curr_dict in collection_elements_list:
                pair_id = curr_dict['name'];    #guaranteed to be found in dictionary, just check whether both halves exist
                if(len(pair_id_to_direction_dict[pair_id]) != 2):
                    sys.stdout.write('WARNING: Two halves of pair %d not found for collection %s, skipping pair'%(pair_id, collection_name));
                    continue;
                #Add identifiers for members of pair
                collection_elements_list[list_idx]['element_identifiers'] = [];
                for direction_dict in pair_id_to_direction_dict.itervalues():
                    for element_dict in direction_dict.itervalues():
                        collection_elements_list[list_idx]['element_identifiers'].append(element_dict);
                list_idx += 1;
        if(skip_collection or len(collection_elements_list) == 0):
            continue;
        payload = { 'source':'new_collection', 'collection_type':'list:paired' if is_paired_list else 'list', 'name':collection_name,
                'element_identifiers':collection_elements_list };
        #print(payload);
        history_client.create_dataset_collection(target_history_id, payload);
        print('INFO: created collection "%s" of type %s with %d element(s)'%(collection_name, 'list:paired' if is_paired_list else 'list',
            len(collection_elements_list)));

def main():
    parser = OptionParser()
    parser.add_option("-A", "--auth-file", dest="auth_filename",
                              help="JSON file with Galaxy host and key", metavar="FILE");
    parser.add_option("-f", "--uuid-file", dest="uuids_filename",
                              help="TSV file with list of UUIDs to import. The first row is assumed to be a header", metavar="FILE");
    parser.add_option("-H", "--target-history", dest="target_history",
                              help="Target history name in Galaxy to copy datasets into", metavar="HISTORY_NAME");
    (options, args) = parser.parse_args()
    if(not options.auth_filename):
        print_error_and_exit('Authentication file not provided');
    #if(not options.uuids_filename):
        #print_error_and_exit('TSV file with UUIDs not provided');
    if(not options.target_history):
        print_error_and_exit('Galaxy history name where datasets will be imported not provided');

    #Read authentication info
    galaxy_host,galaxy_key = parse_auth_file(options.auth_filename);

    gi = GalaxyInstance(url=galaxy_host, key=galaxy_key);
    history_client = HistoryClient(gi); 
    library_client = LibraryClient(gi);
    folder_client = FoldersClient(gi);

    #Read UUIDs file
    if(options.uuids_filename):
        try:
            uuids_fd = open(options.uuids_filename, 'rb');
        except IOError:
            print_error_and_exit('Could not open TSV file with UUIDs '+options.uuids_filename);
    else:
        uuids_fd = sys.stdin;
    queried_ds_uuid_dict = parse_TSV_file(uuids_fd);

    #Search for datasets
    find_datasets_by_uuids_in_histories(gi, history_client, queried_ds_uuid_dict);
    find_datasets_by_uuids_in_libraries(gi, library_client, queried_ds_uuid_dict);

    dataset_info_list = queried_ds_uuid_dict.values();
    #Validate datasets, discard repeats
    validate_queried_dataset_info(dataset_info_list);

    #Get/create target history
    target_history_id = get_or_create_history_id(gi, history_client, options.target_history);
    #Copy datasets from library to history
    copy_from_lib(gi, history_client, dataset_info_list, target_history_id=target_history_id);
    #Copy from history to /tmp and back - don't use anymore
    #copy_to_tmp_lib_and_back(gi, library_client, history_client, folder_client, '/tmp', dataset_info_list, target_history_id=target_history_id);
    #Copy history datasets from other histories
    copy_other_history_datasets(gi, history_client, dataset_info_list, target_history_id=target_history_id);
    #Create dataset collections
    create_dataset_collections(gi, history_client, dataset_info_list, target_history_id=target_history_id);

if __name__ == "__main__":
    main()
